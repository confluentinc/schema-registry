/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AsyncRestServiceTest {

  private static final long TIMEOUT_SECONDS = 10;

  private FakeSchemaRegistry registry;
  private AsyncRestService restService;

  @Before
  public void setUp() throws IOException {
    registry = new FakeSchemaRegistry();
  }

  @After
  public void tearDown() throws IOException {
    if (restService != null) {
      restService.close();
    }
    registry.close();
  }

  @Test
  public void testGetIdSendsGetAndParsesResponse() throws Exception {
    registry.enqueue(200, "{\"schema\": \"\\\"string\\\"\"}");
    restService = newRestService(Collections.emptyMap(), registry.url());

    SchemaString result = await(restService.getId(42, "my-subject"));

    assertEquals("\"string\"", result.getSchemaString());
    RecordedRequest request = registry.onlyRequest();
    assertEquals("GET", request.method);
    assertEquals("/schemas/ids/42?fetchMaxId=false&subject=my-subject", request.uri);
    assertEquals("true", request.headers.get(RestService.ACCEPT_UNKNOWN_PROPERTIES));
  }

  @Test
  public void testRegisterSchemaPostsRequestBody() throws Exception {
    registry.enqueue(200, "{\"id\": 7}");
    restService = newRestService(Collections.emptyMap(), registry.url());
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema("\"string\"");

    RegisterSchemaResponse response = await(restService.registerSchema(request, "foo", true));

    assertEquals(7, response.getId());
    RecordedRequest recorded = registry.onlyRequest();
    assertEquals("POST", recorded.method);
    assertEquals("/subjects/foo/versions?normalize=true", recorded.uri);
    assertEquals(request, RegisterSchemaRequest.fromJson(recorded.body));
    assertEquals(Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED, recorded.headers.get("Content-Type"));
  }

  @Test
  public void testResponseIsDecodedAsUtf8WithoutCharset() throws Exception {
    // FakeSchemaRegistry sends Content-Type application/json with no charset parameter
    registry.enqueue(200, "{\"schema\": \"café\"}");
    restService = newRestService(Collections.emptyMap(), registry.url());

    SchemaString result = await(restService.getId(1, null));

    assertEquals("café", result.getSchemaString());
  }

  @Test
  public void testNoContentReturnsNull() throws Exception {
    registry.enqueue(204, null);
    restService = newRestService(Collections.emptyMap(), registry.url());

    assertNull(await(restService.getConfig("foo", false)));
  }

  @Test
  public void testErrorResponseFailsWithRestClientException() throws Exception {
    registry.enqueue(404, "{\"error_code\": 40403, \"message\": \"Schema not found\"}");
    restService = newRestService(retries(3), registry.url());

    RestClientException e = awaitFailure(restService.getId(1, null), RestClientException.class);

    assertEquals(404, e.getStatus());
    assertEquals(40403, e.getErrorCode());
    assertEquals("Schema not found; error code: 40403", e.getMessage());
    // 404 is not retriable
    assertEquals(1, registry.requests.size());
  }

  @Test
  public void testUnparseableErrorResponse() throws Exception {
    registry.enqueue(400, "not json");
    restService = newRestService(Collections.emptyMap(), registry.url());

    RestClientException e = awaitFailure(restService.getId(1, null), RestClientException.class);

    assertEquals(400, e.getStatus());
    assertEquals(50005, e.getErrorCode());
    assertTrue(e.getMessage().contains("not json"));
  }

  @Test
  public void testRetriesRetriableErrorThenSucceeds() throws Exception {
    registry.enqueue(500, "{\"error_code\": 50001, \"message\": \"boom\"}");
    registry.enqueue(500, "{\"error_code\": 50001, \"message\": \"boom\"}");
    registry.enqueue(200, "{\"schema\": \"\\\"string\\\"\"}");
    restService = newRestService(retries(2), registry.url());

    SchemaString result = await(restService.getId(1, null));

    assertEquals("\"string\"", result.getSchemaString());
    assertEquals(3, registry.requests.size());
  }

  @Test
  public void testRetriesExhausted() throws Exception {
    for (int i = 0; i < 2; i++) {
      registry.enqueue(500, "{\"error_code\": 50001, \"message\": \"boom\"}");
    }
    restService = newRestService(retries(1), registry.url());

    RestClientException e = awaitFailure(restService.getId(1, null), RestClientException.class);

    assertEquals(500, e.getStatus());
    assertEquals(2, registry.requests.size());
  }

  @Test
  public void testFailsOverToNextUrlOnConnectionError() throws Exception {
    registry.enqueue(200, "{\"schema\": \"\\\"string\\\"\"}");
    String deadUrl = "http://127.0.0.1:" + unusedPort();
    restService = newRestService(Collections.emptyMap(), deadUrl, registry.url());

    SchemaString result = await(restService.getId(1, null));

    assertEquals("\"string\"", result.getSchemaString());
    assertEquals(1, registry.requests.size());
    // The dead URL is moved past, so later requests go straight to the live one
    assertEquals(registry.url(), restService.getBaseUrls().current());
  }

  @Test
  public void testDoesNotFailOverOnNonRetriableError() throws Exception {
    registry.enqueue(404, "{\"error_code\": 40403, \"message\": \"Schema not found\"}");
    FakeSchemaRegistry secondRegistry = new FakeSchemaRegistry();
    try {
      restService = newRestService(Collections.emptyMap(), registry.url(), secondRegistry.url());

      awaitFailure(restService.getId(1, null), RestClientException.class);

      assertEquals(1, registry.requests.size());
      assertEquals(0, secondRegistry.requests.size());
    } finally {
      secondRegistry.close();
    }
  }

  @Test
  public void testBasicAuthHeader() throws Exception {
    registry.enqueue(200, "{\"schema\": \"\\\"string\\\"\"}");
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
    configs.put(SchemaRegistryClientConfig.USER_INFO_CONFIG, "user:password");
    restService = newRestService(configs, registry.url());

    await(restService.getId(1, null));

    assertEquals("Basic dXNlcjpwYXNzd29yZA==",
        registry.onlyRequest().headers.get("Authorization"));
  }

  @Test
  public void testCustomHttpHeaders() throws Exception {
    registry.enqueue(200, "{\"schema\": \"\\\"string\\\"\"}");
    restService = new AsyncRestService(new UrlList(registry.url()), Collections.emptyMap(),
        null, null, Collections.singletonMap("X-Custom", "value"));

    await(restService.getId(1, null));

    assertEquals("value", registry.onlyRequest().headers.get("X-Custom"));
  }

  @Test
  public void testTestCompatibility() throws Exception {
    registry.enqueue(200, "{\"is_compatible\": true}");
    registry.enqueue(200, "{\"is_compatible\": false, \"messages\": [\"field removed\"]}");
    registry.enqueue(200, "{\"is_compatible\": false}");
    restService = newRestService(Collections.emptyMap(), registry.url());
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema("\"string\"");

    assertEquals(Collections.emptyList(),
        await(restService.testCompatibility(request, "foo", "latest", false, true)));
    assertEquals(Collections.singletonList("field removed"),
        await(restService.testCompatibility(request, "foo", "latest", false, true)));
    assertEquals(Collections.singletonList("Schemas are incompatible"),
        await(restService.testCompatibility(request, "foo", "latest", false, true)));
    assertEquals("/compatibility/subjects/foo/versions/latest?normalize=false&verbose=true",
        registry.requests.get(0).uri);
  }

  @Test
  public void testCallbacksRunOnSuppliedExecutor() throws Exception {
    registry.enqueue(200, "{\"schema\": \"\\\"string\\\"\"}");
    AtomicInteger tasks = new AtomicInteger();
    Executor countingExecutor = task -> {
      tasks.incrementAndGet();
      ForkJoinPool.commonPool().execute(task);
    };
    restService = new AsyncRestService(new UrlList(registry.url()), Collections.emptyMap(),
        null, null, null, countingExecutor);

    await(restService.getId(1, null));

    assertTrue("expected response handling to use the supplied executor", tasks.get() > 0);
  }

  @Test
  public void testRequestAfterCloseFails() throws Exception {
    restService = newRestService(Collections.emptyMap(), registry.url());
    restService.close();

    CompletableFuture<SchemaString> future = restService.getId(1, null);

    try {
      future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      fail("Expected the request to fail after close");
    } catch (ExecutionException | CancellationException expected) {
      // either is fine: the client must not hang or succeed
    }
    assertEquals(0, registry.requests.size());
    restService = null;
  }

  private static Map<String, Object> retries(int maxRetries) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.MAX_RETRIES_CONFIG, maxRetries);
    configs.put(SchemaRegistryClientConfig.RETRIES_WAIT_MS_CONFIG, 1);
    configs.put(SchemaRegistryClientConfig.RETRIES_MAX_WAIT_MS_CONFIG, 5);
    return configs;
  }

  private static AsyncRestService newRestService(Map<String, ?> configs, String... urls) {
    return new AsyncRestService(new UrlList(Arrays.asList(urls)), configs, null, null, null);
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
    return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private static <E extends Throwable> E awaitFailure(CompletableFuture<?> future,
                                                      Class<E> expected) throws Exception {
    try {
      future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      assertTrue("Expected " + expected.getSimpleName() + " but got " + e.getCause(),
          expected.isInstance(e.getCause()));
      return expected.cast(e.getCause());
    }
    fail("Expected " + expected.getSimpleName());
    return null;
  }

  private static int unusedPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static class RecordedRequest {
    final String method;
    final String uri;
    final Map<String, String> headers;
    final String body;

    RecordedRequest(String method, String uri, Map<String, String> headers, String body) {
      this.method = method;
      this.uri = uri;
      this.headers = headers;
      this.body = body;
    }
  }

  private static class CannedResponse {
    final int status;
    final String body;

    CannedResponse(int status, String body) {
      this.status = status;
      this.body = body;
    }
  }

  /**
   * A local HTTP server that records each request and replies with queued responses in order.
   */
  private static class FakeSchemaRegistry implements AutoCloseable {
    private final HttpServer server;
    private final Queue<CannedResponse> responses = new ConcurrentLinkedQueue<>();
    final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();

    FakeSchemaRegistry() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/", this::handle);
      server.start();
    }

    String url() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    /**
     * Queues a response; a null body sends no content.
     */
    void enqueue(int status, String body) {
      responses.add(new CannedResponse(status, body));
    }

    RecordedRequest onlyRequest() {
      assertEquals(1, requests.size());
      return requests.get(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
      Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      exchange.getRequestHeaders().forEach((name, values) -> headers.put(name, values.get(0)));
      String requestBody =
          new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      requests.add(new RecordedRequest(exchange.getRequestMethod(),
          exchange.getRequestURI().toString(), headers, requestBody));

      CannedResponse response = responses.poll();
      if (response == null) {
        response = new CannedResponse(500,
            "{\"error_code\": 50000, \"message\": \"no response queued\"}");
      }
      if (response.body == null) {
        exchange.sendResponseHeaders(response.status, -1);
      } else {
        byte[] bytes = response.body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(response.status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(bytes);
        }
      }
      exchange.close();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
