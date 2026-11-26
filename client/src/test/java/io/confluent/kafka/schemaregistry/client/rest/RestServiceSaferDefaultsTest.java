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

import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.Test;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// Pins down the safer client defaults from issue #4588: during a Schema
// Registry outage the client should bound its connections, fail fast, and
// back off instead of flooding the service with new connections and retries.
public class RestServiceSaferDefaultsTest {

  @Test
  public void testTimeoutDefaultsAreShort() {
    Map<String, Object> configs = Collections.emptyMap();
    assertEquals(800,
        (int) SchemaRegistryClientConfig.getHttpConnectTimeoutMs(configs));
    assertEquals(6000,
        (int) SchemaRegistryClientConfig.getHttpReadTimeoutMs(configs));
    assertEquals(500,
        (int) SchemaRegistryClientConfig.getHttpConnectionRequestTimeoutMs(configs));
  }

  @Test
  public void testRetryBackoffDefaultsAreLonger() {
    Map<String, Object> configs = Collections.emptyMap();
    assertEquals(2000,
        (int) SchemaRegistryClientConfig.getRetriesWaitMs(configs));
    assertEquals(30000,
        (int) SchemaRegistryClientConfig.getRetriesMaxWaitMs(configs));
  }

  @Test
  public void testPoolDefaultsAreBounded() {
    Map<String, Object> configs = Collections.emptyMap();
    assertEquals(10,
        (int) SchemaRegistryClientConfig.getHttpClientPoolMaxTotalConnections(configs));
    assertEquals(10,
        (int) SchemaRegistryClientConfig.getHttpClientPoolMaxPerRouteConnections(configs));
  }

  @Test
  public void testPoolAndTimeoutConfigsCanBeOverridden() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.HTTP_CLIENT_POOL_MAX_TOTAL_CONNECTIONS, "25");
    configs.put(SchemaRegistryClientConfig.HTTP_CLIENT_POOL_MAX_PER_ROUTE_CONNECTIONS, "7");
    configs.put(SchemaRegistryClientConfig.HTTP_CONNECTION_REQUEST_TIMEOUT_MS, "1500");
    assertEquals(25,
        (int) SchemaRegistryClientConfig.getHttpClientPoolMaxTotalConnections(configs));
    assertEquals(7,
        (int) SchemaRegistryClientConfig.getHttpClientPoolMaxPerRouteConnections(configs));
    assertEquals(1500,
        (int) SchemaRegistryClientConfig.getHttpConnectionRequestTimeoutMs(configs));
  }

  @Test
  public void testApacheHttpClientIsUsedByDefault() {
    assertTrue(SchemaRegistryClientConfig.useApacheHttpClient(Collections.emptyMap()));

    RestService restService = new RestService("http://localhost:8081");
    restService.configure(Collections.emptyMap());
    assertNotNull(restService.getApacheHttpClient());
  }

  @Test
  public void testApacheHttpClientCanBeDisabled() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.USE_APACHE_HTTP_CLIENT, "false");

    RestService restService = new RestService("http://localhost:8081");
    restService.configure(configs);
    assertNull(restService.getApacheHttpClient());
  }

  @Test
  public void testApacheConstructorHasBoundedPoolDefaults() throws Exception {
    RestService restService = new RestService("http://localhost:8081", false, true);
    assertEquals(500, restServiceField(restService, "httpConnectionRequestTimeoutMs"));
    assertEquals(10, restServiceField(restService, "httpClientPoolMaxTotalConnections"));
    assertEquals(10, restServiceField(restService, "httpClientPoolMaxPerRouteConnections"));

    // Configuration and setter overrides still win over the constructor defaults.
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.HTTP_CONNECTION_REQUEST_TIMEOUT_MS, "1500");
    configs.put(SchemaRegistryClientConfig.HTTP_CLIENT_POOL_MAX_TOTAL_CONNECTIONS, "25");
    restService.configure(configs);
    assertEquals(1500, restServiceField(restService, "httpConnectionRequestTimeoutMs"));
    assertEquals(25, restServiceField(restService, "httpClientPoolMaxTotalConnections"));

    restService.setHttpClientPoolMaxPerRouteConnections(7);
    assertEquals(7, restServiceField(restService, "httpClientPoolMaxPerRouteConnections"));
  }

  @Test
  public void testUrlConstructorAppliesDefaultsWithoutConfigure() throws Exception {
    CachedSchemaRegistryClient client =
        new CachedSchemaRegistryClient("http://localhost:8081", 100);
    RestService restService = clientRestService(client);
    assertEquals(800, restServiceField(restService, "httpConnectTimeoutMs"));
    assertEquals(6000, restServiceField(restService, "httpReadTimeoutMs"));
    assertEquals(500, restServiceField(restService, "httpConnectionRequestTimeoutMs"));
    assertEquals(10, restServiceField(restService, "httpClientPoolMaxTotalConnections"));
    assertEquals(10, restServiceField(restService, "httpClientPoolMaxPerRouteConnections"));
    assertNotNull(restService.getApacheHttpClient());
  }

  @Test
  public void testUrlConstructorPreservesExplicitTransportOverride() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.USE_APACHE_HTTP_CLIENT, "false");
    CachedSchemaRegistryClient client =
        new CachedSchemaRegistryClient("http://localhost:8081", 100, configs);
    RestService restService = clientRestService(client);
    assertNull(restService.getApacheHttpClient());
    assertEquals(800, restServiceField(restService, "httpConnectTimeoutMs"));
    assertEquals(6000, restServiceField(restService, "httpReadTimeoutMs"));
  }

  @Test
  public void testApacheClientDoesNotRetryOn503() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/", exchange -> {
      requestCount.incrementAndGet();
      byte[] body = "{\"error_code\": 50301, \"message\": \"unavailable\"}"
          .getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(503, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    });
    server.start();
    try {
      RestService restService = new RestService(
          "http://localhost:" + server.getAddress().getPort(), false, true);
      Map<String, Object> configs = new HashMap<>();
      configs.put(SchemaRegistryClientConfig.MAX_RETRIES_CONFIG, "0");
      restService.configure(configs);

      try {
        restService.getId(1);
        fail("Expected a RestClientException for the 503 response");
      } catch (RestClientException e) {
        assertEquals(503, e.getStatus());
      }
      // Apache's own automatic retries would re-send the request even with
      // max.retries=0; with them disabled the server sees exactly one request.
      assertEquals(1, requestCount.get());
    } finally {
      server.stop(0);
    }
  }

  private static int restServiceField(RestService restService, String name) throws Exception {
    Field field = RestService.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.getInt(restService);
  }

  private static RestService clientRestService(CachedSchemaRegistryClient client)
      throws Exception {
    Field field = CachedSchemaRegistryClient.class.getDeclaredField("restService");
    field.setAccessible(true);
    return (RestService) field.get(client);
  }
}
