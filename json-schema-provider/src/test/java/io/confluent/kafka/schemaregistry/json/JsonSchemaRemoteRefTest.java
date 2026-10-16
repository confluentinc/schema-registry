/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@code blockRemoteRefs} behavior of {@link JsonSchema}: when set, a
 * {@code $ref}/{@code $dynamicRef} to an unregistered http/https document is rejected rather
 * than fetched over the network.
 */
public class JsonSchemaRemoteRefTest {

  private static final String DRAFT7 = "\"$schema\":\"http://json-schema.org/draft-07/schema#\"";

  private HttpServer server;
  private final AtomicInteger hits = new AtomicInteger();

  @Before
  public void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/", this::respond);
    server.start();
  }

  private void respond(HttpExchange exchange) throws IOException {
    hits.incrementAndGet();
    byte[] body = "{\"type\":\"string\"}".getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, body.length);
    exchange.getResponseBody().write(body);
    exchange.close();
  }

  @After
  public void stopServer() {
    server.stop(0);
  }

  private int port() {
    return server.getAddress().getPort();
  }

  private static String draft7WithRef(String ref) {
    return "{" + DRAFT7 + ",\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\""
        + ref + "\"}}}";
  }

  private static JsonSchema schema(
      String schemaString,
      Map<String, String> resolvedReferences,
      boolean blockRemoteRefs) {
    return new JsonSchema(
        schemaString,
        Collections.emptyList(),
        resolvedReferences,
        null,
        null,
        null,
        blockRemoteRefs);
  }

  private static void assertBlocked(Throwable t) {
    if (!JsonSchema.isRemoteRefBlocked(t)) {
      throw new AssertionError("Expected a 'remote fetch disabled' cause but was: " + t, t);
    }
  }

  private static void assertNotBlocked(JsonSchema schema) {
    try {
      schema.rawSchema();
    } catch (RuntimeException e) {
      if (JsonSchema.isRemoteRefBlocked(e)) {
        throw new AssertionError("Schema must not be blocked, but it was: " + e, e);
      }
    }
  }

  private static void assertRefRejected(String schemaString) {
    try {
      schema(schemaString, Collections.emptyMap(), true).rawSchema();
      fail("Expected loading to fail because the remote $ref fetch is disabled");
    } catch (RuntimeException e) {
      assertBlocked(e);
    }
  }

  @Test
  public void localSchemaLoadsWhenBlocked_latestDraft() {
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
    assertNotNull(schema(s, Collections.emptyMap(), true).rawSchema());
  }

  @Test
  public void localSchemaLoadsWhenBlocked_draft7() {
    String s = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
    assertNotNull(schema(s, Collections.emptyMap(), true).rawSchema());
  }

  @Test
  public void httpRefRejectedWhenBlocked_latestDraft() {
    assertRefRejected("{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}");
  }

  @Test
  public void httpsRefRejectedWhenBlocked_draft7() {
    assertRefRejected("{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"https://example.com/r.json\"}}}");
  }

  @Test
  public void dynamicRefRejectedWhenBlocked_latestDraft() {
    assertRefRejected("{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"properties\":{\"a\":{\"$dynamicRef\":\"http://example.com/r.json#node\"}}}");
  }

  @Test
  public void registeredHttpRefNotBlocked_latestDraft() {
    String ref = "http://acme.com/widget.json";
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"" + ref + "\"}}}";
    Map<String, String> resolvedRefs = Collections.singletonMap(
        ref, "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"string\"}");
    // A registered reference resolves from the in-memory mappings, not the network.
    assertNotNull(schema(s, resolvedRefs, true).rawSchema());
  }

  @Test
  public void registeredHttpRefNotBlocked_draft7() {
    String ref = "http://acme.com/widget.json";
    String s = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"" + ref + "\"}}}";
    Map<String, String> resolvedRefs = Collections.singletonMap(ref, "{\"type\":\"string\"}");
    assertNotNull(schema(s, resolvedRefs, true).rawSchema());
  }

  @Test
  public void notBlockedWhenBlockRemoteRefsFalse() {
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"properties\":{\"a\":{\"$ref\":\"http://nonexistent.invalid/r.json\"}}}";
    assertNotBlocked(schema(s, Collections.emptyMap(), false));
  }

  // The registration pipeline copies/normalizes the parsed schema before it is loaded, so the
  // block must survive those operations.

  @Test
  public void blockSurvivesCopyWithVersion() {
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}";
    JsonSchema copy = schema(s, Collections.emptyMap(), true).copy(0);
    assertTrue(copy instanceof JsonSchema);
    try {
      copy.rawSchema();
      fail("Expected the copy to still block the remote $ref fetch");
    } catch (RuntimeException e) {
      assertBlocked(e);
    }
  }

  @Test
  public void blockSurvivesNormalize() {
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}";
    JsonSchema normalized = schema(s, Collections.emptyMap(), true).normalize();
    try {
      normalized.rawSchema();
      fail("Expected the normalized copy to still block the remote $ref fetch");
    } catch (RuntimeException e) {
      assertBlocked(e);
    }
  }

  @Test
  public void blockedPreviousSurfacesAsIncompatibilityNotError() {
    String previous = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}";
    String current = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
    List<String> errors = schema(current, Collections.emptyMap(), true)
        .isBackwardCompatible(schema(previous, Collections.emptyMap(), true));
    assertFalse(errors.isEmpty());
    assertTrue(errors.get(0).contains("unsupported external reference"));
  }

  // isBlockedRef

  @Test
  public void isBlockedRefBlocksNonLocalSchemes() {
    assertTrue(JsonSchema.isBlockedRef("http://example.com/x.json"));
    assertTrue(JsonSchema.isBlockedRef("https://example.com/x.json"));
    assertTrue(JsonSchema.isBlockedRef("ftp://example.com/x.json"));
    assertTrue(JsonSchema.isBlockedRef("jrt:/java.base/schema.json"));
    assertTrue(JsonSchema.isBlockedRef("mailto:a@example.com"));
  }

  @Test
  public void isBlockedRefUnwrapsJar() {
    assertTrue(JsonSchema.isBlockedRef("jar:http://example.com/x.jar!/s.json"));
    assertTrue(JsonSchema.isBlockedRef("jar:jar:http://example.com/x.jar!/y.jar!/s.json"));
    assertTrue(JsonSchema.isBlockedRef("jar:ftp://example.com/x.jar!/s.json"));
    assertTrue(JsonSchema.isBlockedRef("jar:file://example.com/x.jar!/s.json"));
    assertFalse(JsonSchema.isBlockedRef("jar:file:/local.jar!/s.json"));
  }

  @Test
  public void isBlockedRefDeepJarNestingFailsClosed() {
    StringBuilder deep = new StringBuilder();
    for (int i = 0; i < 10_000; i++) {
      deep.append("jar:");
    }
    assertTrue(JsonSchema.isBlockedRef(deep + "http://example.com/x.jar!/s.json"));
    assertTrue(JsonSchema.isBlockedRef(deep + "file:/local.jar!/s.json"));
    // Unparseable input takes the string-based path, which is also bounded.
    assertTrue(JsonSchema.isBlockedRef(deep + "ftp://example.com/x{.jar!/s.json"));
  }

  @Test
  public void isBlockedRefFileAuthority() {
    assertTrue(JsonSchema.isBlockedRef("file://example.com/x.json"));
    assertTrue(JsonSchema.isBlockedRef("file://example_host/x.json"));
    assertFalse(JsonSchema.isBlockedRef("file:/x.json"));
    assertFalse(JsonSchema.isBlockedRef("file:///x.json"));
  }

  @Test
  public void isBlockedRefUnparseableSchemeShapedFailsClosed() {
    assertTrue(JsonSchema.isBlockedRef("ftp://example.com/x{.json"));
    assertTrue(JsonSchema.isBlockedRef("jar:ftp://example.com/x{.jar!/s.json"));
    assertTrue(JsonSchema.isBlockedRef(" jar: http://example.com/x.jar!/s.json"));
    assertTrue(JsonSchema.isBlockedRef("file://example.com/a b.json"));
  }

  @Test
  public void isBlockedRefAllowsLocalAndSchemeless() {
    assertFalse(JsonSchema.isBlockedRef(null));
    assertFalse(JsonSchema.isBlockedRef("classpath:/x.json"));
    assertFalse(JsonSchema.isBlockedRef("classpath://a b.json"));
    assertFalse(JsonSchema.isBlockedRef("file:///local/a b.json"));
    assertFalse(JsonSchema.isBlockedRef("common/addr.json"));
    assertFalse(JsonSchema.isBlockedRef("not a uri"));
  }

  // Loader-level behavior

  @Test
  public void jarWrappedHttpRefRejectedWithoutFetchWhenBlocked() {
    assertRefRejected(draft7WithRef("jar:http://127.0.0.1:" + port() + "/x.jar!/s.json"));
    assertEquals(0, hits.get());
  }

  @Test
  public void fileAuthorityRefRejectedWhenBlocked() {
    assertRefRejected(draft7WithRef("file://127.0.0.1/x"));
    assertEquals(0, hits.get());
  }

  @Test
  public void blockMessageOmitsUserinfoAndQuery() {
    String ref = "http://userinfo-marker:pw@127.0.0.1:" + port() + "/r.json?query-marker=1";
    try {
      schema(draft7WithRef(ref), Collections.emptyMap(), true).rawSchema();
      fail("Expected loading to fail because the remote $ref fetch is disabled");
    } catch (RuntimeException e) {
      assertBlocked(e);
      for (Throwable c = e; c != null; c = c.getCause()) {
        String message = String.valueOf(c.getMessage());
        assertFalse(message, message.contains("userinfo-marker"));
        assertFalse(message, message.contains("query-marker"));
      }
    }
    assertEquals(0, hits.get());
  }

  @Test
  public void httpRefStillFetchedWhenNotBlocked() {
    String ref = "http://127.0.0.1:" + port() + "/r.json";
    assertNotNull(schema(draft7WithRef(ref), Collections.emptyMap(), false).rawSchema());
    assertTrue(hits.get() > 0);
  }
}
