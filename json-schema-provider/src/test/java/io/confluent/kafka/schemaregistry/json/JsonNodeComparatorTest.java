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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JsonNodeComparatorTest {

  private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

  private HttpServer server;
  private final AtomicInteger hits = new AtomicInteger();

  @Before
  public void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/a", exchange -> respond(exchange, "{\"type\":\"string\"}"));
    server.createContext("/b", exchange -> respond(exchange, "{\"type\":\"integer\"}"));
    server.start();
  }

  private void respond(HttpExchange exchange, String json) throws IOException {
    hits.incrementAndGet();
    byte[] body = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, body.length);
    exchange.getResponseBody().write(body);
    exchange.close();
  }

  @After
  public void stopServer() {
    server.stop(0);
  }

  private String url(String path) {
    return "http://127.0.0.1:" + server.getAddress().getPort() + path;
  }

  private static JsonNode node(String json) throws IOException {
    return MAPPER.readTree(json);
  }

  private static List<String> sorted(JsonNodeComparator comparator, JsonNode... nodes) {
    List<JsonNode> list = new ArrayList<>();
    Collections.addAll(list, nodes);
    list.sort(comparator);
    return list.stream().map(JsonNode::toString).collect(Collectors.toList());
  }

  private static org.everit.json.schema.loader.SchemaClient blockingClient() {
    return new JsonSchema("{\"type\":\"string\"}", Collections.emptyList(),
        Collections.emptyMap(), null, null, null, true).everitSchemaClient();
  }

  @Test
  public void defaultClientFetchesRefsAndOrdersByFetchedContent() throws IOException {
    JsonNode refA = node("{\"$ref\":\"" + url("/a") + "\"}");
    JsonNode refB = node("{\"$ref\":\"" + url("/b") + "\"}");

    List<String> ab = sorted(new JsonNodeComparator(), refA, refB);
    List<String> ba = sorted(new JsonNodeComparator(), refB, refA);

    assertTrue(hits.get() > 0);
    // Order is decided by the fetched schemas, not by the input order.
    assertEquals(ab, ba);
    // A null client is the default path.
    assertEquals(ab, sorted(new JsonNodeComparator(null), refB, refA));
    // The two refs must not tie, otherwise the checks above prove nothing.
    assertNotEquals(0, new JsonNodeComparator().compare(refA, refB));
  }

  @Test
  public void blockedClientRejectsRefsWithoutFetching() throws IOException {
    JsonNode refA = node("{\"$ref\":\"" + url("/a") + "\"}");
    JsonNode refB = node("{\"$ref\":\"" + url("/b") + "\"}");
    try {
      sorted(new JsonNodeComparator(blockingClient()), refB, refA);
      fail("Expected the remote $ref fetch to be rejected");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(JsonSchema.REMOTE_REF_DISABLED_MESSAGE));
    }
    assertEquals(0, hits.get());
  }

  @Test
  public void refFreeBranchesOrderedIdenticallyWithAndWithoutClient() throws IOException {
    JsonNode string = node("{\"type\":\"string\"}");
    JsonNode integer = node("{\"type\":\"integer\"}");
    JsonNode object = node("{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"string\"}}}");

    assertEquals(
        sorted(new JsonNodeComparator(), object, integer, string),
        sorted(new JsonNodeComparator(blockingClient()), object, integer, string));
    assertEquals(0, hits.get());
  }

  private static JsonSchema oneOfSchema(String first, String second, boolean blockRemoteRefs) {
    String schema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"oneOf\":[{\"$ref\":\"" + first + "\"},{\"$ref\":\"" + second + "\"}]}";
    return new JsonSchema(schema, Collections.emptyList(), Collections.emptyMap(),
        null, null, null, blockRemoteRefs);
  }

  private static Map<SchemaEntity, Set<String>> tagFirstBranch() {
    return Collections.singletonMap(
        new SchemaEntity("oneof.0.object", SchemaEntity.EntityType.SR_RECORD),
        Collections.singleton("TAG"));
  }

  private static String taggedRef(JsonSchema schema) throws IOException {
    JsonNode result = MAPPER.readTree(
        schema.copy(tagFirstBranch(), Collections.emptyMap()).canonicalString());
    for (JsonNode branch : result.get("oneOf")) {
      if (branch.has("confluent:tags")) {
        return branch.get("$ref").asText();
      }
    }
    throw new AssertionError("No branch was tagged: " + result);
  }

  @Test
  public void copyWithTagsUsesFetchedOrderWhenNotBlocked() throws IOException {
    // The tag lands on the branch that sorts first, whatever the declared order is.
    JsonNode refA = node("{\"$ref\":\"" + url("/a") + "\"}");
    JsonNode refB = node("{\"$ref\":\"" + url("/b") + "\"}");
    final String expected = MAPPER.readTree(
        sorted(new JsonNodeComparator(), refB, refA).get(0)).get("$ref").asText();
    hits.set(0);

    String declaredBA = taggedRef(oneOfSchema(url("/b"), url("/a"), false));
    String declaredAB = taggedRef(oneOfSchema(url("/a"), url("/b"), false));

    assertTrue(hits.get() > 0);
    assertEquals(declaredBA, declaredAB);
    assertEquals(expected, declaredBA);
  }

  @Test
  public void copyWithTagsRejectsRefsWhenBlocked() {
    try {
      oneOfSchema(url("/b"), url("/a"), true).copy(tagFirstBranch(), Collections.emptyMap());
      fail("Expected the remote $ref fetch to be rejected");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(JsonSchema.REMOTE_REF_DISABLED_MESSAGE));
    }
    assertEquals(0, hits.get());
  }
}
