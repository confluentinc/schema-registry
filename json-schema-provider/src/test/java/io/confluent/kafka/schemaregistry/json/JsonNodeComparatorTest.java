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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
      sorted(new JsonNodeComparator(b -> b.schemaClient(blockingClient())), refB, refA);
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
        sorted(new JsonNodeComparator(b -> b.schemaClient(blockingClient())),
            object, integer, string));
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
      assertEquals(JsonSchema.REMOTE_REF_DISABLED_MESSAGE, e.getMessage());
      assertTrue(JsonSchema.isRemoteRefBlocked(e.getCause()));
    }
    assertEquals(0, hits.get());
  }

  @Test
  public void copyWithTagsResolvesRegisteredRefsWhenBlocked() throws IOException {
    String refName = "http://acme.example/widget.json";
    String schema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"oneOf\":[{\"$ref\":\"" + refName + "\"},"
        + "{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"string\"}}}]}";
    JsonSchema jsonSchema = new JsonSchema(schema,
        Collections.singletonList(new SchemaReference(refName, "widget", 1)),
        Collections.singletonMap(refName, "{\"type\":\"string\"}"),
        null, null, null, true);

    JsonNode result = MAPPER.readTree(
        jsonSchema.copy(tagFirstBranch(), Collections.emptyMap()).canonicalString());

    // The registered ref resolves locally; the object branch sorts first and gets the tag.
    assertTrue(result.toString(), result.get("oneOf").get(1).has("confluent:tags"));
    assertEquals(0, hits.get());
  }

  private static final String DRAFT_7 = "\"$schema\":\"http://json-schema.org/draft-07/schema#\"";

  private static JsonSchema oneOf(String prefix, Map<String, String> resolved, boolean block,
      String... branches) {
    String json = "{" + DRAFT_7 + prefix + ",\"oneOf\":[" + String.join(",", branches) + "]}";
    return new JsonSchema(json, Collections.emptyList(), resolved, null, null, null, block);
  }

  private static JsonNode tagged(JsonSchema schema, int index) throws IOException {
    SchemaEntity entity =
        new SchemaEntity("oneof." + index + ".object", SchemaEntity.EntityType.SR_RECORD);
    return MAPPER.readTree(schema.copy(
        Collections.singletonMap(entity, Collections.singleton("T")),
        Collections.emptyMap()).canonicalString());
  }

  // For each sorted index, the declared index of the branch that receives the tag.
  private static int[] order(JsonSchema schema) throws IOException {
    JsonNode declared = schema.toJsonNode().get("oneOf");
    int[] result = new int[declared.size()];
    for (int k = 0; k < result.length; k++) {
      result[k] = -1;
      JsonNode branches = tagged(schema, k).get("oneOf");
      for (int i = 0; i < branches.size(); i++) {
        if (branches.get(i).has("confluent:tags")) {
          ObjectNode hit = (ObjectNode) branches.get(i).deepCopy();
          hit.remove("confluent:tags");
          assertEquals(declared.get(i), hit);
          result[k] = i;
        }
      }
    }
    return result;
  }

  @Test
  public void copyWithTagsKeepsOrderOfBranchesWithOwnDefinitions() throws IOException {
    // Recorded from the loader before branches could resolve against the root.
    for (boolean block : new boolean[] {false, true}) {
      JsonSchema schema = oneOf(",\"definitions\":{\"d\":{\"type\":\"string\"}}",
          Collections.emptyMap(), block,
          "{\"definitions\":{\"d\":{\"type\":\"integer\"}},\"$ref\":\"#/definitions/d\"}",
          "{\"type\":\"string\"}",
          "{\"definitions\":{\"d\":{\"type\":\"object\"}},\"$ref\":\"#/definitions/d\"}",
          "{\"type\":\"object\",\"definitions\":{\"d\":{\"type\":\"integer\"}},"
              + "\"properties\":{\"p\":{\"$ref\":\"#/definitions/d\"}}}",
          "{\"type\":\"integer\"}");
      assertArrayEquals(new int[] {4, 3, 0, 2, 1}, order(schema));
    }
  }

  @Test
  public void copyWithTagsResolvesBranchRefsAgainstRootWhenBlocked() throws IOException {
    JsonSchema relative = oneOf(",\"$id\":\"http://acme.example/root.json\"",
        Collections.singletonMap("http://acme.example/widget.json", "{\"type\":\"string\"}"),
        true, "{\"$ref\":\"widget.json\"}", "{\"type\":\"integer\"}",
        "{\"type\":\"boolean\"}");
    assertArrayEquals(new int[] {2, 1, 0}, order(relative));

    JsonSchema local = oneOf(",\"definitions\":{\"d\":{\"type\":\"integer\"}}",
        Collections.emptyMap(), true, "{\"$ref\":\"#/definitions/d\"}",
        "{\"type\":\"string\"}", "{\"type\":\"boolean\"}");
    assertArrayEquals(new int[] {2, 0, 1}, order(local));
  }

  @Test
  public void copyWithTagsReportsUnloadableBranchAsIllegalArgument() {
    for (boolean block : new boolean[] {false, true}) {
      assertBranchRejected(oneOf("", Collections.emptyMap(), block,
          "{\"$ref\":\"#/definitions/missing\"}", "{\"type\":\"string\"}"));
    }
    assertBranchRejected(oneOf("", Collections.emptyMap(), false,
        "{\"$ref\":\"http://127.0.0.1:1/x\"}", "{\"type\":\"string\"}"));
  }

  @Test
  public void copyWithTagsWritesWhereInlineTaggedEntitiesReads() throws IOException {
    String objA = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
    String objB = "{\"type\":\"object\",\"default\":{\"b\":\"x\"},"
        + "\"properties\":{\"b\":{\"$ref\":\"#/definitions/s\"}}}";
    String defs = ",\"definitions\":{\"s\":{\"type\":\"string\"},"
        + "\"tree\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}},"
        + "\"list\":{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/list\"}},"
        + "\"self\":{\"type\":\"array\",\"items\":{\"$ref\":\"root.json\"}}}";
    for (boolean block : new boolean[] {false, true}) {
      assertTagsReadBack(oneOf(defs, Collections.emptyMap(), block, objB, objA,
          "{\"type\":\"string\",\"const\":\"c\"}", "{\"$ref\":\"#/definitions/tree\"}",
          "{\"$ref\":\"#/definitions/list\"}", "{\"$ref\":\"#/definitions/list\"}"));
      assertTagsReadBack(oneOf(defs, Collections.emptyMap(), block, objA, "true",
          "{\"$ref\":\"#/definitions/s\"}", objB));
      assertTagsReadBack(oneOf(",\"$id\":\"http://acme.example/root.json\"",
          Collections.emptyMap(), block, "{\"type\":\"string\",\"const\":\"b\"}",
          "{\"$ref\":\"#\"}", "{\"$ref\":\"#\"}", objA));
      assertTagsReadBack(oneOf(",\"$id\":\"http://acme.example/root.json\"" + defs,
          Collections.emptyMap(), block, objA, "{\"$ref\":\"#/definitions/self\"}", objB));
    }
  }

  // Each object branch tagged at oneof.k must be read back at oneof.k
  private static void assertTagsReadBack(JsonSchema schema) throws IOException {
    int checked = 0;
    for (int k = 0; k < schema.toJsonNode().get("oneOf").size(); k++) {
      SchemaEntity entity =
          new SchemaEntity("oneof." + k + ".object", SchemaEntity.EntityType.SR_RECORD);
      JsonSchema result;
      try {
        result = (JsonSchema) schema.copy(
            Collections.singletonMap(entity, Collections.singleton("T")), Collections.emptyMap());
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("can be tagged"));
        continue;
      }
      for (JsonNode branch : result.toJsonNode().get("oneOf")) {
        if (branch.has("confluent:tags") && "object".equals(branch.path("type").asText())) {
          assertEquals(result.toJsonNode().toString(), Collections.singleton("T"),
              result.inlineTaggedEntities().get(entity));
          checked++;
        }
      }
    }
    assertTrue(checked > 0);
  }

  private static void assertBranchRejected(JsonSchema schema) {
    try {
      tagged(schema, 0);
      fail("Expected the branch to be rejected");
    } catch (IllegalArgumentException e) {
      assertNotNull(e.getCause());
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void copyWithTagsKeepsTodaysSortForWrappedRootUnion() throws IOException {
    String json = "{" + DRAFT_7 + ",\"type\":\"object\","
        + "\"properties\":{\"h\":{\"type\":\"string\"}},"
        + "\"allOf\":[{\"type\":\"string\",\"const\":\"b\"},"
        + "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}]}";
    SchemaEntity entity =
        new SchemaEntity("allof.0.allof.1.object", SchemaEntity.EntityType.SR_RECORD);
    JsonSchema result = (JsonSchema) new JsonSchema(json).copy(
        Collections.singletonMap(entity, Collections.singleton("T")), Collections.emptyMap());
    assertEquals(Collections.singleton("T"), result.inlineTaggedEntities().get(entity));
  }

  @Test
  public void copyWithTagsRejectsPathsOutsideTheSchema() {
    JsonSchema schema = oneOf("", Collections.emptyMap(), true, "{\"type\":\"string\"}",
        "{\"type\":\"integer\"}");
    for (String path : new String[] {"oneof.2.object", "oneof.-1.object", "oneof.x.object"}) {
      SchemaEntity entity = new SchemaEntity(path, SchemaEntity.EntityType.SR_RECORD);
      try {
        schema.copy(Collections.singletonMap(entity, Collections.singleton("T")),
            Collections.emptyMap());
        fail("Expected " + path + " to be rejected");
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("No matching path"));
      }
    }
  }

  @Test
  public void copyWithTagsOutsideUnionsDoesNotLoadTheSchema() {
    // Properties named like union steps are not unions
    String json = "{" + DRAFT_7 + ",\"type\":\"object\",\"properties\":{"
        + "\"q\":{\"type\":\"string\"},"
        + "\"oneof\":{\"type\":\"object\",\"properties\":{\"q\":{\"type\":\"string\"}}},"
        + "\"anyof.q\":{\"type\":\"string\"},"
        + "\"r\":{\"$ref\":\"" + url("/a") + "\"}}}";
    Map<SchemaEntity, Set<String>> tags = new HashMap<>();
    for (String path : new String[] {"object.q", "object.oneof.object.q", "object.anyof.q"}) {
      tags.put(new SchemaEntity(path, SchemaEntity.EntityType.SR_FIELD),
          Collections.singleton("T"));
    }
    JsonSchema result = (JsonSchema) new JsonSchema(json, Collections.emptyList(),
        Collections.emptyMap(), null, null, null, false)
        .copy(tags, Collections.emptyMap());
    assertEquals(0, hits.get());
    assertEquals(tags, result.inlineTaggedEntities());
  }

  @Test
  public void copyWithTagsLoadsEachBranchOnce() {
    int n = 30;
    List<String> branches = new ArrayList<>();
    Map<SchemaEntity, Set<String>> tags = new HashMap<>();
    for (int i = 0; i < n; i++) {
      branches.add("{\"type\":\"object\",\"properties\":{\"p" + i + "\":{\"type\":\"string\"}}}");
      tags.put(new SchemaEntity("oneof." + i + ".object", SchemaEntity.EntityType.SR_RECORD),
          Collections.singleton("T" + i));
    }
    AtomicInteger configured = new AtomicInteger();
    String json = "{" + DRAFT_7 + ",\"oneOf\":[" + String.join(",", branches) + "]}";
    JsonSchema schema = new JsonSchema(json, Collections.emptyList(), Collections.emptyMap(),
        null, null, null, true) {
      @Override
      protected org.everit.json.schema.loader.SchemaClient everitSchemaClient() {
        configured.incrementAndGet();
        return super.everitSchemaClient();
      }
    };
    schema.copy(tags, Collections.emptyMap());
    // One check up front and one load of the whole schema; no branch is loaded on its own.
    assertEquals(2, configured.get());
  }
}
