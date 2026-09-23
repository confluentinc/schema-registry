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

package io.confluent.kafka.serializers.provenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * JSON Schema read with {@code use.provenance}: a property dropped and re-added across an interior
 * version is pruned from the document, since the reader's property is a new one.
 */
class JsonProvenanceDeserializerTest {

  private static final String TOPIC = "json";
  private static final String SUBJECT = TOPIC + "-value";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ProvenanceMockSchemaRegistryClient client;
  private KafkaJsonSchemaSerializer<Object> serializer;

  @BeforeEach
  void init() {
    client = new ProvenanceMockSchemaRegistryClient();
    serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
  }

  @Test
  void aPropertyDroppedAndReAddedIsPrunedAsANewOne() throws Exception {
    JsonSchema v1 = object(number("id"), string("note"));
    JsonSchema v2 = object(number("id"));
    // The description only keeps v3 from being deduplicated into v1, which it otherwise equals.
    JsonSchema v3 = object(number("id"),
        "\"note\": {\"type\": \"string\", \"description\": \"new\"}");
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode on = read(v3, bytes, "v1");
    assertEquals(7, on.get("id").asInt());
    assertFalse(on.has("note"));
    assertEquals("ada", read(v3, bytes, null).get("note").asText());
  }

  @Test
  void aNestedPropertyDroppedAndReAddedIsPrunedInsideItsObjectAndArray() throws Exception {
    String line = "{\"type\": \"object\", \"properties\": {"
        + "\"sku\": {\"type\": \"string\"}, \"note\": {\"type\": \"string\"}}}";
    String lineWithoutNote = "{\"type\": \"object\", \"properties\": {"
        + "\"sku\": {\"type\": \"string\"}}}";
    String lineWithNewNote = "{\"type\": \"object\", \"properties\": {"
        + "\"sku\": {\"type\": \"string\"}, "
        + "\"note\": {\"type\": \"string\", \"description\": \"new\"}}}";
    JsonSchema v1 = object("\"line\": " + line,
        "\"lines\": {\"type\": \"array\", \"items\": " + line + "}");
    JsonSchema v2 = object("\"line\": " + lineWithoutNote,
        "\"lines\": {\"type\": \"array\", \"items\": " + lineWithoutNote + "}");
    JsonSchema v3 = object("\"line\": " + lineWithNewNote,
        "\"lines\": {\"type\": \"array\", \"items\": " + lineWithNewNote + "}");
    byte[] bytes = write(v1, "{\"line\": {\"sku\": \"a\", \"note\": \"x\"}, "
        + "\"lines\": [{\"sku\": \"b\", \"note\": \"y\"}]}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode on = read(v3, bytes, "v1");
    assertEquals("a", on.get("line").get("sku").asText());
    assertFalse(on.get("line").has("note"));
    assertEquals("b", on.get("lines").get(0).get("sku").asText());
    assertFalse(on.get("lines").get(0).has("note"));
  }

  @Test
  void aPropertyPresentThroughoutIsLeftAlone() throws Exception {
    JsonSchema v1 = object(number("id"), string("name"));
    JsonSchema v2 = object(number("id"), string("name"), string("extra"));
    byte[] bytes = write(v1, "{\"id\": 7, \"name\": \"ada\"}");
    client.register(SUBJECT, v2);

    JsonNode on = read(v2, bytes, "v1");
    assertEquals(read(v2, bytes, null), on);
    assertTrue(on.has("name"));
  }

  // --- Helpers -----------------------------------------------------------------------------------

  private byte[] write(JsonSchema writer, String json) throws Exception {
    client.register(SUBJECT, writer);
    return serializer.serialize(TOPIC, JsonSchemaUtils.envelope(writer, MAPPER.readTree(json)));
  }

  private JsonNode read(JsonSchema reader, byte[] bytes, String provenance) {
    KafkaJsonSchemaDeserializer<JsonNode> deserializer =
        new KafkaJsonSchemaDeserializer<>(client, config(provenance));
    return (JsonNode) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, writer -> reader).getValue();
  }

  private static Map<String, Object> config(String provenance) {
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", "bogus");
    config.put("auto.register.schemas", false);
    config.put("use.latest.version", false);
    if (provenance != null) {
      config.put("use.provenance", provenance);
    }
    return config;
  }

  private static String number(String name) {
    return "\"" + name + "\": {\"type\": \"number\"}";
  }

  private static String string(String name) {
    return "\"" + name + "\": {\"type\": \"string\"}";
  }

  private static JsonSchema object(String... properties) {
    return new JsonSchema("{\"type\": \"object\", \"title\": \"Row\", \"properties\": {"
        + String.join(", ", properties) + "}}");
  }
}
