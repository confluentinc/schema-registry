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

package io.confluent.kafka.serializers.provenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * JSON Schema read with {@code provenance.algorithm}: a property dropped and re-added across an interior
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
  void theLatestVersionAsReaderPrunesAPropertyReAddedSinceTheWriter() throws Exception {
    // use.latest.version: the reader is the latest version, v3, whose note is new to v1's record.
    JsonSchema v1 = object(number("id"), string("note"));
    JsonSchema v2 = object(number("id"));
    JsonSchema v3 = object(number("id"),
        "\"note\": {\"type\": \"string\", \"description\": \"new\"}");
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);
    Map<String, Object> config = config("v1");
    config.put("use.latest.version", true);
    config.put("latest.compatibility.strict", false);

    JsonNode read = new KafkaJsonSchemaDeserializer<JsonNode>(client, config)
        .deserialize(TOPIC, bytes);
    assertEquals(7, read.get("id").asInt());
    assertFalse(read.has("note"));
  }

  @Test
  void aRecordNamingItsSchemaByGuidAloneIsReadByProvenance() throws Exception {
    JsonSchema v1 = object(number("id"), string("note"));
    JsonSchema v2 = object(number("id"));
    JsonSchema v3 = object(number("id"),
        "\"note\": {\"type\": \"string\", \"description\": \"new\"}");
    client.register(SUBJECT, v1);
    RecordHeaders headers = new RecordHeaders();
    Map<String, Object> byGuid = config(null);
    byGuid.put("value.schema.id.serializer", HeaderSchemaIdSerializer.class.getName());
    byte[] bytes = new KafkaJsonSchemaSerializer<>(client, byGuid).serialize(TOPIC, headers,
        JsonSchemaUtils.envelope(v1, MAPPER.readTree("{\"id\": 7, \"note\": \"ada\"}")));
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode on = (JsonNode) new KafkaJsonSchemaDeserializer<JsonNode>(client, config("v1"))
        .deserializeWithSchema(TOPIC, headers, bytes, writer -> v3).getValue();
    assertFalse(on.has("note"));
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
  void aValueOfABranchContinuedAtAnotherPositionByHintIsPruned() throws Exception {
    // Equal validation shapes, but the hints pair each branch with the other's position.
    String union = "\"p\": {\"anyOf\": [{\"type\": \"string\"}, {\"type\": \"integer\"}], "
        + "\"confluent:union\": [{\"name\": \"%s\"}, {\"name\": \"%s\"}]}";
    JsonSchema v1 = object(String.format(union, "A", "B"));
    JsonSchema v2 = object(String.format(union, "B", "A"));
    byte[] bytes = write(v1, "{\"p\": \"x\"}");
    client.register(SUBJECT, v2);

    assertFalse(read(v2, bytes, "v1").has("p"));
    assertEquals("x", read(v2, bytes, null).get("p").asText());
  }

  @Test
  void aValueFollowsItsTypeWhereTitlesSwap() throws Exception {
    // A title only documents: the string branch continues the string branch, whatever its title.
    JsonSchema v1 = object("\"p\": {\"anyOf\": [{\"title\": \"A\", \"type\": \"string\"}, "
        + "{\"title\": \"B\", \"type\": \"integer\"}]}");
    JsonSchema v2 = object("\"p\": {\"anyOf\": [{\"title\": \"B\", \"type\": \"string\"}, "
        + "{\"title\": \"A\", \"type\": \"integer\"}]}");
    byte[] bytes = write(v1, "{\"p\": \"x\"}");
    client.register(SUBJECT, v2);

    assertEquals("x", read(v2, bytes, "v1").get("p").asText());
  }

  @Test
  void aReaderDifferingOnlyInDescriptionsIsMatchedByStructure() throws Exception {
    // No version has its description; it has v3's structure, the latest with it: note is new.
    JsonSchema v1 = object(number("id"), string("note"));
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    client.register(SUBJECT, object(number("id")));
    client.register(SUBJECT, object(number("id"),
        "\"note\": {\"type\": \"string\", \"description\": \"new\"}"));
    JsonSchema reader = object(number("id"),
        "\"note\": {\"type\": \"string\", \"description\": \"again\"}");

    assertFalse(read(reader, bytes, "v1").has("note"));
  }

  @Test
  void aReaderInliningWhatAVersionReferencesIsMatchedByStructure() throws Exception {
    // D inline is the same location as D by $ref: the reader is still v3, where f is new.
    String d = "{\"type\": \"object\", \"properties\": {\"k\": {\"type\": \"string\"}}}";
    String byRef = "{\"type\": \"object\", \"properties\": "
        + "{\"d\": {\"$ref\": \"#/definitions/D\"}%s}, \"definitions\": {\"D\": " + d + "}}";
    String f = ", \"f\": {\"type\": \"string\"}";
    byte[] bytes = write(new JsonSchema(String.format(byRef, f)),
        "{\"d\": {\"k\": \"K\"}, \"f\": \"old\"}");
    String g = ", \"g\": {\"type\": \"string\"}";
    client.register(SUBJECT, new JsonSchema(String.format(byRef, "")));
    client.register(SUBJECT, new JsonSchema(String.format(byRef, f + g)));
    JsonSchema reader = new JsonSchema(
        "{\"type\": \"object\", \"properties\": {\"d\": " + d + f + g + "}}");

    JsonNode read = read(reader, bytes, "v1");
    assertFalse(read.has("f"));
    assertEquals("K", read.get("d").get("k").asText());
  }

  @Test
  void aReaderSkippingADefThatOnlyReferencesAnotherIsMatchedByStructure() throws Exception {
    // D is only a $ref to E: a reader naming E, or inlining it, is still v3, where f is new.
    String e = "{\"type\": \"object\", \"properties\": {\"k\": {\"type\": \"string\"}}}";
    String chain = "{\"type\": \"object\", \"properties\": "
        + "{\"d\": {\"$ref\": \"#/definitions/D\"}%s}, \"definitions\": "
        + "{\"D\": {\"$ref\": \"#/definitions/E\"}, \"E\": " + e + "}}";
    String f = ", \"f\": {\"type\": \"string\"}";
    String g = ", \"g\": {\"type\": \"string\"}";
    byte[] bytes = write(new JsonSchema(String.format(chain, f)),
        "{\"d\": {\"k\": \"K\"}, \"f\": \"old\"}");
    client.register(SUBJECT, new JsonSchema(String.format(chain, "")));
    client.register(SUBJECT, new JsonSchema(String.format(chain, f + g)));

    for (String reader : new String[] {
        "{\"type\": \"object\", \"properties\": {\"d\": {\"$ref\": \"#/definitions/E\"}" + f + g
            + "}, \"definitions\": {\"E\": " + e + "}}",
        "{\"type\": \"object\", \"properties\": {\"d\": " + e + f + g + "}}"}) {
      JsonNode read = read(new JsonSchema(reader), bytes, "v1");
      assertFalse(read.has("f"), reader);
      assertEquals("K", read.get("d").get("k").asText(), reader);
    }
  }

  @Test
  void aVersionWithADefinitionReferringOnlyToItselfIsReadWithoutProvenance() throws Exception {
    // D refers only to itself: v2, and a reader of it, have no provenance, and read as written.
    String a = "\"a\": {\"type\": \"string\"}";
    byte[] bytes = write(object(a), "{\"a\": \"A\"}");
    JsonSchema v2 = new JsonSchema("{\"type\": \"object\", \"properties\": {" + a + ", "
        + "\"d\": {\"$ref\": \"#/definitions/D\"}}, "
        + "\"definitions\": {\"D\": {\"$ref\": \"#/definitions/D\"}}}");
    client.register(SUBJECT, v2);

    JsonNode read = assertTimeoutPreemptively(Duration.ofSeconds(20), () -> read(v2, bytes, "v1"));
    assertEquals("A", read.get("a").asText());
  }

  @Test
  void aChainDoublingADefinitionAtEachLevelIsComparedQuickly() throws Exception {
    // D_i names D_i+1 twice under propertyNames, which the logical type ignores: shared shapes
    // compared node by node would take hours at this depth.
    StringBuilder defs = new StringBuilder();
    for (int i = 0; i < 40; i++) {
      defs.append(i > 0 ? ", " : "").append("\"D").append(i).append("\": {\"type\": \"object\", ")
          .append("\"propertyNames\": {\"anyOf\": [{\"$ref\": \"#/definitions/D").append(i + 1)
          .append("\"}, {\"$ref\": \"#/definitions/D").append(i + 1).append("\"}]}}");
    }
    defs.append(", \"D40\": {\"type\": \"string\"}");
    String schema = "{\"type\": \"object\", \"properties\": {\"u\": {\"oneOf\": ["
        + "{\"type\": \"object\", \"properties\": {\"k\": {\"const\": \"A\"}, "
        + "\"x\": {\"$ref\": \"#/definitions/D0\"}}}, "
        + "{\"type\": \"object\", \"properties\": {\"k\": {\"const\": \"B\"}, "
        + "\"y\": {\"type\": \"string\"}}}]}%s}, \"definitions\": {" + defs + "}}";
    String f = ", \"f\": {\"type\": \"string\"%s}";
    byte[] bytes = write(new JsonSchema(String.format(schema, String.format(f, ""))),
        "{\"u\": {\"k\": \"B\", \"y\": \"Y\"}, \"f\": \"old\"}");
    client.register(SUBJECT, new JsonSchema(String.format(schema, "")));
    JsonSchema reader = new JsonSchema(String.format(schema,
        String.format(f, ", \"description\": \"again\"")));
    client.register(SUBJECT, reader);

    JsonNode read =
        assertTimeoutPreemptively(Duration.ofSeconds(20), () -> read(reader, bytes, "v1"));
    assertFalse(read.has("f"));
    assertEquals("Y", read.get("u").get("y").asText());
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

  @Test
  void aReadRuleNeitherSeesNorLosesAPrunedProperty() throws Exception {
    // The document is pruned before the domain rules: a rule's value for the re-added note is
    // kept, and a required one's default comes first.
    JsonSchema v1 = object(number("id"), string("note"));
    JsonSchema v2 = object(number("id"));
    Rule fill = new Rule("fill", null, RuleKind.TRANSFORM, RuleMode.READ, "CEL_FIELD", null, null,
        "name == 'note' ; 'filled'", null, null, false);
    JsonSchema v3 = (JsonSchema) new JsonSchema("{\"type\": \"object\", \"properties\": {"
        + number("id") + ", \"note\": {\"type\": \"string\", \"default\": \"dflt\"}},"
        + " \"required\": [\"note\"]}")
        .copy(null, new RuleSet(null, Collections.singletonList(fill)));
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("filled", read(v3, bytes, "v1").get("note").asText());
  }

  @Test
  void aRequiredNullablePropertyReAddedWithoutDefaultReadsNull() throws Exception {
    // As Pydantic writes Optional[str] with no default: required, but null is a value it takes,
    // so the re-added f reads null rather than failing the record.
    JsonSchema v1 = object(number("id"), "\"f\": {\"type\": \"integer\"}");
    JsonSchema v2 = object(number("id"));
    JsonSchema v3 = new JsonSchema("{\"type\": \"object\", \"properties\": {" + number("id")
        + ", \"f\": {\"anyOf\": [{\"type\": \"string\"}, {\"type\": \"null\"}]}},"
        + " \"required\": [\"f\"]}");
    byte[] bytes = write(v1, "{\"id\": 7, \"f\": 11}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode read = read(v3, bytes, "v1");
    assertEquals(7, read.get("id").asInt());
    assertTrue(read.get("f").isNull());
  }

  @Test
  void aPrunedPropertyReadsAsOneNeverWrittenUnderValidation() throws Exception {
    // With validation on, everit fills a default for an absent property; pruning first gives a
    // pruned one the same, and removes an old value the reader would reject.
    JsonSchema v1 = object(number("id"), string("note"));
    JsonSchema v2 = object(number("id"));
    JsonSchema v3 = object(number("id"), "\"note\": {\"type\": \"integer\", \"default\": 9}");
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    Map<String, Object> config = config("v1");
    config.put("json.fail.invalid.schema", true);
    JsonNode on = (JsonNode) new KafkaJsonSchemaDeserializer<JsonNode>(client, config)
        .deserializeWithSchema(TOPIC, new RecordHeaders(), bytes, writer -> v3).getValue();
    assertEquals(9, on.get("note").asInt());
  }

  @Test
  void aReAddedPropertyWhoseOldValueNoBranchAcceptsIsPruned() throws Exception {
    // The old value of the property to prune fits no branch; it must still not reach the new one.
    String closed = "{\"type\": \"object\", \"properties\": {" + number("b") + "},"
        + " \"additionalProperties\": false}";
    JsonSchema v1 = object("\"u\": {\"oneOf\": [{\"type\": \"object\", \"properties\": {"
        + number("a") + ", " + string("t") + "}}, " + closed + "]}");
    JsonSchema v2 = object("\"u\": {\"oneOf\": [{\"type\": \"object\", \"properties\": {"
        + number("a") + "}}, " + closed + "]}");
    JsonSchema v3 = object("\"u\": {\"oneOf\": [{\"type\": \"object\", \"properties\": {"
        + number("a") + ", \"t\": {\"type\": \"integer\"}}}, " + closed + "]}");
    byte[] bytes = write(v1, "{\"u\": {\"a\": 1, \"t\": \"old\"}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertFalse(read(v3, bytes, "v1").get("u").has("t"));
    assertEquals("old", read(v3, bytes, null).get("u").get("t").asText());
  }

  @Test
  void aBranchNotDeclaringThePropertyLeavesItUnambiguous() throws Exception {
    // The record fits A and B; only A declares t, and there t continues.
    String a = "{\"type\": \"object\", \"properties\": {" + number("a") + ", " + string("t")
        + "}}";
    String b = "{\"type\": \"object\", \"properties\": {" + number("b") + "}}";
    String c = "{\"type\": \"object\", \"properties\": {" + number("c") + "},"
        + " \"required\": [\"c\"]}";
    String cWithT = "{\"type\": \"object\", \"properties\": {" + number("c") + ", "
        + string("t") + "}, \"required\": [\"c\"]}";
    JsonSchema v1 = object("\"u\": {\"anyOf\": [" + a + ", " + b + ", " + c + "]}");
    JsonSchema v2 = object("\"u\": {\"anyOf\": [" + a + ", " + b + ", " + cWithT + "]}");
    byte[] bytes = write(v1, "{\"u\": {\"a\": 1, \"t\": \"kept\"}}");
    client.register(SUBJECT, v2);

    assertEquals("kept", read(v2, bytes, "v1").get("u").get("t").asText());
  }

  @Test
  void anIntegralDecimalChoosesTheBranchValidationWould() throws Exception {
    // Under 2020-12 json-sKema counts 1.0 an integer and everit does not: the pruner must not
    // leave the value to the branch everit alone accepts, where t continues.
    String a = "{\"type\": \"object\", \"properties\": {" + number("k") + ", " + string("t")
        + "}, \"unevaluatedProperties\": false}";
    String b = "{\"type\": \"object\", \"properties\": {\"k\": {\"type\": \"integer\"}, "
        + string("n") + "%s}}";
    JsonSchema v1 = modern("\"u\": {\"anyOf\": [" + a + ", "
        + String.format(b, ", " + string("t")) + "]}");
    JsonSchema v2 = modern("\"u\": {\"anyOf\": [" + a + ", " + String.format(b, "") + "]}");
    JsonSchema v3 = modern("\"u\": {\"anyOf\": [" + a + ", " + String.format(b,
        ", \"t\": {\"type\": \"string\", \"description\": \"re-added\"}") + "]}");
    byte[] bytes = write(v1, "{\"u\": {\"k\": 1.0, \"n\": \"x\", \"t\": \"old\"}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertFalse(read(v3, bytes, "v1").get("u").has("t"));
    assertEquals("old", read(v3, bytes, null).get("u").get("t").asText());
  }

  @Test
  void aRequiredPropertyWithNoValueIsNamedByItsPath() throws Exception {
    JsonSchema v1 = object("\"o\": {\"type\": \"object\", \"properties\": {" + string("t") + "}}");
    JsonSchema v2 = object("\"o\": {\"type\": \"object\", \"properties\": {}}");
    JsonSchema v3 = object("\"o\": {\"type\": \"object\", \"properties\": {" + string("t")
        + "}, \"required\": [\"t\"]}");
    byte[] bytes = write(v1, "{\"o\": {\"t\": \"old\"}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    Exception e = assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
    assertEquals("Property [o, t] has no value to read: provenance withholds it, as new to the "
        + "reader or read as another branch than written, and the reader requires it and "
        + "declares no default.", e.getCause().getMessage());
  }

  @Test
  void aPrunedPropertyRequiredByAnyAllOfPartIsRequired() throws Exception {
    // Every allOf part applies: t, required by a part declaring nothing, takes the default
    // another part declares.
    String base = "{\"type\": \"object\", \"properties\": {" + number("id") + ", "
        + "\"t\": {\"type\": \"string\", \"default\": \"dflt\"%s}}}";
    String requiresT = "{\"required\": [\"t\"]}";
    JsonSchema v1 = object("\"p\": {\"allOf\": [" + String.format(base, "") + ", " + requiresT
        + "]}");
    JsonSchema v2 = object("\"p\": {\"allOf\": [{\"type\": \"object\", \"properties\": {"
        + number("id") + "}}]}");
    JsonSchema v3 = object("\"p\": {\"allOf\": ["
        + String.format(base, ", \"description\": \"re-added\"") + ", " + requiresT + "]}");
    byte[] bytes = write(v1, "{\"p\": {\"id\": 1, \"t\": \"old\"}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("dflt", read(v3, bytes, "v1").get("p").get("t").asText());
  }

  @Test
  void aPrunedPropertyRequiredByAnyAllOfPartFailsWithoutADefault() throws Exception {
    // The part requiring t comes second; it must not matter which part is reached first.
    String own = "{\"type\": \"object\", \"properties\": {" + string("t") + "}}";
    String part = "{\"type\": \"object\", \"properties\": {" + number("id")
        + ", \"t\": {\"type\": \"string\"%s}}, \"required\": [\"t\"]}";
    JsonSchema v1 = object("\"p\": {\"allOf\": [" + own + ", " + String.format(part, "") + "]}");
    JsonSchema v2 = object("\"p\": {\"allOf\": [{\"type\": \"object\", \"properties\": {"
        + number("id") + "}}]}");
    JsonSchema v3 = object("\"p\": {\"allOf\": [" + own + ", "
        + String.format(part, ", \"description\": \"re-added\"") + "]}");
    byte[] bytes = write(v1, "{\"p\": {\"id\": 1, \"t\": \"old\"}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    Exception e = assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
    assertTrue(e.getCause().getMessage().startsWith("Property [p, t] has no value to read"));
  }

  @Test
  void anAmbiguousPropertyIsRequiredOnlyIfEveryBranchRequiresIt() throws Exception {
    // The record fits A, where t continues and is required, and B, where t is new and optional:
    // t is pruned and the record reads as a B, whichever branch comes first.
    String a = "{\"type\": \"object\", \"properties\": {" + number("a") + ", " + string("t")
        + "}, \"required\": [\"t\"]}";
    String b = "{\"type\": \"object\", \"properties\": {" + number("b") + "%s}}";
    String[] bs = {String.format(b, ", " + string("t")), String.format(b, ""),
        String.format(b, ", \"t\": {\"type\": \"string\", \"description\": \"re-added\"}")};
    for (boolean aFirst : new boolean[] {true, false}) {
      init();
      JsonSchema[] versions = new JsonSchema[3];
      for (int i = 0; i < 3; i++) {
        versions[i] = object("\"u\": {\"anyOf\": [" + (aFirst ? a + ", " + bs[i] : bs[i] + ", " + a)
            + "]}");
      }
      byte[] bytes = write(versions[0], "{\"u\": {\"a\": 1, \"t\": \"x\"}}");
      client.register(SUBJECT, versions[1]);
      client.register(SUBJECT, versions[2]);

      assertFalse(read(versions[2], bytes, "v1").get("u").has("t"));
    }
  }

  @Test
  void aPropertyRequiredByAnAllOfPartBesideAUnionIsRequired() throws Exception {
    // P applies whichever branch of the union beside it the value takes, alone or ambiguously.
    String p = "{\"type\": \"object\", \"properties\": {\"t\": {\"type\": \"string\", "
        + "\"default\": \"dflt\"%s}}, \"required\": [\"t\"]}";
    String x = "{\"type\": \"object\", \"properties\": {" + number("x") + "%s}, "
        + "\"required\": [\"x\"]}";
    String y = "{\"type\": \"object\", \"properties\": {" + number("y") + "}, "
        + "\"required\": [\"y\"]}";
    String z = "{\"type\": \"object\", \"properties\": {" + number("z") + "%s}}";
    String t = ", " + string("t");
    for (String other : new String[] {y, z}) {
      init();
      JsonSchema v1 = object("\"p\": {\"allOf\": [" + String.format(p, "") + ", {\"anyOf\": ["
          + String.format(x, t) + ", " + String.format(other, t) + "]}]}");
      JsonSchema v2 = object("\"p\": {\"allOf\": [{\"type\": \"object\", \"properties\": {"
          + number("q") + "}}, {\"anyOf\": [" + String.format(x, "") + ", "
          + String.format(other, "") + "]}]}");
      JsonSchema v3 = object("\"p\": {\"allOf\": ["
          + String.format(p, ", \"description\": \"v3\"") + ", {\"anyOf\": ["
          + String.format(x, t) + ", " + String.format(other, t) + "]}]}");
      byte[] bytes = write(v1, "{\"p\": {\"x\": 1, \"t\": \"old\"}}");
      client.register(SUBJECT, v2);
      client.register(SUBJECT, v3);

      assertEquals("dflt", read(v3, bytes, "v1").get("p").get("t").asText());
    }
  }

  @Test
  void aDefaultStandingInForAPrunedValueIsNotPrunedItself() throws Exception {
    // o and o.t are both new; o's default holds a t of its own, which is the reader's, not old.
    String o = "\"o\": {\"type\": \"object\", \"properties\": {" + string("t") + "}, "
        + "\"default\": {\"t\": \"from-default\"}%s}";
    JsonSchema v1 = object(String.format(o, ""));
    JsonSchema v2 = object(number("q"));
    JsonSchema v3 = new JsonSchema("{\"type\": \"object\", \"title\": \"Row\", "
        + "\"properties\": {" + String.format(o, ", \"description\": \"v3\"") + "}, "
        + "\"required\": [\"o\"]}");
    byte[] bytes = write(v1, "{\"o\": {\"t\": \"old\"}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("from-default", read(v3, bytes, "v1").get("o").get("t").asText());
  }

  @Test
  void aNullDefaultStandsInUnderAModernDraft() throws Exception {
    JsonSchema v1 = modern(number("x"), string("t"));
    JsonSchema v2 = modern(number("x"));
    JsonSchema v3 = new JsonSchema("{\"$schema\": "
        + "\"https://json-schema.org/draft/2020-12/schema\", "
        + "\"type\": \"object\", \"title\": \"Row\", \"properties\": {" + number("x") + ", "
        + "\"t\": {\"type\": [\"string\", \"null\"], \"default\": null}}, "
        + "\"required\": [\"t\"]}");
    byte[] bytes = write(v1, "{\"x\": 1, \"t\": \"old\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode read = read(v3, bytes, "v1");
    assertTrue(read.has("t"));
    assertTrue(read.get("t").isNull());
  }

  @Test
  void aRequirementBesideManyAmbiguousUnionsIsKept() throws Exception {
    // P applies in every reading of the value, however many the ambiguous unions beside it make:
    // eleven make 2048, past the number weighed one by one.
    StringBuilder unions = new StringBuilder();
    for (int i = 0; i < 11; i++) {
      unions.append(", {\"anyOf\": [{\"type\": \"object\", \"properties\": {")
          .append(number("x" + i)).append("%1$s}}, {\"type\": \"object\", \"properties\": {")
          .append(number("y" + i)).append("%1$s}}]}");
    }
    String p = "{\"type\": \"object\", \"properties\": {\"t\": {\"type\": \"string\", "
        + "\"default\": \"dflt\"%s}}, \"required\": [\"t\"]}";
    String t = ", " + string("t");
    JsonSchema v1 = object("\"p\": {\"allOf\": [" + String.format(p, "")
        + String.format(unions.toString(), t) + "]}");
    JsonSchema v2 = object("\"p\": {\"allOf\": [{\"type\": \"object\", \"properties\": {"
        + number("q") + "}}" + String.format(unions.toString(), "") + "]}");
    JsonSchema v3 = object("\"p\": {\"allOf\": [" + String.format(p, ", \"description\": \"v3\"")
        + String.format(unions.toString(), t) + "]}");
    byte[] bytes = write(v1, "{\"p\": {\"t\": \"old\"}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("dflt", read(v3, bytes, "v1").get("p").get("t").asText());
  }

  @Test
  void aRequirementEveryReadingMakesIsKeptPastTheCap() throws Exception {
    // Whichever branch of the first union the value takes requires t; ten more ambiguous unions,
    // requiring nothing, make 2048 readings — past the number weighed one by one.
    String c = "{\"type\": \"object\", \"properties\": {" + number("%1$s") + "%2$s}, "
        + "\"required\": [\"t\"]}";
    String t = ", \"t\": {\"type\": \"string\", \"default\": \"dflt\"%s}";
    StringBuilder unions = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      unions.append(", {\"anyOf\": [{\"type\": \"object\", \"properties\": {")
          .append(number("x" + i)).append("%1$s}}, {\"type\": \"object\", \"properties\": {")
          .append(number("y" + i)).append("%1$s}}]}");
    }
    String[] ts = {String.format(t, ""), "", String.format(t, ", \"description\": \"v3\"")};
    JsonSchema[] versions = new JsonSchema[3];
    for (int v = 0; v < 3; v++) {
      String first = v == 1
          ? "{\"anyOf\": [{\"type\": \"object\", \"properties\": {" + number("c") + "}}, "
              + "{\"type\": \"object\", \"properties\": {" + number("d") + "}}]}"
          : "{\"anyOf\": [" + String.format(c, "c", ts[v]) + ", " + String.format(c, "d", ts[v])
              + "]}";
      versions[v] = object("\"p\": {\"allOf\": [" + first
          + String.format(unions.toString(), v == 1 ? "" : ", " + string("t")) + "]}");
    }
    byte[] bytes = write(versions[0], "{\"p\": {\"t\": \"old\"}}");
    client.register(SUBJECT, versions[1]);
    client.register(SUBJECT, versions[2]);

    assertEquals("dflt", read(versions[2], bytes, "v1").get("p").get("t").asText());
  }

  @Test
  void aPrunedPropertyTakesTheDefaultOfTheDefinitionItRefersTo() throws Exception {
    JsonSchema v1 = object(number("x"), string("t"));
    JsonSchema v2 = object(number("x"));
    JsonSchema v3 = new JsonSchema("{\"type\": \"object\", \"title\": \"Row\", \"properties\": {"
        + number("x") + ", \"t\": {\"$ref\": \"#/definitions/T\"}}, \"required\": [\"t\"], "
        + "\"definitions\": {\"T\": {\"type\": \"string\", \"default\": \"d\"}}}");
    byte[] bytes = write(v1, "{\"x\": 1, \"t\": \"old\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("d", read(v3, bytes, "v1").get("t").asText());
  }

  @Test
  void aPropertyIsNotRequiredWhereABranchWithoutItFits() throws Exception {
    // A requires t, B is open and has none: pruned, the value is a valid B, so reads. With B
    // closed no reading lacks t, and the record fails as rule 3 has it.
    for (boolean closed : new boolean[] {false, true}) {
      init();
      String b = "{\"type\": \"object\", \"properties\": {" + number("b") + "}"
          + (closed ? ", \"additionalProperties\": false" : "") + "}";
      String a = "{\"type\": \"object\", \"properties\": {" + number("a") + "%s}%s}";
      JsonSchema v1 = object("\"u\": {\"anyOf\": [" + String.format(a, ", " + string("t"), "")
          + ", " + b + "]}");
      JsonSchema v2 = object("\"u\": {\"anyOf\": [" + String.format(a, "", "") + ", " + b + "]}");
      JsonSchema v3 = object("\"u\": {\"anyOf\": [" + String.format(a,
          ", \"t\": {\"type\": \"string\", \"description\": \"v3\"}",
          ", \"required\": [\"t\"]") + ", " + b + "]}");
      byte[] bytes = write(v1, "{\"u\": {\"a\": 1, \"t\": \"old\"}}");
      client.register(SUBJECT, v2);
      client.register(SUBJECT, v3);

      if (closed) {
        assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
      } else {
        assertFalse(read(v3, bytes, "v1").get("u").has("t"));
      }
    }
  }

  @Test
  void aClosedBranchWithoutThePropertyIsAReadingOfThePrunedValue() throws Exception {
    // B is closed and declares only a: it rejects the value while t is in it, but t pruned, the
    // value is a B, as a record never written with t reads.
    String b = "{\"type\": \"object\", \"properties\": {" + number("a") + "}, "
        + "\"additionalProperties\": false}";
    String a = "{\"type\": \"object\", \"properties\": {" + number("a") + "%s}%s}";
    JsonSchema v1 = object("\"u\": {\"anyOf\": [" + String.format(a, ", " + string("t"), "")
        + ", " + b + "]}");
    JsonSchema v2 = object("\"u\": {\"anyOf\": [" + String.format(a, "", "") + ", " + b + "]}");
    JsonSchema v3 = object("\"u\": {\"anyOf\": [" + String.format(a,
        ", \"t\": {\"type\": \"string\", \"description\": \"v3\"}",
        ", \"required\": [\"t\"]") + ", " + b + "]}");
    byte[] bytes = write(v1, "{\"u\": {\"a\": 1, \"t\": \"old\"}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertFalse(read(v3, bytes, "v1").get("u").has("t"));
  }

  @Test
  void aModernDraftDefaultBesideARefStandsIn() throws Exception {
    // 2019-09 and later honour keywords beside $ref, a default among them.
    JsonSchema v1 = modern(number("x"), string("t"));
    JsonSchema v2 = modern(number("x"));
    JsonSchema v3 = new JsonSchema("{\"$schema\": "
        + "\"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", "
        + "\"title\": \"Row\", \"properties\": {" + number("x") + ", \"t\": {\"$ref\": "
        + "\"#/$defs/T\", \"default\": \"sib\"}}, \"required\": [\"t\"], "
        + "\"$defs\": {\"T\": {\"type\": \"string\"}}}");
    byte[] bytes = write(v1, "{\"x\": 1, \"t\": \"old\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals("sib", read(v3, bytes, "v1").get("t").asText());
  }

  @Test
  void aBranchInsertedInFrontLeavesTheOthersTheirValues() throws Exception {
    // V1 names unhinted branches by position; C inserted in front must not hand A the ids of B,
    // which sat at A's new position and had no x.
    String a = "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"a\"]}, "
        + number("x") + "}}";
    String b = "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"b\"]}, "
        + number("y") + "}}";
    String c = "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"c\"]}, "
        + number("z") + "}}";
    JsonSchema v1 = object("\"u\": {\"oneOf\": [" + a + ", " + b + "]}");
    JsonSchema v2 = object("\"u\": {\"oneOf\": [" + c + ", " + a + ", " + b + "]}");
    byte[] bytes = write(v1, "{\"u\": {\"kind\": \"a\", \"x\": 5}}");
    client.register(SUBJECT, v2);

    assertEquals(5, read(v2, bytes, "v1").get("u").get("x").asInt());
  }

  @Test
  void aConstBranchReorderedOrInsertedKeepsItsValue() throws Exception {
    // Each const continues the branch holding its value, so neither change prunes it.
    String a = "{\"const\": \"a\"}";
    String b = "{\"const\": \"b\"}";
    JsonSchema v1 = object("\"u\": {\"oneOf\": [" + a + ", " + b + "]}");
    byte[] bytes = write(v1, "{\"u\": \"b\"}");
    JsonSchema inserted = object("\"u\": {\"oneOf\": [{\"const\": \"z\"}, " + a + ", " + b + "]}");
    client.register(SUBJECT, inserted);
    assertEquals("b", read(inserted, bytes, "v1").path("u").asText());

    JsonSchema reordered = object("\"u\": {\"oneOf\": [" + b + ", " + a + "]}");
    client.register(SUBJECT, reordered);
    assertEquals("b", read(reordered, bytes, "v1").path("u").asText());
  }

  @Test
  void aBranchWhoseTitleMovedKeepsItsValues() throws Exception {
    // The card branch is unchanged but for its title, which a new branch now has: it keeps its
    // identity, so its values, as validation would read them.
    String card = "\"number\": {\"type\": \"string\"}, \"cvv\": {\"type\": \"string\"}";
    JsonSchema v1 = object("\"u\": {\"oneOf\": [{\"type\": \"object\", \"title\": \"Payment\", "
        + "\"properties\": {" + card + "}}, {\"type\": \"string\"}]}");
    JsonSchema v2 = object("\"u\": {\"oneOf\": [{\"type\": \"object\", \"title\": \"Card\", "
        + "\"properties\": {" + card + "}}, {\"type\": \"object\", \"title\": \"Payment\", "
        + "\"properties\": {\"iban\": {\"type\": \"string\"}}}, {\"type\": \"string\"}]}");
    byte[] bytes = write(v1, "{\"u\": {\"number\": \"4111\", \"cvv\": \"123\"}}");
    client.register(SUBJECT, v2);

    assertEquals("4111", read(v2, bytes, "v1").path("u").path("number").asText());
  }

  @Test
  void aBranchThatMovesAndGainsAMemberKeepsItsValues() throws Exception {
    String a = "{\"type\": \"object\", \"properties\": {" + number("x") + "}}";
    String b = "{\"type\": \"object\", \"properties\": {" + number("y") + "}}";
    String bw = "{\"type\": \"object\", \"properties\": {" + number("y") + ", "
        + number("w") + "}}";
    JsonSchema v1 = object("\"u\": {\"oneOf\": [" + a + ", " + b + "]}");
    JsonSchema v2 = object("\"u\": {\"oneOf\": [" + bw + ", " + a + "]}");
    byte[] bytes = write(v1, "{\"u\": {\"y\": 5}}");
    client.register(SUBJECT, v2);

    assertEquals(5, read(v2, bytes, "v1").get("u").get("y").asInt());
  }

  @Test
  void aRecordOfABranchTheReaderLacksIsNotReadAsAnother() throws Exception {
    // Pruning the discriminator a new branch shares must not leave the value an instance of a
    // continuing branch: p was written as b's, never a's.
    JsonSchema v1 = object("\"u\": {\"oneOf\": [" + kinded("a", "p") + ", " + kinded("b", "p")
        + "]}");
    JsonSchema v2 = object("\"u\": {\"oneOf\": [" + kinded("a", "p") + ", " + kinded("c", "q")
        + "]}");
    byte[] ofB = write(v1, "{\"u\": {\"kind\": \"b\", \"p\": 7}}");
    byte[] ofA = write(v1, "{\"u\": {\"kind\": \"a\", \"p\": 8}}");
    client.register(SUBJECT, v2);

    assertFalse(read(v2, ofB, "v1").get("u").has("p"));
    assertEquals(8, read(v2, ofA, "v1").get("u").get("p").asInt());
  }

  @Test
  void aPrimitiveInAReAddedUnionBranchIsPruned() throws Exception {
    // The value lives in its branch: number, dropped and re-added, is new.
    JsonSchema v1 = object("\"e\": {\"type\": [\"string\", \"number\"]}");
    JsonSchema v2 = object("\"e\": {\"type\": [\"string\", \"boolean\"]}");
    JsonSchema v3 = object("\"e\": {\"type\": [\"string\", \"boolean\", \"number\"]}");
    byte[] number = write(v1, "{\"e\": 2.5}");
    byte[] string = write(v1, "{\"e\": \"s\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertFalse(read(v3, number, "v1").has("e"));
    assertEquals("s", read(v3, string, "v1").get("e").asText());
  }

  @Test
  void aValueEqualToAPlacedDefaultIsStillPruned() throws Exception {
    // Jackson shares one node for true: b, holding it too, must not pass for a's default.
    String flag = "{\"type\": \"boolean\"}";
    JsonSchema v1 = object("\"a\": " + flag, "\"b\": " + flag);
    JsonSchema v2 = object();
    JsonSchema v3 = new JsonSchema("{\"type\": \"object\", \"title\": \"Row\", \"properties\": "
        + "{\"a\": {\"type\": \"boolean\", \"default\": true}, \"b\": " + flag + "}, "
        + "\"required\": [\"a\"]}");
    byte[] bytes = write(v1, "{\"a\": true, \"b\": true}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode read = read(v3, bytes, "v1");
    assertTrue(read.get("a").asBoolean());
    assertFalse(read.has("b"));
  }

  @Test
  void anItemOrValueOfABranchContinuedAtAnotherPositionByHintIsPruned() throws Exception {
    // As for a property's own union: the hints pair each item or value branch with the other's.
    String swap = "[{\"type\": \"string\"}, {\"type\": \"integer\"}], "
        + "\"confluent:union\": [{\"name\": \"%s\"}, {\"name\": \"%s\"}]";
    for (String shape : new String[] {
        "{\"type\": \"array\", \"items\": {\"anyOf\": %s}}",
        "{\"type\": \"object\", \"connect.type\": \"map\", "
            + "\"additionalProperties\": {\"anyOf\": %s}}"}) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
      boolean array = shape.contains("items");
      JsonSchema v1 = object("\"e\": " + String.format(shape, String.format(swap, "A", "B")));
      JsonSchema v2 = object("\"e\": " + String.format(shape, String.format(swap, "B", "A")));
      byte[] bytes = write(v1, array ? "{\"e\": [\"x\"]}" : "{\"e\": {\"k\": \"x\"}}");
      client.register(SUBJECT, v2);

      assertFalse(read(v2, bytes, "v1").has("e"), shape);
      assertTrue(read(v2, bytes, null).has("e"), shape);
    }
  }

  @Test
  void aPrimitiveInAReAddedItemOrValueBranchIsPruned() throws Exception {
    // The item's union is a location of its own, as a property's is: number, re-added, is new.
    for (String shape : new String[] {
        "{\"type\": \"array\", \"items\": {\"type\": %s}}",
        "{\"type\": \"object\", \"connect.type\": \"map\", "
            + "\"additionalProperties\": {\"type\": %s}}"}) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
      boolean array = shape.contains("items");
      JsonSchema v1 = object("\"e\": " + String.format(shape, "[\"string\", \"number\"]"));
      JsonSchema v2 = object("\"e\": " + String.format(shape, "[\"string\", \"boolean\"]"));
      JsonSchema v3 = object("\"e\": "
          + String.format(shape, "[\"string\", \"boolean\", \"number\"]"));
      byte[] number = write(v1, array ? "{\"e\": [2.5, \"s\"]}" : "{\"e\": {\"k\": 2.5}}");
      byte[] string = write(v1, array ? "{\"e\": [\"s\"]}" : "{\"e\": {\"k\": \"s\"}}");
      client.register(SUBJECT, v2);
      client.register(SUBJECT, v3);

      assertFalse(read(v3, number, "v1").has("e"), shape);
      assertTrue(read(v3, string, "v1").has("e"), shape);
    }
  }

  @Test
  void aValueTheOlderReaderReadsAsAnotherBranchIsPruned() throws Exception {
    // Written as branch 0, but e = 4.5 fits only branch 1 under v1, whose l is not the l it was
    // written as: a name spelled in several branches is always checked.
    String older = "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"k1\"]}, "
        + "\"e\": {\"type\": \"%s\"}, " + number("l") + "}}";
    String other = "{\"type\": \"object\", \"properties\": {" + number("l") + "%s}}";
    JsonSchema v1 = object("\"g\": {\"oneOf\": [" + String.format(older, "integer") + ", "
        + String.format(other, "") + "]}");
    JsonSchema v2 = object("\"g\": {\"oneOf\": [" + String.format(older, "number") + ", "
        + String.format(other, ", \"kind\": {\"enum\": [\"k2\"]}") + "]}");
    client.register(SUBJECT, v1);
    byte[] bytes = write(v2, "{\"g\": {\"kind\": \"k1\", \"e\": 4.5, \"l\": 5.5}}");

    assertFalse(read(v1, bytes, "v1").get("g").has("l"));
  }

  @Test
  void aPrimitiveInAReAddedBranchOfAUnionUnderAllOfIsPruned() throws Exception {
    // allOf applies its parts as the walk does: the union in one is the property's own.
    JsonSchema v1 = object("\"e\": {\"allOf\": [{\"type\": [\"string\", \"number\"]}]}");
    JsonSchema v2 = object("\"e\": {\"allOf\": [{\"type\": [\"string\", \"boolean\"]}]}");
    JsonSchema v3 = object(
        "\"e\": {\"allOf\": [{\"type\": [\"string\", \"boolean\", \"number\"]}]}");
    byte[] number = write(v1, "{\"e\": 2.5}");
    byte[] string = write(v1, "{\"e\": \"s\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertFalse(read(v3, number, "v1").has("e"));
    assertEquals("s", read(v3, string, "v1").get("e").asText());
  }

  @Test
  void aPrimitiveFittingNoBranchOfTheReaderIsPruned() throws Exception {
    // 1.0 is no integer to everit, and 2.5 fits no branch of v3: neither continues anything.
    JsonSchema v1 = object("\"e\": {\"type\": [\"string\", \"integer\", \"number\"]}");
    JsonSchema v2 = object("\"e\": {\"type\": [\"string\", \"boolean\"]}");
    JsonSchema v3 = object("\"e\": {\"type\": [\"string\", \"boolean\", \"integer\"]}");
    byte[] decimal = write(v1, "{\"e\": 1.0}");
    byte[] fraction = write(v1, "{\"e\": 2.5}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertFalse(read(v3, decimal, "v1").has("e"));
    assertFalse(read(v3, fraction, "v1").has("e"));
  }

  @Test
  void anIntegralDecimalInAContinuingIntegerBranchStaysUnderAModernDraft() throws Exception {
    // json-sKema counts 1.0 an integer, and so does the pruner there: boolean is re-added
    // beside it, but 1.0 stays in the integer branch it was written in.
    JsonSchema v1 = modern("\"e\": {\"type\": [\"string\", \"integer\", \"boolean\"]}");
    JsonSchema v2 = modern("\"e\": {\"type\": [\"string\", \"integer\"]}");
    JsonSchema v3 = modern("\"e\": {\"type\": [\"string\", \"integer\", \"boolean\"]}",
        "\"x\": {\"type\": \"number\"}");
    byte[] bytes = write(v1, "{\"e\": 1.0}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals(1, read(v3, bytes, "v1").get("e").asInt());
  }

  @Test
  void aMapInAReAddedBranchIsPruned() throws Exception {
    // A map has no property of its own to prune: its branch holds it, inline, behind a $ref,
    // or as an array item.
    String map = "{\"type\": \"object\", \"connect.type\": \"map\", "
        + "\"additionalProperties\": {\"type\": \"string\"}}";
    String defs = ", \"$defs\": {\"Map\": " + map + "}}";
    String[][] shapes = {
        {"{\"oneOf\": [{\"type\": \"integer\"}, %s]}", map, "}", "{\"k\": \"s\"}"},
        {"{\"oneOf\": [{\"type\": \"integer\"}, %s]}", "{\"$ref\": \"#/$defs/Map\"}", defs,
            "{\"k\": \"s\"}"},
        {"{\"type\": \"array\", \"items\": {\"oneOf\": [{\"type\": \"integer\"}, %s]}}", map, "}",
            "[{\"k\": \"s\"}]"}};
    for (String[] shape : shapes) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
      String head = "{\"type\": \"object\", \"properties\": {\"e\": ";
      JsonSchema v1 = new JsonSchema(head + String.format(shape[0], shape[1]) + "}" + shape[2]);
      JsonSchema v2 = new JsonSchema(
          head + String.format(shape[0], "{\"type\": \"boolean\"}") + "}" + shape[2]);
      // g only keeps v3 from being deduplicated into v1.
      JsonSchema v3 = new JsonSchema(head + String.format(shape[0], shape[1])
          + ", \"g\": {\"type\": \"string\"}}" + shape[2]);
      byte[] bytes = write(v1, "{\"e\": " + shape[3] + "}");
      client.register(SUBJECT, v2);
      client.register(SUBJECT, v3);

      assertTrue(read(v3, bytes, null).has("e"), shape[1]);
      assertFalse(read(v3, bytes, "v1").has("e"), shape[1]);
    }
  }

  @Test
  void aNullMemberBehindARefIsANullMember() throws Exception {
    // Moving the null member behind a $ref changes no union: o stays nullable, u keeps its two.
    String ab = "{\"type\": \"object\", \"properties\": {\"a\": {\"type\": \"string\"}}}";
    String body = "{\"type\": \"object\", \"properties\": {\"o\": {\"oneOf\": [%1$s, " + ab
        + "]}, \"u\": {\"oneOf\": [%1$s, {\"type\": \"string\"}, {\"type\": \"integer\"}]}}%2$s}";
    JsonSchema v1 = new JsonSchema(String.format(body, "{\"type\": \"null\"}", ""));
    JsonSchema v2 = new JsonSchema(String.format(body, "{\"$ref\": \"#/$defs/N\"}",
        ", \"$defs\": {\"N\": {\"type\": \"null\"}}"));
    byte[] bytes = write(v1, "{\"o\": {\"a\": \"s\"}, \"u\": \"t\"}");
    client.register(SUBJECT, v2);

    JsonNode read = read(v2, bytes, "v1");
    assertEquals("s", read.get("o").get("a").asText());
    assertEquals("t", read.get("u").asText());
  }

  @Test
  void aMapInAReAddedBranchBehindAnAllOfIsPruned() throws Exception {
    // The converter reads this allOf as the map: its kind holds it, however it is spelled.
    String map = "{\"type\": \"object\", \"connect.type\": \"map\", "
        + "\"additionalProperties\": {\"type\": \"string\"}}";
    String body = "{\"type\": \"object\", \"properties\": {%s\"e\": {\"oneOf\": "
        + "[{\"type\": \"integer\"}, %s]}}, \"$defs\": {\"M\": " + map + "}}";
    String allOf = "{\"allOf\": [{\"$ref\": \"#/$defs/M\"}, {\"minProperties\": 0}]}";
    JsonSchema v1 = new JsonSchema(String.format(body, "", allOf));
    JsonSchema v2 = new JsonSchema(String.format(body, "", "{\"type\": \"boolean\"}"));
    JsonSchema v3 = new JsonSchema(String.format(body, "\"g\": {\"type\": \"string\"}, ", allOf));
    byte[] bytes = write(v1, "{\"e\": {\"k\": \"s\"}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertTrue(read(v3, bytes, null).has("e"));
    assertFalse(read(v3, bytes, "v1").has("e"));
  }

  @Test
  void anObjectInAReAddedAnyValueBranchIsPruned() throws Exception {
    // A {} or true branch is a scalar to the logical type, with no properties for the walk to
    // prune: like a map's, its branch holds the object.
    for (String any : new String[] {"{}", "true"}) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
      String body = "{\"type\": \"object\", \"properties\": {%s\"e\": {\"oneOf\": "
          + "[{\"type\": \"integer\"}, %s]}}}";
      JsonSchema v1 = new JsonSchema(String.format(body, "", any));
      JsonSchema v2 = new JsonSchema(String.format(body, "", "{\"type\": \"boolean\"}"));
      JsonSchema v3 = new JsonSchema(String.format(body, "\"g\": {\"type\": \"string\"}, ", any));
      byte[] bytes = write(v1, "{\"e\": {\"k\": \"s\"}}");
      client.register(SUBJECT, v2);
      client.register(SUBJECT, v3);

      assertTrue(read(v3, bytes, null).has("e"), any);
      assertFalse(read(v3, bytes, "v1").has("e"), any);
    }
  }

  @Test
  void aMapFittingNoBranchOfTheReaderIsPruned() throws Exception {
    // Under draft-07, 1.0 is no integer: the map fits no branch of v3, and the branch it was
    // written in does not continue into one.
    String body = "{\"type\": \"object\", \"properties\": {\"e\": {\"oneOf\": ["
        + "{\"type\": \"string\"}, {\"type\": \"object\", \"connect.type\": \"map\", "
        + "\"additionalProperties\": {\"type\": \"%s\"}}]}}}";
    JsonSchema v1 = new JsonSchema(String.format(body, "number"));
    JsonSchema v2 = new JsonSchema(String.format(body, "boolean"));
    JsonSchema v3 = new JsonSchema(String.format(body, "integer"));
    byte[] map = write(v1, "{\"e\": {\"k\": 1.0}}");
    byte[] string = write(v1, "{\"e\": \"s\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertFalse(read(v3, map, "v1").has("e"));
    assertEquals("s", read(v3, string, "v1").get("e").asText());
  }

  @Test
  void aNullMemberBehindARefKeepsProvenanceUnderAModernDraft() throws Exception {
    // A 2020-12 null definition used to cost the subject its provenance, so note kept its value.
    String ab = "{\"type\": \"object\", \"properties\": {\"a\": {\"type\": \"string\"}}}";
    String body = "{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", "
        + "\"type\": \"object\", \"properties\": {%2$s\"o\": {\"oneOf\": [%1$s, " + ab + "]}}, "
        + "\"$defs\": {\"N\": {\"type\": \"null\"}}}";
    String ref = "{\"$ref\": \"#/$defs/N\"}";
    String note = "\"note\": {\"type\": \"string\"}, ";
    JsonSchema v1 = new JsonSchema(String.format(body, "{\"type\": \"null\"}", note));
    JsonSchema v2 = new JsonSchema(String.format(body, ref, ""));
    JsonSchema v3 = new JsonSchema(String.format(body, ref, note));
    byte[] bytes = write(v1, "{\"o\": {\"a\": \"s\"}, \"note\": \"old\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode read = read(v3, bytes, "v1");
    assertEquals("s", read.get("o").get("a").asText());
    assertFalse(read.has("note"));
  }

  @Test
  void aPropertyRequiredByAnotherIsDefaultedOrFailsWhenPruned() throws Exception {
    // a's presence requires p, by draft-07 dependencies or 2020-12 dependentRequired: a pruned p
    // takes its default, or fails the record, as one listed in required does.
    String body = "{\"type\": \"object\", \"properties\": {\"a\": {\"type\": \"integer\"}%s}%s}";
    String p = ", \"p\": {\"type\": \"string\"%s}";
    for (String requires : new String[] {", \"dependencies\": {\"a\": [\"p\"]}",
        ", \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", "
            + "\"dependentRequired\": {\"a\": [\"p\"]}"}) {
      for (String defaulted : new String[] {"", ", \"default\": \"pd\""}) {
        client = new ProvenanceMockSchemaRegistryClient();
        serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
        JsonSchema v1 = new JsonSchema(String.format(body, String.format(p, ""), ""));
        JsonSchema v2 = new JsonSchema(String.format(body, "", ""));
        JsonSchema v3 = new JsonSchema(String.format(body, String.format(p, defaulted), requires));
        byte[] bytes = write(v1, "{\"a\": 1, \"p\": \"old\"}");
        client.register(SUBJECT, v2);
        client.register(SUBJECT, v3);

        if (defaulted.isEmpty()) {
          Exception e = assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
          assertTrue(e.getCause().getMessage().startsWith("Property [p] has no value to read"),
              requires);
        } else {
          assertEquals("pd", read(v3, bytes, "v1").get("p").asText(), requires);
        }
      }
    }
  }

  @Test
  void aPropertyRequiredOnlyByAPrunedSiblingIsNotRequired() throws Exception {
    // The trigger and the property it requires are both new: once both are pruned nothing is
    // required, whichever of the two the pruner reaches first.
    for (String[] pair : new String[][] {{"z", "b"}, {"b", "z"}}) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
      String trigger = pair[0];
      String dependent = pair[1];
      String props = string(dependent) + ", \"" + trigger + "\": {\"type\": \"integer\"}";
      JsonSchema v1 = object(props);
      JsonSchema v2 = object(string("q"));
      JsonSchema v3 = new JsonSchema("{\"type\": \"object\", \"title\": \"Row\", \"properties\": {"
          + props + ", " + string("g") + "}, \"dependencies\": {\"" + trigger + "\": [\""
          + dependent + "\"]}}");
      byte[] bytes = write(v1, "{\"" + dependent + "\": \"x\", \"" + trigger + "\": 1}");
      client.register(SUBJECT, v2);
      client.register(SUBJECT, v3);

      assertEquals(0, read(v3, bytes, "v1").size(), trigger);
    }
  }

  @Test
  void aPropertyRequiredByAnotherAllOfPartIsDefaultedOrFails() throws Exception {
    // The dependencies clause sits in an allOf part apart from the one declaring p.
    String v3 = "{\"allOf\": [{\"type\": \"object\", \"properties\": {\"a\": {\"type\": "
        + "\"integer\"}, \"p\": {\"type\": \"string\"%s}}}, {\"dependencies\": {\"a\": [\"p\"]}}]}";
    for (String defaulted : new String[] {"", ", \"default\": \"pd\""}) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
      JsonSchema v1 = object("\"a\": {\"type\": \"integer\"}", string("p"));
      JsonSchema reader = new JsonSchema(String.format(v3, defaulted));
      byte[] bytes = write(v1, "{\"a\": 1, \"p\": \"old\"}");
      client.register(SUBJECT, object("\"a\": {\"type\": \"integer\"}"));
      client.register(SUBJECT, reader);

      if (defaulted.isEmpty()) {
        Exception e = assertThrows(SerializationException.class, () -> read(reader, bytes, "v1"));
        assertTrue(e.getCause().getMessage().startsWith("Property [p] has no value to read"));
      } else {
        assertEquals("pd", read(reader, bytes, "v1").get("p").asText());
      }
    }
  }

  @Test
  void aDefaultTheReaderRejectsIsNoValue() throws Exception {
    // n is new and required, but its default is no integer: no value to read, not an invalid one.
    JsonSchema v1 = object("\"a\": {\"type\": \"integer\"}", "\"n\": {\"type\": \"integer\"}");
    JsonSchema v2 = object("\"a\": {\"type\": \"integer\"}");
    JsonSchema v3 = new JsonSchema("{\"type\": \"object\", \"title\": \"Row\", \"properties\": {"
        + "\"a\": {\"type\": \"integer\"}, \"n\": {\"type\": \"integer\", \"default\": \"oops\"}}, "
        + "\"required\": [\"n\"]}");
    byte[] bytes = write(v1, "{\"a\": 1, \"n\": 3}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    Exception e = assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
    assertTrue(e.getCause().getMessage().contains("does not validate"));
  }

  @Test
  void anUnusedUnconvertibleDefinitionKeepsProvenance() throws Exception {
    // An unused 2020-12 definition the logical type cannot express used to cost the subject its
    // provenance, so the re-added note kept its value.
    String body = "{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", "
        + "\"type\": \"object\", \"properties\": {\"a\": {\"type\": \"string\"}%s}, "
        + "\"$defs\": {\"U\": {\"not\": {\"type\": \"string\"}}}}";
    JsonSchema v1 = new JsonSchema(String.format(body, ", " + string("note")));
    JsonSchema v2 = new JsonSchema(String.format(body, ""));
    JsonSchema v3 = new JsonSchema(String.format(body, ", " + string("note") + ", "
        + string("g")));
    byte[] bytes = write(v1, "{\"a\": \"x\", \"note\": \"old\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode read = read(v3, bytes, "v1");
    assertEquals("x", read.get("a").asText());
    assertFalse(read.has("note"));
  }

  @Test
  void aValueInARestartedObjectBranchIsPrunedWhole() throws Exception {
    // The object branch is removed and re-added: x is new, and once it is pruned the empty object
    // is no value of the reader's either. A key the reader never declares keeps it.
    String body = "{\"type\": \"object\", \"properties\": {%s\"p\": {\"oneOf\": [%s]}}%s}";
    String object = "{\"type\": \"object\", \"properties\": {\"x\": {\"type\": \"string\"}}}";
    JsonSchema v1 = new JsonSchema(
        String.format(body, "", object + ", {\"type\": \"integer\"}", ""));
    JsonSchema v2 = new JsonSchema(String.format(body, "",
        "{\"type\": \"integer\"}, {\"type\": \"boolean\"}", ""));
    JsonSchema v3 = new JsonSchema(String.format(body, "\"g\": {\"type\": \"string\"}, ",
        object + ", {\"type\": \"integer\"}", ""));
    byte[] bytes = write(v1, "{\"p\": {\"x\": \"old\"}}");
    byte[] extra = write(v1, "{\"p\": {\"x\": \"old\", \"z\": 1}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertFalse(read(v3, bytes, "v1").has("p"));
    assertEquals(1, read(v3, extra, "v1").get("p").get("z").asInt());
    assertFalse(read(v3, extra, "v1").get("p").has("x"));
  }

  @Test
  void aRequiredValueInARestartedObjectBranchTakesItsDefaultOrFails() throws Exception {
    String object = "{\"type\": \"object\", \"properties\": {\"x\": {\"type\": \"string\"}}}";
    String body = "{\"type\": \"object\", \"properties\": {%s\"p\": {\"oneOf\": [%s]%s}}, "
        + "\"required\": [\"p\"]}";
    for (String defaulted : new String[] {"", ", \"default\": 7"}) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
      JsonSchema v1 = new JsonSchema(
          String.format(body, "", object + ", {\"type\": \"integer\"}", ""));
      JsonSchema v2 = new JsonSchema(String.format(body, "",
          "{\"type\": \"integer\"}, {\"type\": \"boolean\"}", ""));
      JsonSchema v3 = new JsonSchema(String.format(body, "\"g\": {\"type\": \"string\"}, ",
          object + ", {\"type\": \"integer\"}", defaulted));
      byte[] bytes = write(v1, "{\"p\": {\"x\": \"old\"}}");
      client.register(SUBJECT, v2);
      client.register(SUBJECT, v3);

      if (defaulted.isEmpty()) {
        Exception e = assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
        assertTrue(e.getCause().getMessage().startsWith("Property [p] has no value to read"));
      } else {
        assertEquals(7, read(v3, bytes, "v1").get("p").asInt());
      }
    }
  }

  @Test
  void aContinuingDiscriminatorSurvivesARestartedBranchBelowIt() throws Exception {
    // f0's T28 branch is removed and re-added: its members are pruned, and the empty f0, which
    // fits all of f0's branches, used to break k1 and cost l the continuing kind.
    String k4 = "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"k4\"]}, "
        + "\"g0\": {\"type\": \"number\"}}}";
    String k5 = k4.replace("k4", "k5");
    String t28 = "{\"type\": \"object\", \"title\": \"T28\", \"properties\": {\"kind\": "
        + "{\"enum\": [\"k6\"]}, \"g0\": {\"type\": \"number\"}}}";
    String k1 = "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"k1\"]}, "
        + "\"j\": {\"type\": \"integer\"}, \"f0\": {\"oneOf\": [%s]}}}";
    String k2 = "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"k2\"]}, "
        + "\"f\": {\"type\": \"boolean\"}}}";
    String k7 = "{\"type\": \"object\", \"properties\": {\"e0\": {\"type\": \"integer\"}, "
        + "\"kind\": {\"enum\": [\"k7\"]}}}";
    String top = "{\"type\": \"object\", \"properties\": {\"l\": {\"oneOf\": [%s]}}}";
    JsonSchema v1 = new JsonSchema(String.format(top,
        String.format(k1, k4 + ", " + k5 + ", " + t28) + ", " + k2));
    JsonSchema v2 = new JsonSchema(String.format(top,
        String.format(k1, k4 + ", " + k5) + ", " + k2));
    JsonSchema v3 = new JsonSchema(String.format(top,
        k7 + ", " + String.format(k1, t28 + ", " + k4 + ", " + k5) + ", " + k2));
    byte[] bytes = write(v1,
        "{\"l\": {\"kind\": \"k1\", \"j\": 3, \"f0\": {\"kind\": \"k6\", \"g0\": 4.5}}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode l = read(v3, bytes, "v1").get("l");
    assertEquals("k1", l.get("kind").asText());
    assertEquals(3, l.get("j").asInt());
    assertFalse(l.has("f0"));
  }

  @Test
  void aValueLeftAmbiguousByPruningLosesItsAmbiguousMembers() throws Exception {
    // Only the re-added b1 kept p out of the second branch: once it is pruned, l reads two ways,
    // and with a new p.l in k9 it must read one. An x the second branch rejects keeps l.
    String body = "{\"type\": \"object\", \"properties\": {\"p\": {\"oneOf\": ["
        + "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"k1\"]}, %s"
        + "\"l\": {\"type\": \"boolean\"}}}, "
        + "{\"type\": \"object\", \"properties\": {\"b1\": {\"type\": \"integer\"}, "
        + "\"l\": {\"type\": \"boolean\"}, \"x\": {\"type\": \"integer\"}}}%s]}}}";
    String k9 = ", {\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"k9\"]}, "
        + "\"l\": {\"type\": \"boolean\"}}}";
    JsonSchema v1 = new JsonSchema(String.format(body, string("b1") + ", ", ""));
    JsonSchema v2 = new JsonSchema(String.format(body, "", ""));
    JsonSchema v3 = new JsonSchema(String.format(body, string("b1") + ", ", k9));
    byte[] bytes = write(v1, "{\"p\": {\"kind\": \"k1\", \"b1\": \"old\", \"l\": true}}");
    byte[] ruled = write(v1,
        "{\"p\": {\"kind\": \"k1\", \"b1\": \"old\", \"l\": true, \"x\": \"s\"}}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    assertEquals(MAPPER.readTree("{\"p\": {\"kind\": \"k1\"}}"), read(v3, bytes, "v1"));
    assertEquals(MAPPER.readTree("{\"p\": {\"kind\": \"k1\", \"l\": true, \"x\": \"s\"}}"),
        read(v3, ruled, "v1"));
  }

  @Test
  void aNestedUnionHoldingThePropertyAsAnExtraIsNoReadingOfIt() throws Exception {
    // The inner union fits only through B, where kind is an extra: as in the flat union, kind
    // reads at A alone and continues. l still reads two ways and goes (k9 makes p.l strict).
    String a = "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"k1\"]}, "
        + "\"l\": {\"type\": \"boolean\"}}}";
    String b = "{\"type\": \"object\", \"properties\": {\"l\": {\"type\": \"boolean\"}, "
        + "\"x\": {\"type\": \"integer\"}}}";
    String c = "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"k9\"]}, "
        + "\"l\": {\"type\": \"boolean\"}}}";
    String body = "{\"type\": \"object\", \"properties\": {\"p\": {\"oneOf\": [" + a
        + ", {\"oneOf\": [" + b + ", %s]}]}}}";
    JsonSchema v1 = new JsonSchema(String.format(body, "{\"type\": \"integer\"}"));
    JsonSchema v2 = new JsonSchema(String.format(body, c));
    byte[] bytes = write(v1, "{\"p\": {\"kind\": \"k1\", \"l\": true}}");
    client.register(SUBJECT, v2);

    assertEquals(MAPPER.readTree("{\"p\": {\"kind\": \"k1\"}}"), read(v2, bytes, "v1"));
  }

  @Test
  void aPropertyRequiredByASchemaDependencyIsDefaultedOrFails() throws Exception {
    String body = "{\"type\": \"object\", \"properties\": {\"a\": {\"type\": \"string\"}, %s}, "
        + "\"dependencies\": {\"a\": {\"required\": [\"x\"]}}}";
    for (String x : new String[] {"\"x\": {\"type\": \"string\", \"default\": \"d\"}",
        "\"x\": {\"type\": \"string\", \"description\": \"new\"}"}) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
      JsonSchema v1 = new JsonSchema(String.format(body, string("x")));
      JsonSchema v2 = new JsonSchema(String.format(body, "\"zz\": {\"type\": \"boolean\"}"));
      JsonSchema v3 = new JsonSchema(String.format(body, x));
      byte[] bytes = write(v1, "{\"a\": \"s\", \"x\": \"old\"}");
      client.register(SUBJECT, v2);
      client.register(SUBJECT, v3);

      if (x.contains("default")) {
        assertEquals("d", read(v3, bytes, "v1").get("x").asText());
      } else {
        Exception e = assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
        assertTrue(e.getCause().getMessage().startsWith("Property [x] has no value to read"));
      }
    }
  }

  @Test
  void deeplyNestedUnionsArePrunedInTimeLinearInTheirDepth() throws Exception {
    // Each union along the path asked whether its branches reach x by walking the rest of the
    // path, so the cost doubled per level; 18 levels took seconds per record.
    int depth = 18;
    String withX = "{\"type\": \"object\", \"properties\": {\"x\": {\"type\": \"string\"%s}, "
        + "\"y\": {\"type\": \"string\"}}}";
    String noX = "{\"type\": \"object\", \"properties\": {\"y\": {\"type\": \"string\"}}}";
    JsonSchema v1 = new JsonSchema(nested(depth, String.format(withX, "")));
    JsonSchema v2 = new JsonSchema(nested(depth, noX));
    JsonSchema v3 = new JsonSchema(
        nested(depth, String.format(withX, ", \"description\": \"new\"")));
    String doc = "{\"x\": \"old\", \"y\": \"keep\"}";
    for (int level = 1; level <= depth; level++) {
      doc = "{\"p" + level + "\": " + doc + "}";
    }
    byte[] bytes = write(v1, "{\"p\": " + doc + "}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode read = assertTimeoutPreemptively(Duration.ofSeconds(5), () -> read(v3, bytes, "v1"));
    JsonNode leaf = read.get("p");
    for (int level = depth; level >= 1; level--) {
      leaf = leaf.get("p" + level);
    }
    assertEquals("keep", leaf.get("y").asText());
    assertFalse(leaf.has("x"));
  }

  @Test
  void aPropertyRequiredInAnAllOfOfASchemaDependencyIsDefaultedOrFails() throws Exception {
    // The dependency's schema requires x only through an allOf part, in either draft's spelling.
    String[] bodies = {
        "{\"type\": \"object\", \"properties\": {\"a\": {\"type\": \"string\"}, %s}, "
            + "\"dependencies\": {\"a\": {\"allOf\": [{\"required\": [\"x\"]}]}}}",
        "{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", "
            + "\"properties\": {\"a\": {\"type\": \"string\"}, %s}, "
            + "\"dependentSchemas\": {\"a\": {\"allOf\": [{\"required\": [\"x\"]}]}}}"};
    for (String body : bodies) {
      for (String x : new String[] {"\"x\": {\"type\": \"string\", \"default\": \"d\"}",
          "\"x\": {\"type\": \"string\", \"description\": \"new\"}"}) {
        client = new ProvenanceMockSchemaRegistryClient();
        serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
        JsonSchema v1 = new JsonSchema(String.format(body, string("x")));
        JsonSchema v2 = new JsonSchema(String.format(body, "\"zz\": {\"type\": \"boolean\"}"));
        JsonSchema v3 = new JsonSchema(String.format(body, x));
        byte[] bytes = write(v1, "{\"a\": \"s\", \"x\": \"old\"}");
        client.register(SUBJECT, v2);
        client.register(SUBJECT, v3);

        if (x.contains("default")) {
          assertEquals("d", read(v3, bytes, "v1").get("x").asText());
        } else {
          Exception e = assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
          assertTrue(e.getCause().getMessage().startsWith("Property [x] has no value to read"));
        }
      }
    }
  }

  @Test
  void aReaderInstancePinnedOnceIsNotPinnedWhenHandedOverWithout() throws Exception {
    // v3 has v1's structure, note re-added: one reader instance with v3's text, pinned to v1
    // once, kept that pin when handed over unpinned, and gave v3's new note the old value.
    String v1Text = "{\"type\": \"object\", \"properties\": {\"id\": {\"type\": \"integer\"}, "
        + "\"note\": {\"type\": \"string\"}}}";
    JsonSchema v1 = new JsonSchema(v1Text);
    JsonSchema v3 = new JsonSchema(v1Text.replace("{\"type\": \"object\",",
        "{\"type\": \"object\", \"description\": \"v3\","));
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"old\"}");
    client.register(SUBJECT, object("\"id\": {\"type\": \"integer\"}"));
    client.register(SUBJECT, v3);
    KafkaJsonSchemaDeserializer<JsonNode> deserializer =
        new KafkaJsonSchemaDeserializer<>(client, config("v1"));

    JsonNode pinned = (JsonNode) deserializer.deserializeWithReaderSchema(TOPIC,
        new RecordHeaders(), bytes, w -> ReaderSchema.of(v3, SUBJECT, 1), false).getValue();
    JsonNode unpinned = (JsonNode) deserializer.deserializeWithSchema(TOPIC, new RecordHeaders(),
        bytes, w -> v3).getValue();
    assertEquals("old", pinned.get("note").asText());
    assertFalse(unpinned.has("note"));
  }

  @Test
  void aChainOfDependenciesIsDefaultedOrFailsAlongItsLength() throws Exception {
    // a requires x and x requires y, both re-added: x's default makes y required in turn.
    String body = "{\"type\": \"object\", \"properties\": {\"a\": {\"type\": \"string\"}, %s}, "
        + "\"dependencies\": {\"a\": [\"x\"], \"x\": [\"y\"]}}";
    for (String y : new String[] {"\"y\": {\"type\": \"string\", \"default\": \"e\"}",
        "\"y\": {\"type\": \"string\", \"description\": \"new\"}"}) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
      JsonSchema v1 = new JsonSchema(String.format(body, string("x") + ", " + string("y")));
      JsonSchema v2 = new JsonSchema(String.format(body, "\"zz\": {\"type\": \"boolean\"}"));
      JsonSchema v3 = new JsonSchema(String.format(body,
          "\"x\": {\"type\": \"string\", \"default\": \"d\"}, " + y));
      byte[] bytes = write(v1, "{\"a\": \"s\", \"x\": \"old\", \"y\": \"oldy\"}");
      client.register(SUBJECT, v2);
      client.register(SUBJECT, v3);

      if (y.contains("default")) {
        assertEquals("e", read(v3, bytes, "v1").get("y").asText());
      } else {
        Exception e = assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
        assertTrue(e.getCause().getMessage().startsWith("Property [y] has no value to read"));
      }
    }
  }

  @Test
  void aDeferredPropertyWhoseObjectWasPrunedSinceIsNotDecided() throws Exception {
    // x is new and deferred, a requiring it; pruning it leaves o fitting only B's closed object,
    // so o goes whole. x's requirement then stood for nothing, yet failed the record.
    String o = "{\"type\": \"object\", \"properties\": {\"a\": {\"type\": \"string\"}%s}%s}";
    String a = "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"k1\"]}, "
        + "\"o\": %s}}";
    String b = ", {\"type\": \"object\", \"properties\": {\"o\": "
        + String.format(o, "", ", \"additionalProperties\": false") + "}}";
    String body = "{\"type\": \"object\", \"properties\": {\"p\": {\"oneOf\": [%s%s]}}}";
    String dependency = ", \"dependencies\": {\"a\": [\"x\"]}";
    for (String branchB : new String[] {b, ""}) {
      client = new ProvenanceMockSchemaRegistryClient();
      serializer = new KafkaJsonSchemaSerializer<>(client, config(null));
      JsonSchema v1 = new JsonSchema(String.format(body,
          String.format(a, String.format(o, ", " + string("x"), dependency)), branchB));
      JsonSchema v2 = new JsonSchema(String.format(body,
          String.format(a, String.format(o, "", "")), branchB));
      JsonSchema v3 = new JsonSchema(String.format(body, String.format(a, String.format(o,
          ", \"x\": {\"type\": \"string\", \"description\": \"new\"}", dependency)), branchB));
      byte[] bytes = write(v1,
          "{\"p\": {\"kind\": \"k1\", \"o\": {\"a\": \"s\", \"x\": \"old\"}}}");
      client.register(SUBJECT, v2);
      client.register(SUBJECT, v3);

      if (branchB.isEmpty()) {
        // Control: o stays, so x is still required and has no default.
        Exception e = assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
        assertTrue(e.getCause().getMessage().startsWith("Property [p, o, x] has no value to read"));
      } else {
        assertEquals(MAPPER.readTree("{\"p\": {\"kind\": \"k1\"}}"), read(v3, bytes, "v1"));
      }
    }
  }

  @Test
  void aRootUnionIsReadOverTheWire() throws Exception {
    // A root union's branches have no names of their own: the response must still carry them.
    String root = "{\"oneOf\": [{\"type\": \"object\", \"properties\": {%s}}, "
        + "{\"type\": \"integer\"}]}";
    JsonSchema v1 = new JsonSchema(String.format(root, string("x")));
    JsonSchema v2 = new JsonSchema(String.format(root, string("x") + ", " + string("g")));
    byte[] bytes = write(v1, "{\"x\": \"s\"}");
    client.register(SUBJECT, v2);

    assertEquals("s", read(v2, bytes, "v1").get("x").asText());
  }

  @Test
  void anAllOfNamingOneDefinitionTwiceIsWalkedOncePerLevel() throws Exception {
    // Each level is allOf [$ref L, $ref L]: walked part by part, the leaf is reached 2^depth
    // times (hours here); walked once per level, a read takes about a millisecond.
    int depth = 32;
    String x = "\"x\": {\"type\": \"string\"%s}, ";
    StringBuilder defs = new StringBuilder(
        "\"L0\": {\"type\": \"object\", \"properties\": {%s\"y\": {\"type\": \"string\"}}}");
    for (int level = 1; level <= depth; level++) {
      defs.append(", \"L").append(level).append("\": {\"type\": \"object\", \"properties\": ")
          .append("{\"p\": {\"allOf\": [{\"$ref\": \"#/definitions/L").append(level - 1)
          .append("\"}, {\"$ref\": \"#/definitions/L").append(level - 1).append("\"}]}}}");
    }
    String body = "{\"type\": \"object\", \"properties\": {\"root\": {\"$ref\": "
        + "\"#/definitions/L" + depth + "\"}}, \"definitions\": {" + defs + "}}";
    JsonSchema v1 = new JsonSchema(String.format(body, String.format(x, "")));
    JsonSchema v2 = new JsonSchema(String.format(body, ""));
    JsonSchema v3 = new JsonSchema(
        String.format(body, String.format(x, ", \"description\": \"new\"")));
    String doc = "{\"x\": \"old\", \"y\": \"keep\"}";
    String kept = "{\"y\": \"keep\"}";
    for (int level = 1; level <= depth; level++) {
      doc = "{\"p\": " + doc + "}";
      kept = "{\"p\": " + kept + "}";
    }
    byte[] bytes = write(v1, "{\"root\": " + doc + "}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);

    JsonNode read = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> read(v3, bytes, "v1"));
    assertEquals(MAPPER.readTree("{\"root\": " + kept + "}"), read);
  }

  @Test
  void anIntegerBranchWidenedToNumberKeepsItsValue() throws Exception {
    String union = "\"x\": {\"oneOf\": [{\"type\": \"%s\"}, {\"type\": \"string\"}]}";
    JsonSchema v1 = object(String.format(union, "integer"));
    JsonSchema v2 = object(String.format(union, "number"));
    byte[] bytes = write(v1, "{\"x\": 3}");
    client.register(SUBJECT, v2);

    assertEquals(3, read(v2, bytes, "v1").get("x").asInt());
  }

  @Test
  void aStringBranchBecomingBytesKeepsItsValue() throws Exception {
    String union = "\"f\": {\"oneOf\": [%s, {\"type\": \"boolean\"}]}";
    JsonSchema v1 = object(String.format(union, "{\"type\": \"string\"}"));
    JsonSchema v2 = object(String.format(union,
        "{\"type\": \"string\", \"connect.type\": \"bytes\"}"));
    byte[] bytes = write(v1, "{\"f\": \"YWJj\"}");
    client.register(SUBJECT, v2);

    assertEquals("YWJj", read(v2, bytes, "v1").get("f").asText());
  }

  @Test
  void anArrayBranchWhoseItemsWidenedKeepsItsValue() throws Exception {
    String union = "\"x\": {\"oneOf\": [{\"type\": \"array\", \"items\": {\"type\": \"%s\"}}, "
        + "{\"type\": \"string\"}]}";
    JsonSchema v1 = object(String.format(union, "integer"));
    JsonSchema v2 = object(String.format(union, "number"));
    byte[] bytes = write(v1, "{\"x\": [3, 4]}");
    client.register(SUBJECT, v2);

    assertEquals(MAPPER.readTree("[3, 4]"), read(v2, bytes, "v1").get("x"));
  }

  @Test
  void aValueOfAnItemsUnionReadAsAnotherContinuingBranchIsPruned() throws Exception {
    // Hints keep both branches while they swap types: 3, written as A, would read as B, the
    // column of B's strings. As for a property's own union, it is pruned rather than moved.
    String items = "\"x\": {\"type\": \"array\", \"items\": {\"anyOf\": "
        + "[{\"type\": \"%s\"}, {\"type\": \"%s\"}], "
        + "\"confluent:union\": [{\"name\": \"A\"}, {\"name\": \"B\"}]}}";
    JsonSchema v1 = object(String.format(items, "integer", "string"));
    JsonSchema v2 = object(String.format(items, "string", "integer"));
    byte[] bytes = write(v1, "{\"x\": [3]}");
    client.register(SUBJECT, v2);

    assertFalse(read(v2, bytes, "v1").has("x"));
  }

  @Test
  void aValueOfAMapValuesUnionReadAsAnotherContinuingBranchIsPruned() throws Exception {
    String values = "\"x\": {\"type\": \"object\", \"connect.type\": \"map\", "
        + "\"additionalProperties\": {\"anyOf\": "
        + "[{\"type\": \"%s\"}, {\"type\": \"%s\"}], "
        + "\"confluent:union\": [{\"name\": \"A\"}, {\"name\": \"B\"}]}}";
    JsonSchema v1 = object(String.format(values, "integer", "string"));
    JsonSchema v2 = object(String.format(values, "string", "integer"));
    byte[] bytes = write(v1, "{\"x\": {\"k\": 3}}");
    client.register(SUBJECT, v2);

    assertFalse(read(v2, bytes, "v1").has("x"));
  }

  @Test
  void anArrayOfAnUnchangedUnionIsNotWalkedWhenAnotherPropertyChanges() throws Exception {
    // Only y changes: x's union is its own and the same on both sides, so its 100,000 elements
    // need no walk, where a comparison of the whole schema would walk them on every read.
    String x = "\"x\": {\"type\": \"array\", \"items\": "
        + "{\"anyOf\": [{\"type\": \"integer\"}, {\"type\": \"string\"}]}}";
    JsonSchema v1 = object(x, "\"y\": {\"type\": \"string\"}");
    JsonSchema v2 = object(x, "\"z\": {\"type\": \"string\"}");
    StringBuilder items = new StringBuilder();
    for (int i = 0; i < 100_000; i++) {
      items.append(i > 0 ? "," : "").append(i);
    }
    byte[] bytes = write(v1, "{\"x\": [" + items + "], \"y\": \"old\"}");
    client.register(SUBJECT, v2);
    read(v2, bytes, "v1");

    JsonNode read = assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      JsonNode last = null;
      for (int i = 0; i < 50; i++) {
        last = read(v2, bytes, "v1");
      }
      return last;
    });
    assertEquals(100_000, read.get("x").size());
  }

  @Test
  void aTypedReaderWithNoReaderSchemaGetsNoOldValue() throws Exception {
    // The application's class is the reader, as Avro's and Protobuf's generated classes are.
    JsonSchema v1 = object(number("id"), string("note"));
    JsonSchema v2 = object(number("id"));
    JsonSchema v3 = JsonSchemaUtils.getSchema(new Typed());
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);
    Typed read = new KafkaJsonSchemaDeserializer<>(client, config("v1"), Typed.class)
        .deserialize(TOPIC, bytes);
    assertEquals(7, read.id);
    assertNull(read.note);
    // Without provenance the class still reads the old value, as before.
    assertEquals("ada", new KafkaJsonSchemaDeserializer<>(client, config(null), Typed.class)
        .deserialize(TOPIC, bytes).note);
  }

  @Test
  void aWritersJavaTypeIsAReaderToo() throws Exception {
    // No configured type: the class the writer's javaType names is the reader.
    String javaType = "\"javaType\": \"" + Named.class.getName() + "\", ";
    JsonSchema v1 = new JsonSchema("{" + javaType + "\"type\": \"object\", \"properties\": {"
        + number("id") + ", " + string("note") + "}}");
    JsonSchema v2 = new JsonSchema("{" + javaType + "\"type\": \"object\", \"properties\": {"
        + number("id") + "}}");
    JsonSchema v3 = new JsonSchema("{" + javaType + "\"type\": \"object\", \"properties\": {"
        + number("id") + ", \"note\": {\"type\": \"string\", \"description\": \"new\"}}}");
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);
    Object read = new KafkaJsonSchemaDeserializer<>(client, config("v1")).deserialize(TOPIC, bytes);
    assertNull(((Named) read).note);
  }

  @Test
  void aTypedReaderWithoutADefaultConstructorGetsNoOldValue() throws Exception {
    // Jackson builds it through its creator; its schema comes from the class, not an instance.
    JsonSchema v1 = object(number("id"), string("note"));
    JsonSchema v2 = object(number("id"));
    JsonSchema v3 = JsonSchemaUtils.getSchema(new Created(1, "x"));
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);
    Created read = new KafkaJsonSchemaDeserializer<>(client, config("v1"), Created.class)
        .deserialize(TOPIC, bytes);
    assertEquals(7, read.id);
    assertNull(read.note);
  }

  @Test
  void aTypedReadPrunedOfWhatItsWriterRequiresFailsValidation() throws Exception {
    // Validated against the writer, as without provenance: note, which the writer requires, is
    // pruned for the class, whose note is new, so the record fails.
    JsonSchema v1 = new JsonSchema("{\"type\": \"object\", \"properties\": {" + number("id")
        + ", " + string("note") + "}, \"required\": [\"note\"]}");
    JsonSchema v2 = object(number("id"));
    JsonSchema v3 = JsonSchemaUtils.getSchema(new Typed());
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    client.register(SUBJECT, v2);
    client.register(SUBJECT, v3);
    Map<String, Object> config = config("v1");
    config.put("json.fail.invalid.schema", true);
    SerializationException e = assertThrows(SerializationException.class,
        () -> new KafkaJsonSchemaDeserializer<>(client, config, Typed.class)
            .deserialize(TOPIC, bytes));
    // Failed by validation for the pruned note, not by a failure to project.
    assertTrue(causes(e).contains("ValidationException") && causes(e).contains("note"),
        causes(e));
  }

  @Test
  void aValidatedTypedReadAcceptsWhatItsWriterAccepts() throws Exception {
    // The class requires a primitive its writer never had: validated as without provenance,
    // against the writer, it reads Jackson's 0.
    JsonSchema v1 = object(number("id"), string("note"));
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    client.register(SUBJECT, object(number("id")));
    client.register(SUBJECT, JsonSchemaUtils.getSchema(new WithCount()));
    Map<String, Object> config = config("v1");
    config.put("json.fail.invalid.schema", true);
    WithCount read = new KafkaJsonSchemaDeserializer<>(client, config, WithCount.class)
        .deserialize(TOPIC, bytes);
    assertEquals(0, read.count);
    assertNull(read.note);
  }

  @Test
  void aTypedReadARacingReconfigureInterruptsGetsNoOldValue() throws Exception {
    // A reconfigure replaces the projector while the read derives its class's schema: the read's
    // projection must still take the class as derived, not as v1, whose text it shares.
    String text = JsonSchemaUtils.getSchemaOfClass(Twin.Pojo.class, null, null, true, true,
        MAPPER, null).canonicalString();
    JsonSchema v1 = new JsonSchema(text);
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\", \"kind\": \"A\"}");
    String note = ",\"note\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},"
        + "{\"type\":\"string\"}]}";
    client.register(SUBJECT, new JsonSchema(text.replace(note, "")));
    // Equivalent to the class, so the version it stands for, in text unlike v1's.
    client.register(SUBJECT, new JsonSchema(text.replace("{\"type\":\"string\"}]}",
        "{\"type\":\"string\",\"description\":\"re-added\"}]}")));
    Map<String, Object> config = config("v1");
    // Named in the config too, so the reconfigure keeps it.
    config.put("json.value.type", Gated.Pojo.class.getName());
    KafkaJsonSchemaDeserializer<Gated.Pojo> deserializer =
        new KafkaJsonSchemaDeserializer<>(client, config, Gated.Pojo.class);
    AtomicReference<Gated.Pojo> read = new AtomicReference<>();
    Gated.armed = true;
    Thread reader = new Thread(() -> read.set(deserializer.deserialize(TOPIC, bytes)), "reader");
    reader.start();
    assertTrue(Gated.entered.await(10, TimeUnit.SECONDS), "the read never reached the gate");
    Thread reconfigure = new Thread(() -> deserializer.configure(config, false), "reconfigure");
    reconfigure.start();
    // The race is only run once the reconfigure is parked behind the read.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (reconfigure.getState() == Thread.State.RUNNABLE && System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
    Gated.release.countDown();
    reader.join(10_000);
    reconfigure.join(10_000);
    assertFalse(reader.isAlive() || reconfigure.isAlive(), "a thread did not finish");
    assertNotNull(read.get(), "the read failed");
    assertNull(read.get().note);
  }

  @Test
  void aWritersJavaTypeIsNotInitializedForAPayloadTheReadRefuses() throws Exception {
    // As without provenance: a scalar payload is refused before its javaType is loaded.
    int id = client.register(SUBJECT, new JsonSchema("{\"type\": \"string\", \"javaType\": \""
        + Marked.class.getName() + "\"}"));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0);
    out.write(ByteBuffer.allocate(4).putInt(id).array());
    out.write("\"x\"".getBytes(StandardCharsets.UTF_8));
    assertThrows(SerializationException.class,
        () -> new KafkaJsonSchemaDeserializer<>(client, config("v1"))
            .deserialize(TOPIC, out.toByteArray()));
    assertNull(System.getProperty(Marked.INITIALIZED));
  }

  @Test
  void aFirstTypedReadRacingAReconfigureCompletes() throws Exception {
    // The reconfigure holds the deserializer's monitor while the read first derives its class's
    // schema: neither may wait for the other.
    JsonSchema v1 = object(number("id"), string("note"));
    byte[] bytes = write(v1, "{\"id\": 7, \"note\": \"ada\"}");
    Map<String, Object> config = config("v1");
    KafkaJsonSchemaDeserializer<Typed> deserializer =
        new KafkaJsonSchemaDeserializer<>(client, config, Typed.class);
    Thread reader = new Thread(() -> deserializer.deserialize(TOPIC, bytes), "reader");
    Thread reconfigure = new Thread(() -> {
      synchronized (deserializer) {
        reader.start();
        while (reader.getState() != Thread.State.BLOCKED && reader.isAlive()) {
          Thread.onSpinWait();
        }
        deserializer.configure(config, false);
      }
    }, "reconfigure");
    reconfigure.start();
    reconfigure.join(5000);
    reader.join(5000);
    assertFalse(reconfigure.isAlive() || reader.isAlive(),
        "the read and the reconfigure deadlocked");
  }

  /** A typed reader built through a creator, with no default constructor. */
  public static class Created {
    public final Integer id;
    public final String note;

    @JsonCreator
    public Created(@JsonProperty("id") Integer id, @JsonProperty("note") String note) {
      this.id = id;
      this.note = note;
    }
  }

  /** A typed reader with a primitive, which the generated schema requires. */
  public static class WithCount {
    public Integer id;
    public int count;
    public String note;
  }

  /** A typed reader whose schema's derivation initializes an enum, held at a gate when armed. */
  public static class Gated {
    static volatile boolean armed;
    static final CountDownLatch entered = new CountDownLatch(1);
    static final CountDownLatch release = new CountDownLatch(1);

    /** Initialized while the reader's schema is derived. */
    public enum Kind {
      A, B;

      static {
        if (armed) {
          entered.countDown();
          try {
            release.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    /** The typed reader. */
    public static class Pojo {
      public Integer id;
      public String note;
      public Kind kind;
    }
  }

  /** Gated's shape, to derive v1's text from without touching the gate. */
  public static class Twin {
    /** The same values. */
    public enum Kind { A, B }

    /** The same properties. */
    public static class Pojo {
      public Integer id;
      public String note;
      public Kind kind;
    }
  }

  /** A class a writer's javaType names: initializing it is recorded. */
  public static class Marked {
    static final String INITIALIZED = "provenance.test.marked.initialized";

    static {
      System.setProperty(INITIALIZED, "true");
    }
  }

  /** A typed reader: the class an application deserializes into. */
  public static class Typed {
    public Integer id;
    public String note;
  }

  /** The class a writer's javaType names. */
  public static class Named {
    public Double id;
    public String note;
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
      config.put("provenance.algorithm", provenance);
    }
    return config;
  }

  private static String number(String name) {
    return "\"" + name + "\": {\"type\": \"number\"}";
  }

  private static String string(String name) {
    return "\"" + name + "\": {\"type\": \"string\"}";
  }

  // A branch told apart by a one-value kind, holding one number property.
  private static String kinded(String kind, String property) {
    return "{\"type\": \"object\", \"properties\": {\"kind\": {\"enum\": [\"" + kind
        + "\"]}, " + number(property) + "}}";
  }

  // p: a union, depth levels deep, of an object holding the next level and an integer.
  private static String nested(int depth, String leaf) {
    String schema = leaf;
    for (int level = 1; level <= depth; level++) {
      schema = "{\"oneOf\": [{\"type\": \"object\", \"properties\": {\"p" + level + "\": "
          + schema + "}}, {\"type\": \"integer\"}]}";
    }
    return "{\"type\": \"object\", \"properties\": {\"p\": " + schema + "}}";
  }

  private static JsonSchema object(String... properties) {
    return new JsonSchema("{\"type\": \"object\", \"title\": \"Row\", \"properties\": {"
        + String.join(", ", properties) + "}}");
  }

  private static JsonSchema modern(String... properties) {
    return new JsonSchema("{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", "
        + "\"type\": \"object\", \"title\": \"Row\", \"properties\": {"
        + String.join(", ", properties) + "}}");
  }

  // Every message in the cause chain, so a test can name the failure it expects.
  private static String causes(Throwable e) {
    StringBuilder chain = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      chain.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append('\n');
    }
    return chain.toString();
  }

}
