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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    assertEquals("Property [o, t] is new to the reader: provenance pairs it with nothing the "
        + "writer wrote, and the reader requires it and declares no default. There is no value "
        + "to read.", e.getCause().getMessage());
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
    assertTrue(e.getCause().getMessage().startsWith("Property [p, t] is new to the reader"));
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

  private static JsonSchema object(String... properties) {
    return new JsonSchema("{\"type\": \"object\", \"title\": \"Row\", \"properties\": {"
        + String.join(", ", properties) + "}}");
  }

  private static JsonSchema modern(String... properties) {
    return new JsonSchema("{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", "
        + "\"type\": \"object\", \"title\": \"Row\", \"properties\": {"
        + String.join(", ", properties) + "}}");
  }
}
