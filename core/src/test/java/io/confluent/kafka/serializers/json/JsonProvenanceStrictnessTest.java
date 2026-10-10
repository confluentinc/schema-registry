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

package io.confluent.kafka.serializers.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

/**
 * Deserialization strictness in JSON: a property whose provenance id is new is pruned — a
 * required one takes its default or fails the record — and a property two union branches share,
 * one continuing and one new, is decided by the branch the document validates against.
 */
class JsonProvenanceStrictnessTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String A_INT = "{\"type\":\"object\",\"properties\":"
      + "{\"a\":{\"type\":\"integer\"}},\"required\":[\"a\"]}";
  private static final String A_STRING = "{\"type\":\"object\",\"properties\":"
      + "{\"a\":{\"type\":\"string\"}},\"required\":[\"a\"]}";
  private static final String NO_A = "{\"type\":\"object\",\"properties\":{}}";

  @Test
  void aRequiredPropertyReAddedTakesItsDefault() throws Exception {
    JsonSchema v3 = object("\"t\":{\"type\":\"integer\",\"default\":9}", "\"t\"");
    JsonNode read = prune("{\"t\":2}", object("\"t\":{\"type\":\"integer\"}", null),
        object("", null), v3);
    assertEquals(MAPPER.readTree("{\"t\":9}"), read);
  }

  @Test
  void aRequiredPropertyReAddedWithNoDefaultFailsTheRecord() {
    JsonSchema v3 = object("\"t\":{\"type\":\"integer\"}", "\"t\"");
    assertThrows(SerializationException.class, () -> prune("{\"t\":2}",
        object("\"t\":{\"type\":\"integer\"}", null), object("", null), v3));
  }

  @Test
  void aSharedPropertyIsKeptForTheBranchThatContinues() throws Exception {
    // Branch 0's a continues; branch 1's a is dropped and re-added.
    JsonNode read = prune("{\"u\":{\"a\":5}}", union(A_INT, A_STRING), union(A_INT, NO_A),
        union(A_INT, A_STRING, "v3"));
    assertEquals(MAPPER.readTree("{\"u\":{\"a\":5}}"), read);
  }

  @Test
  void aSharedPropertyIsPrunedForTheBranchThatIsNew() {
    // A document of branch 1: its a is new, and required there with no default.
    assertThrows(SerializationException.class, () -> prune("{\"u\":{\"a\":\"x\"}}",
        union(A_INT, A_STRING), union(A_INT, NO_A), union(A_INT, A_STRING, "v3")));
  }

  // -------------------------------------------------------------------------------------------

  /** {@code document}, written under the first version, pruned for reading under the last. */
  @Test
  void aValueNoWriterBranchHoldsIsNotReadAsTheBranchContinuingIt() throws Exception {
    // a's string branch continues, a new array branch lands before it. "abc" fits no branch of the
    // writer, whose string is two characters, so it was never the continuing branch's value.
    JsonSchema v1 = new JsonSchema("{\"type\":\"object\",\"properties\":{\"id\":{\"type\":"
        + "\"integer\"},\"a\":{\"oneOf\":[{\"type\":\"integer\"},{\"type\":\"string\","
        + "\"minLength\":2,\"maxLength\":2}]}}}");
    JsonSchema v2 = new JsonSchema("{\"type\":\"object\",\"properties\":{\"id\":{\"type\":"
        + "\"integer\"},\"a\":{\"oneOf\":[{\"type\":\"integer\"},{\"type\":\"array\",\"items\":"
        + "{\"type\":\"string\"}},{\"type\":\"string\"}]}}}");
    assertEquals(MAPPER.readTree("{\"id\":1}"), prune("{\"id\":1,\"a\":\"abc\"}", v1, v2));
    assertEquals(MAPPER.readTree("{\"id\":1,\"a\":\"ab\"}"),
        prune("{\"id\":1,\"a\":\"ab\"}", v1, v2));
  }

  private static JsonNode prune(String document, JsonSchema... versions) throws Exception {
    List<SchemaMetadata> history = new ArrayList<>();
    for (int i = 0; i < versions.length; i++) {
      history.add(new SchemaMetadata(i + 1, i + 1, "JSON", Collections.emptyList(), ""));
    }
    SchemaProvenance provenance = ProvenanceHistory.compute("s", history,
        ProvenanceHistory.held(Arrays.<ParsedSchema>asList(versions)), false);
    JsonNode node = MAPPER.readTree(document);
    JsonProvenanceProjection.of(ProvenanceMapping.join(provenance, 1, versions.length),
        versions[versions.length - 1], versions[0]).prune(node);
    return node;
  }

  private static JsonSchema object(String properties, String required) {
    return new JsonSchema("{\"type\":\"object\",\"properties\":{" + properties + "}"
        + (required != null ? ",\"required\":[" + required + "]" : "") + "}");
  }

  private static JsonSchema union(String first, String second) {
    return union(first, second, "");
  }

  private static JsonSchema union(String first, String second, String description) {
    return new JsonSchema("{\"type\":\"object\",\"description\":\"" + description + "\","
        + "\"properties\":{\"u\":{\"oneOf\":[" + first + "," + second + "]}}}");
  }
}
