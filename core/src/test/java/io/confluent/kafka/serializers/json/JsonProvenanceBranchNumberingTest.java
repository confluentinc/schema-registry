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

package io.confluent.kafka.serializers.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The pruner numbers union branches the way the converter does: non-null subschemas in order, no
 * step for a nullable union V1 collapses. Each case pins both — the branch choices the response
 * gives a property, and the ones the pruner resolves for a document of that branch.
 */
class JsonProvenanceBranchNumberingTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INT = "{\"type\":\"integer\"}";
  private static final String STRING = "{\"type\":\"string\"}";
  private static final String NULL = "{\"type\":\"null\"}";

  static Stream<Arguments> cases() {
    return Stream.of(
        of("a two-branch oneOf", u("oneOf", INT, obj("a")), names("u", "a"),
            "{\"u\":{\"a\":1}}", 1),
        of("a nullable oneOf, collapsed", u("oneOf", NULL, obj("a")), names("u", "a"),
            "{\"u\":{\"a\":1}}"),
        of("null among the branches", u("oneOf", obj("a"), NULL, obj("b")), names("u", "b"),
            "{\"u\":{\"b\":1}}", 1),
        of("a bare one-branch oneOf, kept in V1", u("oneOf", obj("a")), names("u", "a"),
            "{\"u\":{\"a\":1}}", 0),
        of("an anyOf", u("anyOf", STRING, obj("a")), names("u", "a"), "{\"u\":{\"a\":1}}", 1),
        of("a union in an array", "{\"type\":\"object\",\"properties\":{\"xs\":{\"type\":"
                + "\"array\",\"items\":{\"oneOf\":[" + INT + "," + obj("a") + "]}}}}",
            names("xs", null, "a"), "{\"xs\":[{\"a\":1}]}", 1),
        of("a referenced branch", "{\"type\":\"object\",\"properties\":{\"u\":{\"oneOf\":["
                + INT + ",{\"$ref\":\"#/definitions/O\"}]}},\"definitions\":{\"O\":" + obj("a")
                + "}}", names("u", "a"), "{\"u\":{\"a\":1}}", 1),
        of("a union in a union", u("oneOf", INT, "{\"oneOf\":[" + STRING + "," + obj("a") + "]}"),
            names("u", "a"), "{\"u\":{\"a\":1}}", 1, 1));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void thePrunerTakesTheBranchesTheResponseNumbers(String label, String schema,
      List<String> names, String document, List<Integer> expected) throws Exception {
    JsonSchema reader = new JsonSchema(schema);
    SchemaProvenance provenance = ProvenanceHistory.compute("s",
        Collections.singletonList(new ProvenanceHistory.Entry(1, 1, false)),
        Collections.<ParsedSchema>singletonList(reader), false);
    ProvenanceField location = provenance.getVersions().get(0).getFields().stream()
        .filter(f -> names.equals(f.getNames())).findFirst().orElse(null);
    assertNotNull(location, "no location spelled " + names);

    assertEquals(expected, JsonProvenancePruner.branchChoicesAt(
        ProvenanceMapping.join(provenance, 1, 1), location.getPath()), label + ": the response");
    assertEquals(expected, JsonProvenancePruner.branchesTaken(
        reader, MAPPER.readTree(document), names), label + ": the pruner");
  }

  // -------------------------------------------------------------------------------------------

  private static Arguments of(String label, String schema, List<String> names, String document,
      Integer... expected) {
    return Arguments.of(label, schema, names, document, Arrays.asList(expected));
  }

  private static List<String> names(String... steps) {
    return Arrays.asList(steps);
  }

  private static String obj(String property) {
    return "{\"type\":\"object\",\"properties\":{\"" + property + "\":" + INT + "}}";
  }

  private static String u(String criterion, String... branches) {
    return "{\"type\":\"object\",\"properties\":{\"u\":{\"" + criterion + "\":["
        + String.join(",", branches) + "]}}}";
  }
}
