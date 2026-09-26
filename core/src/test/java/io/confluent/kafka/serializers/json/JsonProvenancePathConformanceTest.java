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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The JSON pruner finds properties by the names provenance reports. In each case a property is
 * dropped and re-added, so reading the first version's document under the last must remove it
 * wherever the schema puts it, and nothing else.
 */
class JsonProvenancePathConformanceTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String T = ",\"t\":{\"type\":\"integer\"}";
  private static final String INT = "{\"type\":\"integer\"}";

  static Stream<Arguments> corpus() {
    return Stream.of(
        prunes("top level", object("\"a\":" + INT + "%s"),
            "{\"a\":1,\"t\":2}", "{\"a\":1}"),
        prunes("nested object", object("\"o\":" + object("\"a\":" + INT + "%s")),
            "{\"o\":{\"a\":1,\"t\":2}}", "{\"o\":{\"a\":1}}"),
        prunes("array of objects", object("\"xs\":{\"type\":\"array\",\"items\":"
                + object("\"a\":" + INT + "%s") + "}"),
            "{\"xs\":[{\"a\":1,\"t\":2},{\"a\":3,\"t\":4}]}", "{\"xs\":[{\"a\":1},{\"a\":3}]}"),
        prunes("map of objects", object("\"m\":{\"type\":\"object\",\"connect.type\":\"map\","
                + "\"additionalProperties\":" + object("\"a\":" + INT + "%s") + "}"),
            "{\"m\":{\"k1\":{\"a\":1,\"t\":2},\"k2\":{\"a\":3,\"t\":4}}}",
            "{\"m\":{\"k1\":{\"a\":1},\"k2\":{\"a\":3}}}"),
        // A map with keys that are not strings is an array of key/value entries.
        prunes("map as entries, in the value", object("\"m\":{\"type\":\"array\","
                + "\"connect.type\":\"map\",\"items\":" + object("\"key\":" + INT + ",\"value\":"
                + object("\"a\":" + INT + "%s")) + "}"),
            "{\"m\":[{\"key\":1,\"value\":{\"a\":1,\"t\":2}}]}",
            "{\"m\":[{\"key\":1,\"value\":{\"a\":1}}]}"),
        prunes("map as entries, in the key", object("\"m\":{\"type\":\"array\","
                + "\"connect.type\":\"map\",\"items\":" + object("\"key\":"
                + object("\"a\":" + INT + "%s") + ",\"value\":" + INT) + "}"),
            "{\"m\":[{\"key\":{\"a\":1,\"t\":2},\"value\":3}]}",
            "{\"m\":[{\"key\":{\"a\":1},\"value\":3}]}"),
        Arguments.of("a re-added property named value, in a map as entries",
            ",\"value\":" + INT, object("\"m\":{\"type\":\"array\",\"connect.type\":\"map\","
                + "\"items\":" + object("\"key\":" + INT + ",\"value\":"
                + object("\"a\":" + INT + "%s")) + "}"),
            "{\"m\":[{\"key\":1,\"value\":{\"a\":1,\"value\":2}}]}",
            "{\"m\":[{\"key\":1,\"value\":{\"a\":1}}]}"),
        prunes("definitions and $ref", "{\"type\":\"object\",\"properties\":{\"o\":"
                + "{\"$ref\":\"#/definitions/O\"}},\"definitions\":{\"O\":"
                + object("\"a\":" + INT + "%s") + "}}",
            "{\"o\":{\"a\":1,\"t\":2}}", "{\"o\":{\"a\":1}}"),
        // Property names that look like tokens or escapes are plain names.
        prunes("property named []", object("\"[]\":" + object("\"a\":" + INT + "%s")),
            "{\"[]\":{\"a\":1,\"t\":2}}", "{\"[]\":{\"a\":1}}"),
        prunes("property named {value}", object("\"{value}\":" + object("\"a\":" + INT + "%s")),
            "{\"{value}\":{\"a\":1,\"t\":2}}", "{\"{value}\":{\"a\":1}}"),
        prunes("property named with a leading $",
            object("\"$x\":" + object("\"a\":" + INT + "%s")),
            "{\"$x\":{\"a\":1,\"t\":2}}", "{\"$x\":{\"a\":1}}"),
        prunes("property named with a leading $$",
            object("\"$$x\":" + object("\"a\":" + INT + "%s")),
            "{\"$$x\":{\"a\":1,\"t\":2}}", "{\"$$x\":{\"a\":1}}"),
        prunes("property named $$[]", object("\"$$[]\":" + object("\"a\":" + INT + "%s")),
            "{\"$$[]\":{\"a\":1,\"t\":2}}", "{\"$$[]\":{\"a\":1}}"),
        Arguments.of("a re-added property named with a leading $$", ",\"$$t\":" + INT,
            object("\"a\":" + INT + "%s"), "{\"a\":1,\"$$t\":2}", "{\"a\":1}"),
        Arguments.of("a re-added property named []", ",\"[]\":" + INT,
            object("\"xs\":{\"type\":\"array\",\"items\":" + object("\"a\":" + INT + "%s")
                + "}"),
            "{\"xs\":[{\"a\":1,\"[]\":2}]}", "{\"xs\":[{\"a\":1}]}"),
        // Shapes the logical type does not model as properties are left alone.
        prunes("additionalProperties without connect.type",
            object("\"m\":{\"type\":\"object\",\"additionalProperties\":"
                + object("\"a\":" + INT + "%s") + "}"),
            "{\"m\":{\"k\":{\"a\":1,\"t\":2}}}", "{\"m\":{\"k\":{\"a\":1,\"t\":2}}}"),
        prunes("patternProperties", "{\"type\":\"object\",\"properties\":{\"a\":" + INT
                + "%s},\"patternProperties\":{\"^p\":" + INT + "}}",
            "{\"a\":1,\"t\":2,\"p1\":3}", "{\"a\":1,\"p1\":3}"),
        // A tuple has no logical form, so its schema has no provenance at all.
        prunes("tuple items", object("\"tup\":{\"type\":\"array\",\"items\":["
                + object("\"a\":" + INT + "%s") + "," + INT + "]}"), "{}", null),
        // A re-added union branch has no property of its own: the field holding it stays.
        Arguments.of("a re-added oneOf branch", ",{\"type\":\"string\"}",
            object("\"u\":{\"oneOf\":[" + INT + "%s]}"), "{\"u\":5}", "{\"u\":5}"),
        // A union branch is no step in the document, so a property inside one is found.
        prunes("inside a oneOf branch", object("\"u\":{\"oneOf\":[" + INT + ","
                + object("\"a\":" + INT + "%s") + "]}"),
            "{\"u\":{\"a\":1,\"t\":2}}", "{\"u\":{\"a\":1}}"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("corpus")
  void theReAddedPropertyIsRemovedWhereverItIs(String label, String property, String template,
      String document, String expected) throws Exception {
    JsonSchema v1 = new JsonSchema(String.format(template, property));
    JsonSchema v2 = new JsonSchema(String.format(template, ""));
    List<ProvenanceHistory.Entry> history = Arrays.asList(new ProvenanceHistory.Entry(1, 1, false),
        new ProvenanceHistory.Entry(2, 2, false), new ProvenanceHistory.Entry(3, 3, false));
    List<ParsedSchema> schemas = Arrays.asList(v1, v2, v1);
    if (expected == null) {
      assertThrows(ValidationException.class,
          () -> ProvenanceHistory.compute("s", history, schemas, false));
      return;
    }
    SchemaProvenance provenance = ProvenanceHistory.compute("s", history, schemas, false);
    JsonNode read = MAPPER.readTree(document);

    JsonProvenancePruner.plan(ProvenanceMapping.join(provenance, 1, 3), v1).prune(read);

    assertEquals(MAPPER.readTree(expected), read);
  }

  // -------------------------------------------------------------------------------------------

  private static Arguments prunes(String label, String template, String document,
      String expected) {
    return Arguments.of(label, T, template, document, expected);
  }

  private static String object(String properties) {
    return "{\"type\":\"object\",\"properties\":{" + properties + "}}";
  }
}
