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

package io.confluent.kafka.serializers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The Avro renamer finds locations by the names the converter records, so every location's names
 * must lead through the native schema, and a schema renamed against itself must leave no field
 * unmatched.
 */
class AvroProvenancePathConformanceTest {

  static Stream<String> corpus() {
    return Stream.of(
        record("{\"name\":\"a\",\"type\":\"int\"}", "{\"name\":\"b\",\"type\":\"string\"}"),
        // Nullable wraps, null first or last: no path step.
        record("{\"name\":\"a\",\"type\":[\"null\",\"int\"]}",
            "{\"name\":\"b\",\"type\":[\"string\",\"null\"]}"),
        // Proper unions, null anywhere among the branches.
        record("{\"name\":\"u\",\"type\":[\"int\",\"null\",\"string\"]}"),
        record("{\"name\":\"u\",\"type\":[\"null\",\"int\",\"string\",\"boolean\"]}"),
        record("{\"name\":\"u\",\"type\":[\"int\",\"string\"]}"),
        // Records as union branches, nullable and proper.
        record("{\"name\":\"r\",\"type\":[\"null\"," + inner("A", "x", "int") + "]}"),
        record("{\"name\":\"r\",\"type\":[" + inner("A", "x", "int") + ",\"null\","
            + inner("B", "y", "string") + "]}"),
        // Namespaced branches of a proper union: records, records sharing a simple name, an enum
        // and a fixed. A branch is found natively by its full name.
        record("{\"name\":\"r\",\"type\":[\"string\",{\"type\":\"record\",\"name\":\"A\","
            + "\"namespace\":\"n1\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}]}"),
        record("{\"name\":\"r\",\"type\":[{\"type\":\"record\",\"name\":\"A\",\"namespace\":"
            + "\"n1\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]},{\"type\":\"record\","
            + "\"name\":\"A\",\"namespace\":\"n2\",\"fields\":[{\"name\":\"y\","
            + "\"type\":\"int\"}]}]}"),
        record("{\"name\":\"e\",\"type\":[\"string\",{\"type\":\"enum\",\"name\":\"E\","
            + "\"namespace\":\"n1\",\"symbols\":[\"X\"]},{\"type\":\"fixed\",\"name\":\"F\","
            + "\"namespace\":\"n1\",\"size\":2}]}"),
        // Arrays and maps of records, of unions, and nested.
        record("{\"name\":\"xs\",\"type\":{\"type\":\"array\",\"items\":"
            + inner("A", "x", "int") + "}}"),
        record("{\"name\":\"m\",\"type\":{\"type\":\"map\",\"values\":"
            + inner("A", "x", "int") + "}}"),
        record("{\"name\":\"xs\",\"type\":{\"type\":\"array\",\"items\":"
            + "[\"null\",\"int\",\"string\"]}}"),
        record("{\"name\":\"m\",\"type\":{\"type\":\"map\",\"values\":[\"null\","
            + inner("A", "x", "int") + "]}}"),
        record("{\"name\":\"xss\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"map\","
            + "\"values\":{\"type\":\"array\",\"items\":" + inner("A", "x", "int") + "}}}}"),
        record("{\"name\":\"xs\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\","
            + inner("A", "x", "int") + "]}]}"),
        // A union holding an array and a map.
        record("{\"name\":\"u\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"int\"},"
            + "{\"type\":\"map\",\"values\":" + inner("A", "x", "int") + "}]}"),
        // A named type used at several locations.
        record("{\"name\":\"home\",\"type\":" + inner("Address", "city", "string") + "}",
            "{\"name\":\"work\",\"type\":[\"null\",\"Address\"]}",
            "{\"name\":\"past\",\"type\":{\"type\":\"array\",\"items\":\"Address\"}}"),
        // Deep nesting of records.
        record("{\"name\":\"o\",\"type\":{\"type\":\"record\",\"name\":\"O\",\"fields\":["
            + "{\"name\":\"i\",\"type\":{\"type\":\"record\",\"name\":\"I\",\"fields\":["
            + "{\"name\":\"n\",\"type\":[\"null\"," + inner("N", "leaf", "long") + "]}]}}]}}"),
        // Enums and fixed, bare, nullable, and in proper unions.
        record("{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"E\","
            + "\"symbols\":[\"A\",\"B\"]}}",
            "{\"name\":\"f\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"F\",\"size\":4}]}",
            "{\"name\":\"u\",\"type\":[\"E\",\"F\",\"int\"]}"),
        // Logical types, including in unions.
        record("{\"name\":\"d\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":10,\"scale\":2}}",
            "{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\","
            + "\"logicalType\":\"timestamp-millis\"}]}",
            "{\"name\":\"day\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}",
            "{\"name\":\"id\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}",
            "{\"name\":\"u\",\"type\":[{\"type\":\"int\",\"logicalType\":\"date\"},"
            + "{\"type\":\"fixed\",\"name\":\"D\",\"size\":8,\"logicalType\":\"decimal\","
            + "\"precision\":16,\"scale\":4}]}"),
        // Connect maps with keys that are not strings: arrays of key/value records.
        record("{\"name\":\"m\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\","
            + "\"name\":\"MapEntry\",\"namespace\":\"io.confluent.connect.avro\",\"fields\":["
            + "{\"name\":\"key\",\"type\":\"int\"},{\"name\":\"value\",\"type\":"
            + inner("A", "x", "int") + "}]}}}"),
        record("{\"name\":\"m\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\","
            + "\"name\":\"E\",\"connect.internal.type\":\"MapEntry\",\"fields\":["
            + "{\"name\":\"key\",\"type\":" + inner("K", "k", "int") + "},{\"name\":\"value\","
            + "\"type\":\"int\"}]}}}"),
        // A Flink multiset as an array of entries.
        record("{\"name\":\"ms\",\"type\":{\"type\":\"array\",\"flink.type\":\"multiset\","
            + "\"items\":{\"type\":\"record\",\"name\":\"MapEntry\","
            + "\"namespace\":\"io.confluent.connect.avro\",\"fields\":[{\"name\":\"key\",\"type\":"
            + inner("K", "k", "int") + "},{\"name\":\"value\",\"type\":\"int\"}]}}}"),
        // A Variant, a leaf to the logical type.
        record("{\"name\":\"v\",\"type\":{\"type\":\"record\",\"name\":\"Variant\","
            + "\"namespace\":\"confluent.type\",\"fields\":[{\"name\":\"metadata\",\"type\":\"bytes\"},"
            + "{\"name\":\"value\",\"type\":\"bytes\"}]}}"),
        // Field and type aliases, and namespaces.
        "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"n.s\",\"aliases\":[\"Old\"],"
            + "\"fields\":[{\"name\":\"a\",\"type\":\"int\",\"aliases\":[\"b\"]},"
            + "{\"name\":\"c\",\"type\":{\"type\":\"record\",\"name\":\"C\",\"namespace\":\"o\","
            + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}}]}");
  }

  @ParameterizedTest
  @MethodSource("corpus")
  void everyLocationsNamesReachAFieldOrBranch(String json) {
    Schema schema = new Schema.Parser().parse(json);
    List<ProvenanceField> fields = provenanceOf(schema).getVersions().get(0).getFields();

    assertFalse(fields.isEmpty());
    for (ProvenanceField field : fields) {
      assertTrue(field.getNames() != null && reaches(schema, field.getNames()),
          field.getNames() + " at " + field.getPath());
    }
  }

  @ParameterizedTest
  @MethodSource("corpus")
  void renamingASchemaAgainstItselfMatchesEveryField(String json) {
    Schema schema = new Schema.Parser().parse(json);
    ProvenanceMapping identity = ProvenanceMapping.join(provenanceOf(schema), 1, 1);

    AvroProvenanceRenamer.Renamed renamed = AvroProvenanceRenamer.rename(schema, schema, identity);

    assertFalse(renamed.writer.toString().contains("__provenance_unmatched_"),
        renamed.writer.toString());
  }

  // -------------------------------------------------------------------------------------------

  /**
   * Whether {@code names} lead through {@code schema} as the renamer walks it: a field by name, a
   * union branch by its type's full name, null for an array element or map value.
   */
  private static boolean reaches(Schema schema, List<String> names) {
    Schema at = schema;
    for (String step : names) {
      if (at == null) {
        return false;
      }
      if (step == null) {
        at = at.getType() == Type.ARRAY ? at.getElementType()
            : at.getType() == Type.MAP ? at.getValueType() : null;
      } else if (at.getType() == Type.RECORD && at.getField(step) != null) {
        at = at.getField(step).schema();
      } else if (at.getType() == Type.UNION) {
        at = at.getTypes().stream().filter(b -> b.getFullName().equals(step)).findFirst()
            .orElse(null);
      } else {
        return false;
      }
    }
    return at != null;
  }

  private static SchemaProvenance provenanceOf(Schema schema) {
    return ProvenanceHistory.compute("s",
        Collections.singletonList(new ProvenanceHistory.Entry(1, 1, false)),
        Collections.singletonList(new AvroSchema(schema)), false);
  }

  private static String record(String... fields) {
    return "{\"type\":\"record\",\"name\":\"R\",\"fields\":[" + String.join(",", fields) + "]}";
  }

  private static String inner(String name, String field, String type) {
    return "{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[{\"name\":\"" + field
        + "\",\"type\":\"" + type + "\"}]}";
  }
}
