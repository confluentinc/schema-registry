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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The Avro renamer addresses fields by index path, so every path it looks up must be one the
 * logical-type provenance reports. Each corpus schema is walked the renamer's way, then renamed
 * against itself, which must leave no field unmatched.
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
        // Field and type aliases, and namespaces.
        "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"n.s\",\"aliases\":[\"Old\"],"
            + "\"fields\":[{\"name\":\"a\",\"type\":\"int\",\"aliases\":[\"b\"]},"
            + "{\"name\":\"c\",\"type\":{\"type\":\"record\",\"name\":\"C\",\"namespace\":\"o\","
            + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}}]}");
  }

  @ParameterizedTest
  @MethodSource("corpus")
  void everyPathTheRenamerLooksUpIsReported(String json) {
    Schema schema = new Schema.Parser().parse(json);
    SchemaProvenance provenance = provenanceOf(schema);
    Set<List<Integer>> reported = new HashSet<>();
    for (ProvenanceField field : provenance.getVersions().get(0).getFields()) {
      reported.add(field.getPath());
    }

    List<List<Integer>> lookedUp = new ArrayList<>();
    walk(schema, Collections.emptyList(), lookedUp);

    List<List<Integer>> missing = new ArrayList<>(lookedUp);
    missing.removeAll(reported);
    assertFalse(lookedUp.isEmpty());
    assertTrue(missing.isEmpty(), "not reported: " + missing + " of " + reported);
  }

  @ParameterizedTest
  @MethodSource("corpus")
  void renamingASchemaAgainstItselfMatchesEveryField(String json) {
    Schema schema = new Schema.Parser().parse(json);
    ProvenanceMapping identity = ProvenanceMapping.join(provenanceOf(schema), 1, 1);

    AvroProvenanceRenamer.Renamed renamed = AvroProvenanceRenamer.rename(schema, schema, identity);

    assertFalse(renamed.writer.toString().contains("__provenance_unmatched_"),
        renamed.writer.toString());
    AvroProvenanceRenamer.requireEveryFieldHasAValue(renamed);
  }

  // -------------------------------------------------------------------------------------------

  /**
   * The renamer's walk: a record field and a proper-union branch are looked up; an array element
   * steps 0, a map value 1, and a nullable wrap takes no step.
   */
  private static void walk(Schema schema, List<Integer> path, List<List<Integer>> lookedUp) {
    switch (schema.getType()) {
      case RECORD:
        for (Field field : schema.getFields()) {
          List<Integer> fieldPath = append(path, field.pos());
          lookedUp.add(fieldPath);
          walk(field.schema(), fieldPath, lookedUp);
        }
        break;
      case ARRAY:
        walk(schema.getElementType(), append(path, 0), lookedUp);
        break;
      case MAP:
        walk(schema.getValueType(), append(path, 1), lookedUp);
        break;
      case UNION:
        List<Schema> branches = new ArrayList<>();
        for (Schema branch : schema.getTypes()) {
          if (branch.getType() != Type.NULL) {
            branches.add(branch);
          }
        }
        if (branches.size() == 1) {
          walk(branches.get(0), path, lookedUp);
        } else {
          for (int i = 0; i < branches.size(); i++) {
            List<Integer> branchPath = append(path, i);
            lookedUp.add(branchPath);
            walk(branches.get(i), branchPath, lookedUp);
          }
        }
        break;
      default:
        break;
    }
  }

  private static SchemaProvenance provenanceOf(Schema schema) {
    return ProvenanceHistory.compute("s",
        Collections.singletonList(new ProvenanceHistory.Entry(1, 1, false)),
        Collections.singletonList(new AvroSchema(schema)), false);
  }

  private static List<Integer> append(List<Integer> path, int step) {
    List<Integer> appended = new ArrayList<>(path);
    appended.add(step);
    return appended;
  }

  private static String record(String... fields) {
    return "{\"type\":\"record\",\"name\":\"R\",\"fields\":[" + String.join(",", fields) + "]}";
  }

  private static String inner(String name, String field, String type) {
    return "{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[{\"name\":\"" + field
        + "\",\"type\":\"" + type + "\"}]}";
  }
}
