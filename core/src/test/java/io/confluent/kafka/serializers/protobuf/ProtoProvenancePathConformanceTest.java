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

package io.confluent.kafka.serializers.protobuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The Protobuf renumberer finds fields by the names the converter records, so every location's
 * names must reach a field of the descriptor, and a renumbering moves only the outermost fields
 * that have no writer counterpart.
 */
class ProtoProvenancePathConformanceTest {

  static Stream<Arguments> corpus() {
    return Stream.of(
        // Flink's own output, as LogicalTypeToProtoConverter writes it.
        flink("nullable array of rows", field("xs", array(row("x", "y")).setNullable(true))),
        flink("array of nullable rows", field("xs", array(row("x", "y").setNullable(true)))),
        flink("nullable array of nullable rows",
            field("xs", array(row("x", "y").setNullable(true)).setNullable(true))),
        flink("array of arrays of rows", field("xss", array(array(row("x"))))),
        flink("nullable map of rows", field("m", map(row("x")).setNullable(true))),
        flink("map of nullable arrays of rows", field("m", map(array(row("x")).setNullable(true)))),
        flink("array of maps of rows", field("ms", array(map(row("x"))))),
        flink("array of unions", field("us", array(union("a", row("x"), "b", row("y"))))),
        flink("union in union", field("u", union("i", row("x"),
            "inner", union("s", row("y"), "b", row("z"))))),
        // Hand-written shapes.
        proto3("native map", "map<string, Inner> m = 1;", "message Inner { int32 x = 1; }"),
        proto3("proto3 optional", "optional int32 a = 1; int32 b = 2;"),
        proto3("oneof", "oneof choice { int32 a = 1; Inner b = 2; }",
            "message Inner { int32 x = 1; }"),
        proto3("well-known wrappers", "google.protobuf.Int32Value w = 1; int32 b = 2;"),
        proto3("shared nested message", "Inner a = 1; repeated Inner b = 2;",
            "message Inner { int32 x = 1; }"),
        proto2("proto2 group", "optional group G = 1 { optional int32 x = 2; }"),
        proto2("proto2 extension range", "optional int32 a = 1; extensions 100 to 200;"),
        multi("multi-message", "message Order { int32 id = 1; Line line = 2; }",
            "message Line { string sku = 1; }"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("corpus")
  void everyLocationsNamesReachAFieldOfTheDescriptor(
      String label, ProtobufSchema reader, boolean multi) {
    ProvenanceVersion version = ProvenanceHistory.compute("s",
        Arrays.asList(new ProvenanceHistory.Entry(1, 1, false)),
        Arrays.<ParsedSchema>asList(reader), multi).getVersions().get(0);
    Map<List<Integer>, List<String>> names = new HashMap<>();
    version.getFields().forEach(f -> names.put(f.getPath(), f.getNames()));

    for (ProvenanceField field : version.getFields()) {
      assertNotNull(field.getNames(), "names at " + field.getPath());
      List<Integer> parent = field.getPath().subList(0, field.getPath().size() - 1);
      if (field.getNames().equals(names.getOrDefault(parent, Collections.emptyList()))) {
        continue; // a oneof: no step of its own
      }
      if (multi && field.getNames().size() == 1) {
        // A top-level message of a multi-message file.
        assertNotNull(reader.toDescriptor().getFile()
            .findMessageTypeByName(simple(field.getNames().get(0))), field.getNames().toString());
        continue;
      }
      assertNotNull(walk(reader.toDescriptor(), field.getNames(), multi),
          field.getNames() + " at " + field.getPath());
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("corpus")
  void againstAWriterSharingNothingOnlyTheOutermostFieldsMove(
      String label, ProtobufSchema reader, boolean multi) {
    ProtobufSchema writer = new ProtobufSchema("syntax = \"proto3\";\nmessage Nothing {}\n");
    SchemaProvenance provenance = ProvenanceHistory.compute("s",
        Arrays.asList(new ProvenanceHistory.Entry(1, 1, false),
            new ProvenanceHistory.Entry(2, 2, false)),
        Arrays.<ParsedSchema>asList(writer, reader), multi);

    ProtobufSchema renumbered = ProtoProvenanceRenumberer.renumber(
        reader, ProvenanceMapping.join(provenance, 1, 2), multi).schema;

    // Nothing under a moving field is read; a multi-message writer's record is of a message the
    // reader declares or fails, so there nothing moves at all.
    Descriptor root = reader.toDescriptor();
    Map<String, Integer> before = numbers(root);
    Map<String, Boolean> moved = new TreeMap<>();
    Map<String, Boolean> expected = new TreeMap<>();
    numbers(renumbered.toDescriptor()).forEach((name, number) -> {
      moved.put(name, !number.equals(before.get(name)));
      expected.put(name, !multi && root.findFieldByName(simple(name)) != null
          && name.equals(root.getFullName() + "." + simple(name)));
    });
    assertEquals(expected, moved);
  }

  /** The field {@code names} reach, walking as the renumberer does; null if they do not. */
  private static FieldDescriptor walk(Descriptor root, List<String> names, boolean multi) {
    Descriptor message = root;
    int i = 0;
    if (multi) {
      message = root.getFile().findMessageTypeByName(simple(names.get(0)));
      i = 1;
    }
    FieldDescriptor field = null;
    for (; i < names.size(); i++) {
      if (message == null || names.get(i) == null) {
        return null;
      }
      field = message.findFieldByName(names.get(i));
      if (field == null) {
        return null;
      }
      message = field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
          ? field.getMessageType() : null;
    }
    return field;
  }

  private static String simple(String fullName) {
    return fullName.substring(fullName.lastIndexOf('.') + 1);
  }

  // -------------------------------------------------------------------------------------------

  private static Map<String, Integer> numbers(Descriptor root) {
    Map<String, Integer> numbers = new TreeMap<>();
    for (Descriptor message : root.getFile().getMessageTypes()) {
      collect(message, numbers);
    }
    return numbers;
  }

  private static void collect(Descriptor message, Map<String, Integer> numbers) {
    for (FieldDescriptor field : message.getFields()) {
      numbers.put(field.getFullName(), field.getNumber());
    }
    for (Descriptor nested : message.getNestedTypes()) {
      collect(nested, numbers);
    }
  }

  private static Arguments flink(String label, Field field) {
    Schema root = Schema.createStruct(Arrays.asList(field)).setNullable(false);
    return Arguments.of(label,
        LogicalTypeToProtoConverter.fromLogicalType(new LogicalType(root), "Row"), false);
  }

  private static Arguments proto3(String label, String... body) {
    return Arguments.of(label, new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"google/protobuf/wrappers.proto\";\n"
        + "message Row {\n" + String.join("\n", body) + "\n}\n"), false);
  }

  private static Arguments proto2(String label, String body) {
    return Arguments.of(label, new ProtobufSchema("syntax = \"proto2\";\npackage p;\n"
        + "message Row {\n" + body + "\n}\n"), false);
  }

  private static Arguments multi(String label, String... messages) {
    return Arguments.of(label, new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + String.join("\n", messages) + "\n"), true);
  }

  private static Field field(String name, Schema schema) {
    return new Field(name, schema, 0);
  }

  private static Schema row(String... names) {
    Field[] fields = new Field[names.length];
    for (int i = 0; i < names.length; i++) {
      fields[i] = new Field(names[i], Schema.create(Schema.Type.INT).setNullable(false), i);
    }
    return Schema.createStruct(Arrays.asList(fields)).setNullable(false);
  }

  private static Schema array(Schema element) {
    return Schema.createArray(element).setNullable(false);
  }

  private static Schema map(Schema value) {
    return Schema.createMap(Schema.createString().setNullable(false), value).setNullable(false);
  }

  private static Schema union(String a, Schema first, String b, Schema second) {
    return Schema.createUnion(Arrays.asList(new UnionBranch(a, first), new UnionBranch(b, second)))
        .setNullable(false);
  }
}
