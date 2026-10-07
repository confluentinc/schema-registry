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

package io.confluent.kafka.schemaregistry.type.logical.protobuf.v1;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that proto3 scalar fields without explicit presence get the
 * proto-spec implicit default in {@link LogicalType#getDefaultValues()}.
 * Fields with presence (proto3 {@code optional}, fields in oneofs, MESSAGE
 * types, repeated) are skipped.
 */
class ProtoReaderProto3ImplicitDefaultsTest {

  @Test
  void implicitScalarsCaptured() {
    String protoText =
        "syntax = \"proto3\";\n"
            + "package test;\n"
            + "message Row {\n"
            + "  int32 i = 1;\n"           // no presence -> implicit 0
            + "  int64 l = 2;\n"           // no presence -> implicit 0L
            + "  float f = 3;\n"           // no presence -> implicit 0.0f
            + "  double d = 4;\n"          // no presence -> implicit 0.0
            + "  bool b = 5;\n"            // no presence -> implicit false
            + "  string s = 6;\n"          // no presence -> implicit ""
            + "  bytes by = 7;\n"          // no presence -> implicit empty bytes
            + "  Color c = 8;\n"           // no presence -> implicit RED (first decl)
            + "  optional int32 oi = 9;\n" // presence -> SKIP
            + "}\n"
            + "enum Color { RED = 0; GREEN = 1; BLUE = 2; }\n";

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(
        new ProtobufSchema(protoText));

    Map<List<Integer>, Object> defaults = lt.getDefaultValues();
    assertThat(defaults).containsEntry(List.of(0), 0);
    assertThat(defaults).containsEntry(List.of(1), 0L);
    assertThat(defaults).containsEntry(List.of(2), 0.0f);
    assertThat(defaults).containsEntry(List.of(3), 0.0);
    assertThat(defaults).containsEntry(List.of(4), false);
    assertThat(defaults).containsEntry(List.of(5), "");
    // bytes implicit default is ByteString.EMPTY.
    assertThat(defaults).containsEntry(List.of(6), ByteString.EMPTY);
    // enum implicit default is the first declared value's EnumValueDescriptor.
    assertThat(defaults.get(List.of(7))).isInstanceOf(EnumValueDescriptor.class);
    assertThat(((EnumValueDescriptor) defaults.get(List.of(7))).getName())
        .isEqualTo("RED");
    // optional int32 has presence -> no implicit default recorded.
    assertThat(defaults).doesNotContainKey(List.of(8));
  }

  @Test
  void repeatedAndMessageSkipped() {
    // With two messages declared, Inner is registered first (outer index [0])
    // and Row is the registered root at outer index [1]. So Row's fields are
    // at paths [1, N].
    String protoText =
        "syntax = \"proto3\";\n"
            + "package test;\n"
            + "message Inner { int32 x = 1; }\n"
            + "message Row {\n"
            + "  repeated int32 arr = 1;\n"   // repeated -> emptyList default
            + "  map<string, int32> m = 2;\n" // map      -> emptyMap default
            + "  Inner nested = 3;\n"         // MESSAGE -> SKIP (no scalar default)
            + "  int32 scalar = 4;\n"         // implicit 0
            + "}\n";

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(
        new ProtobufSchema(protoText));

    Map<List<Integer>, Object> defaults = lt.getDefaultValues();
    // Empty-collection defaults (verify the proto3-implicit pass doesn't
    // double-write or override the repeated/map empty defaults):
    assertThat(defaults).containsEntry(List.of(1, 0), java.util.Collections.emptyList());
    assertThat(defaults).containsEntry(List.of(1, 1), java.util.Collections.emptyMap());
    // MESSAGE field -> no entry at the field's own indexPath.
    assertThat(defaults).doesNotContainKey(List.of(1, 2));
    // Scalar implicit default still fires for the proto3 int32.
    assertThat(defaults).containsEntry(List.of(1, 3), 0);
    // Inner.x (proto3 scalar) gets the implicit default too.
    assertThat(defaults).containsEntry(List.of(0), 0);
  }

  @Test
  void proto2WithoutExplicitDefaultStillSkipped() {
    // Proto2 scalars have presence even without [optional], so !hasPresence()
    // filters them out and they get NO implicit default.
    String protoText =
        "syntax = \"proto2\";\n"
            + "package test;\n"
            + "message Row {\n"
            + "  optional int32 a = 1;\n"          // no explicit default
            + "  optional int32 b = 2 [default = 7];\n" // explicit default
            + "}\n";

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(
        new ProtobufSchema(protoText));

    Map<List<Integer>, Object> defaults = lt.getDefaultValues();
    // No implicit default for 'a' — proto2 fields have presence.
    assertThat(defaults).doesNotContainKey(List.of(0));
    // Explicit proto2 default for 'b' still captured.
    assertThat(defaults).containsEntry(List.of(1), 7);
  }
}
