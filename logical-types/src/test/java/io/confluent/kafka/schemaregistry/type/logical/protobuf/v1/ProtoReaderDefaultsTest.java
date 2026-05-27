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

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that LT's Proto reader extracts proto2 native default values into
 * the path-keyed {@link LogicalType#getDefaultValues()} map. Ported from
 * Flink's {@code ProtoToFlinkSchemaConverterTest#testDefaults}.
 *
 * <p>The fixture {@code schema/proto/defaults.proto} is proto2 syntax with
 * {@code [default = N]} on every primitive field, plus a nullable map and
 * nested records.
 */
class ProtoReaderDefaultsTest {

  @Test
  void testDefaults() {
    String protoText = readResource("schema/proto/defaults.proto");
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(
        new ProtobufSchema(protoText));

    // Path semantics: paths reflect the structure of the schema as exposed to
    // consumers. Wrapper layers (the synthesized RepeatedWrapper / OneofWrapper
    // structs that "flink.wrapped" fields are unwrapped from) do NOT add an
    // index — the wrapper is invisible in the returned schema, so its
    // pseudo-field index would be unreachable. ARRAY contributes no index
    // either; descending into the element type uses the parent path directly.
    //
    // Empty-collection defaults: per proto spec, an absent map is empty map
    // and an absent repeated is empty list — recorded as implicit defaults at
    // the field's own indexPath.
    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0), 1),
        Map.entry(List.of(1), 2),
        Map.entry(List.of(2), 3),
        Map.entry(List.of(3), 4),
        Map.entry(List.of(4), 5),
        Map.entry(List.of(5), 6),
        Map.entry(List.of(6), 7L),
        Map.entry(List.of(7), 8L),
        Map.entry(List.of(8), 9.0),
        Map.entry(List.of(9), 10.0),
        Map.entry(List.of(10), 11.0f),
        Map.entry(List.of(11), 12.0f),
        // nullableMap (wrapped MAP) → empty-map default + key/value
        Map.entry(List.of(12), Collections.emptyMap()),
        Map.entry(List.of(12, 0), 15L),
        Map.entry(List.of(12, 1), 16L),
        // nested → value (record) → a/b
        Map.entry(List.of(13, 0, 0), 17L),
        Map.entry(List.of(13, 0, 1), 18L),
        // arr (repeated record) → empty-list default at the array, then
        // element doesn't extend path → value (record field at pos 0) → a/b
        Map.entry(List.of(14), Collections.emptyList()),
        Map.entry(List.of(14, 0, 0), 17L),
        Map.entry(List.of(14, 0, 1), 18L));
  }

  private static String readResource(String path) {
    try (InputStream in = ProtoReaderDefaultsTest.class
        .getClassLoader().getResourceAsStream(path)) {
      if (in == null) {
        throw new IllegalStateException("Missing resource: " + path);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
