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

import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that LT V1 Proto emission produces the exact proto schema text the
 * Flink writer used to emit, for every Flink test fixture (ported from
 * {@code FlinkToProtoSchemaConverterTest}).
 *
 * <p>Comparison: parse both expected and actual into {@code ProtobufSchema}
 * and compare canonical text — same approach Flink's writer test uses.
 */
class LogicalTypeToProtoV1EmissionTest {

  static Stream<Arguments> typesToCheck() {
    return ProtoMappingsV1.get().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("typesToCheck")
  void testV1Emission(ProtoMappingsV1.TypeMapping mapping) {
    ProtobufSchema actual = LogicalTypeToProtoConverter.fromLogicalType(
        mapping.toLogicalType(), "Row", LogicalTypeVersion.V1);
    // V1 emits in raw (un-normalized) form to match Flink writer output.
    // The expected fixture is also raw text — compare both in unresolved
    // simple-name form.
    assertThat(canonicalize(actual.toString()))
        .isEqualTo(canonicalize(
            new ProtobufSchema(mapping.getExpectedProto()).toString()));
  }

  /** Strip whitespace-only line variation, matching Flink's test compare. */
  private static String canonicalize(String s) {
    return s.trim().replaceAll("\\s*\n", "\n");
  }
}
