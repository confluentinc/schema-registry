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

package io.confluent.kafka.schemaregistry.type.logical.avro.v1;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that LT V1 Avro emission produces a {@code Schema.equals}-equivalent
 * Avro schema for every Flink test fixture (ported from
 * {@code FlinkToAvroSchemaConverterTest}).
 *
 * <p>{@code Schema.equals} ignores props, matching how Flink's writer test
 * compared output. Wire-format-level prop ordering is not load-bearing for
 * the Flink reader.
 */
class LogicalTypeToAvroV1EmissionTest {

  static Stream<Arguments> typesToCheck() {
    return AvroMappingsV1.get().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("typesToCheck")
  void testV1Emission(AvroMappingsV1.TypeMapping mapping) {
    AvroSchema actual = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(mapping.getLogicalType()), "io.confluent.row",
        LogicalTypeVersion.V1);
    assertThat(actual.rawSchema()).isEqualTo(mapping.getAvroSchema());
  }
}
