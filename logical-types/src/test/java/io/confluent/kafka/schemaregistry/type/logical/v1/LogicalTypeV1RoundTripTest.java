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

package io.confluent.kafka.schemaregistry.type.logical.v1;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.avro.v1.AvroMappingsV1;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.v1.JsonMappingsV1;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.v1.ProtoMappingsV1;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip stability for V1 emission: LT input → V1 emit → LT read → assert
 * the resulting Schema matches the original.
 *
 * <p>This test pins the "swap-in" guarantee: anything callable into V1
 * emission can be parsed back by LT's reader to a structurally identical LT
 * Schema. If V1 emission or LT reader drifts, this catches it.
 */
class LogicalTypeV1RoundTripTest {

  static Stream<Arguments> avroMappings() {
    // Skip the inline RECORD case — Avro records are always named, so an
    // inline STRUCT round-trips as a NAMED_TYPE_REF (with the record name as
    // the qualified name). That's a documented Avro semantics boundary, not
    // a round-trip regression.
    return AvroMappingsV1.get()
        .filter(t -> t.getLogicalType().getType() != Schema.Type.STRUCT)
        .map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("avroMappings")
  void avroRoundTrip(AvroMappingsV1.TypeMapping mapping) {
    // Snapshot the input DDL BEFORE the converter runs — Schema.withNullable
    // is mutating, and the converter calls it on shared schema references
    // when building wrapper structs. Without the snapshot the assertion
    // compares the post-mutation state to itself.
    String inputDdl = mapping.getLogicalType().toDdl();
    AvroSchema emitted = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(mapping.getLogicalType()), "io.confluent.row",
        LogicalTypeVersion.V1);
    Schema readBack = AvroToLogicalTypeConverter.toRootSchema(emitted);
    assertThat(readBack.toDdl()).isEqualTo(inputDdl);
  }

  static Stream<Arguments> protoMappings() {
    return ProtoMappingsV1.get().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("protoMappings")
  void protoRoundTrip(ProtoMappingsV1.TypeMapping mapping) {
    String inputDdl = mapping.getRootSchema().toDdl();
    ProtobufSchema emitted = LogicalTypeToProtoConverter.fromLogicalType(
        mapping.toLogicalType(), "Row", LogicalTypeVersion.V1);
    Schema readBack = ProtoToLogicalTypeConverter.toRootSchema(emitted);
    assertThat(readBack.toDdl()).isEqualTo(inputDdl);
  }

  static Stream<Arguments> jsonMappings() {
    return JsonMappingsV1.get().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("jsonMappings")
  void jsonRoundTrip(JsonMappingsV1.TypeMapping mapping) {
    String inputDdl = mapping.getRootSchema().toDdl();
    JsonSchema emitted = LogicalTypeToJsonConverter.fromLogicalType(
        mapping.toLogicalType(), "io.confluent.row", LogicalTypeVersion.V1);
    Schema readBack = JsonToLogicalTypeConverter.toRootSchema(emitted);
    assertThat(readBack.toDdl()).isEqualTo(inputDdl);
  }
}
