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

package io.confluent.kafka.schemaregistry.type.logical.policy;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.policy.Invalidity.Rule;
import io.confluent.kafka.schemaregistry.type.logical.policy.LogicalTypeChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Validity cases driven from real schema text rather than a hand-built SRLT.
 *
 * <p>{@link ValidityCheckerTest} builds its input directly, which makes it precise about the rules
 * but blind to whether a hazard survives the conversion to reach them at all. Case collision is the
 * clearest example: it arrives from a customer's Avro or JSON Schema, converts without complaint,
 * and is only caught afterwards — so a rule that fires on a hand-built struct proves nothing about
 * the shape that was actually reported.
 */
class ValidityCheckerEndToEndTest {

  @Test
  void anAvroCaseCollisionSurvivesConversionAndIsCaughtForIceberg() {
    LogicalType type = fromAvro("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"userId\",\"type\":\"int\"},"
        + "{\"name\":\"UserId\",\"type\":\"int\"}]}");

    assertThat(rulesOf(Mode.ICEBERG_V2, type)).containsExactly(Rule.FIELD_NAME_CASE_COLLISION);
    assertThat(rulesOf(Mode.ICEBERG_V3, type)).containsExactly(Rule.FIELD_NAME_CASE_COLLISION);
    assertThat(ValidityChecker.validate(Mode.FLINK, type).isValid()).isTrue();
  }

  @Test
  void aJsonCaseCollisionSurvivesConversionAndIsCaughtForIceberg() {
    LogicalType type = fromJson("{\"type\":\"object\",\"properties\":{"
        + "\"name\":{\"type\":\"string\"},"
        + "\"Name\":{\"type\":\"string\"}}}");

    assertThat(rulesOf(Mode.ICEBERG_V2, type)).containsExactly(Rule.FIELD_NAME_CASE_COLLISION);
    assertThat(ValidityChecker.validate(Mode.FLINK, type).isValid()).isTrue();
  }

  @Test
  void aProtoCaseCollisionSurvivesConversionAndIsCaughtForIceberg() {
    LogicalType type = fromProto(
        "syntax = \"proto3\"; message M { int32 order_id = 1; int32 ORDER_ID = 2; }");

    assertThat(rulesOf(Mode.ICEBERG_V2, type)).containsExactly(Rule.FIELD_NAME_CASE_COLLISION);
    assertThat(ValidityChecker.validate(Mode.FLINK, type).isValid()).isTrue();
  }

  @Test
  void aNestedAvroCaseCollisionIsFoundAtItsPath() {
    LogicalType type = fromAvro("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"payload\",\"type\":{\"type\":\"record\",\"name\":\"P\",\"fields\":["
        + "{\"name\":\"ts\",\"type\":\"long\"},"
        + "{\"name\":\"TS\",\"type\":\"long\"}]}}]}");

    assertThat(pathsOf(Mode.ICEBERG_V2, type)).containsExactly("payload");
  }

  @Test
  void differentlyNamedFieldsSurviveConversionCleanly() {
    LogicalType type = fromAvro("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"userId\",\"type\":\"int\"},"
        + "{\"name\":\"userName\",\"type\":\"string\"}]}");

    for (Mode mode : Mode.values()) {
      assertThat(ValidityChecker.validate(mode, type).isValid()).isTrue();
    }
  }

  private static List<Rule> rulesOf(Mode mode, LogicalType type) {
    return ValidityChecker.validate(mode, type).getInvalidities().stream()
        .map(Invalidity::getRule)
        .collect(Collectors.toList());
  }

  private static List<String> pathsOf(Mode mode, LogicalType type) {
    return ValidityChecker.validate(mode, type).getInvalidities().stream()
        .map(Invalidity::getPath)
        .collect(Collectors.toList());
  }

  private static LogicalType fromAvro(String avroText) {
    return AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(avroText));
  }

  private static LogicalType fromJson(String jsonText) {
    return JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(jsonText));
  }

  private static LogicalType fromProto(String protoText) {
    return ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(protoText));
  }
}
