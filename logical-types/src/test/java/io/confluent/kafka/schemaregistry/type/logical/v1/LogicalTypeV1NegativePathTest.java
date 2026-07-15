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

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that LT V1 emission rejects every input shape Flink's writer would
 * not produce. Each test asserts the same exception class as Flink's writer
 * for the same triggering input. Exception text is intentionally not asserted
 * — LT's wording differs from Flink's by design (LT messages reference the
 * V1 mode rather than naming Flink directly).
 */
class LogicalTypeV1NegativePathTest {

  // ---------- Avro ----------

  @Test
  void avroRejectsTimePrecisionGreaterThan3() {
    Schema s = Schema.createTime(4).setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToAvroConverter
            .fromLogicalType(new LogicalType(s), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void avroRejectsTimestampPrecisionGreaterThan6() {
    Schema s = Schema.createTimestamp(7).setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToAvroConverter
            .fromLogicalType(new LogicalType(s), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void avroRejectsTimestampLtzPrecisionGreaterThan6() {
    Schema s = Schema.createTimestampLtz(7).setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToAvroConverter
            .fromLogicalType(new LogicalType(s), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void avroRejectsUnion() {
    Schema u = Schema.createUnion(Arrays.asList(
            new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
            new UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToAvroConverter
            .fromLogicalType(new LogicalType(u), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void avroRejectsVariant() {
    Schema v = Schema.create(Schema.Type.VARIANT).setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToAvroConverter
            .fromLogicalType(new LogicalType(v), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  // ---------- Proto ----------

  @Test
  void protoRejectsUnion() {
    Schema u = Schema.createUnion(Arrays.asList(
            new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
            new UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema struct = Schema.createStruct(Collections.singletonList(
        new Schema.Field("u", u, 0))).setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToProtoConverter
            .fromLogicalType(new LogicalType(struct), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void protoRejectsVariant() {
    Schema struct = Schema.createStruct(Collections.singletonList(
        new Schema.Field("v",
            Schema.create(Schema.Type.VARIANT).setNullable(false), 0)))
        .setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToProtoConverter
            .fromLogicalType(new LogicalType(struct), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  // ---------- JSON ----------

  @Test
  void jsonRejectsUnion() {
    Schema u = Schema.createUnion(Arrays.asList(
            new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
            new UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToJsonConverter
            .fromLogicalType(new LogicalType(u), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void jsonRejectsVariant() {
    Schema v = Schema.create(Schema.Type.VARIANT).setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToJsonConverter
            .fromLogicalType(new LogicalType(v), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void jsonRejectsEnum() {
    Schema e = Schema.createEnum(Arrays.asList(
        new EnumValue("RED"), new EnumValue("GREEN"))).setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToJsonConverter
            .fromLogicalType(new LogicalType(e), "Color", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void jsonRejectsTimePrecisionGreaterThan3() {
    Schema s = Schema.createTime(4).setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToJsonConverter
            .fromLogicalType(new LogicalType(s), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void jsonRejectsTimestampPrecisionGreaterThan3() {
    Schema s = Schema.createTimestamp(4).setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToJsonConverter
            .fromLogicalType(new LogicalType(s), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void jsonRejectsTimestampLtzPrecisionGreaterThan3() {
    Schema s = Schema.createTimestampLtz(4).setNullable(false);
    assertThatThrownBy(() -> LogicalTypeToJsonConverter
            .fromLogicalType(new LogicalType(s), "Row", LogicalTypeVersion.V1))
        .isInstanceOf(ValidationException.class);
  }
}
