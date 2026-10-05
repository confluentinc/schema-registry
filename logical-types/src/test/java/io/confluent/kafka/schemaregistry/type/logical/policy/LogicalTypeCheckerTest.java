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

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.type.logical.policy.LogicalTypeChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Tests for the public entry point.
 *
 * <p>The rules themselves are covered by {@code CompatibilityCheckerTest} and
 * {@code ValidityCheckerTest}, which reach the implementations directly. What is left to pin here is
 * the contract callers actually see: that both operations are reachable from one class under one
 * {@link Mode}, and that they answer independently — which is the whole reason a caller has to run
 * both.
 */
class LogicalTypeCheckerTest {

  @Test
  void compareReachesTheCompatibilityRules() {
    LogicalType one = struct(required("a", type(Schema.Type.INT)));
    LogicalType two = struct(
        required("a", type(Schema.Type.INT)),
        new Field("b", nonNull(type(Schema.Type.INT)), 1));

    assertThat(LogicalTypeChecker.compare(Mode.FLINK, one, two).getIncompatibilities())
        .singleElement()
        .satisfies(i ->
            assertThat(i.getRule()).isEqualTo(Incompatibility.Rule.REQUIRED_FIELD_ADDED));
  }

  @Test
  void validateReachesTheValidityRules() {
    assertThat(LogicalTypeChecker
        .validate(Mode.FLINK, struct(required("d", Schema.createDecimal(40, 2))))
        .getInvalidities())
        .singleElement()
        .satisfies(i -> assertThat(i.getRule()).isEqualTo(Invalidity.Rule.PRECISION_OUT_OF_RANGE));
  }

  @Test
  void anUnchangedButUnusableSchemaPassesCompareAndFailsValidate() {
    // The gap that makes both calls necessary. Nothing changed, so there is no unsafe change to
    // report; the schema is unusable regardless, which only validate can see.
    LogicalType bad = struct(required("d", Schema.createDecimal(40, 2)));

    assertThat(LogicalTypeChecker.compare(Mode.FLINK, bad, bad).isCompatible()).isTrue();
    assertThat(LogicalTypeChecker.validate(Mode.FLINK, bad).isValid()).isFalse();
  }

  @Test
  void aSafeChangeBetweenTwoUsableSchemasPassesBoth() {
    LogicalType before = struct(required("id", type(Schema.Type.INT)));
    LogicalType after = struct(required("id", type(Schema.Type.BIGINT)));

    for (Mode mode : Mode.values()) {
      assertThat(LogicalTypeChecker.validate(mode, after).isValid()).isTrue();
      assertThat(LogicalTypeChecker.compare(mode, before, after).isCompatible()).isTrue();
    }
  }

  @Test
  void theSameModeDrivesBothOperations() {
    // TIMESTAMP(9) is representable for Flink and Iceberg v3 but not v2, and only validate says so.
    LogicalType nanos = struct(required("ts", Schema.createTimestamp(9)));

    assertThat(LogicalTypeChecker.validate(Mode.ICEBERG_V2, nanos).isValid()).isFalse();
    assertThat(LogicalTypeChecker.validate(Mode.ICEBERG_V3, nanos).isValid()).isTrue();
    assertThat(LogicalTypeChecker.validate(Mode.FLINK, nanos).isValid()).isTrue();
  }

  @Test
  void everyModeIsAcceptedByBothOperations() {
    LogicalType type = struct(required("id", type(Schema.Type.INT)));
    for (Mode mode : Mode.values()) {
      assertThat(LogicalTypeChecker.validate(mode, type)).isNotNull();
      assertThat(LogicalTypeChecker.compare(mode, type, type)).isNotNull();
    }
  }

  private static Schema nonNull(Schema schema) {
    return schema.setNullable(false);
  }

  private static Field required(String name, Schema type) {
    return new Field(name, nonNull(type), 0);
  }

  private static LogicalType struct(Field... fields) {
    return new LogicalType(nonNull(Schema.createStruct(Arrays.asList(fields))));
  }

  private static Schema type(Schema.Type t) {
    return Schema.create(t);
  }
}
