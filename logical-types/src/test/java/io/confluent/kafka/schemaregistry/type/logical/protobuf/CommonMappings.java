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

package io.confluent.kafka.schemaregistry.type.logical.protobuf;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;

import java.util.Arrays;
import java.util.List;

/**
 * Type-mapping matrix for Proto. Each entry is a single-field struct paired
 * with the field's expected logical Schema after round-trip. Drives both
 * directions (LT -> Proto -> LT round-trip equality) so any new primitive type
 * automatically lights up a missing-coverage test until added here.
 *
 * <p>Proto wraps every type in a message — there is no "root primitive". So
 * each mapping is expressed as a single-field struct with the type under test
 * as its only field.
 */
public final class CommonMappings {

  /** A mapping between a logical-type field schema and its expected round-trip. */
  public static class TypeMapping {

    private final String name;
    private final Schema fieldSchema;

    public TypeMapping(String name, Schema fieldSchema) {
      this.name = name;
      this.fieldSchema = fieldSchema;
    }

    public String getName() {
      return name;
    }

    public Schema getFieldSchema() {
      return fieldSchema;
    }

    /** Builds a single-field root struct for round-tripping through the converter. */
    public Schema asRootStruct() {
      return Schema.createStruct(Arrays.asList(
          new Field("f", fieldSchema, 0)))
          .setNullable(false);
    }

    @Override
    public String toString() {
      return name + " -> " + fieldSchema;
    }
  }

  public static List<TypeMapping> get() {
    return Arrays.asList(
        new TypeMapping("BOOLEAN",
            Schema.create(Schema.Type.BOOLEAN).setNullable(false)),
        new TypeMapping("TINYINT",
            Schema.create(Schema.Type.TINYINT).setNullable(false)),
        new TypeMapping("SMALLINT",
            Schema.create(Schema.Type.SMALLINT).setNullable(false)),
        new TypeMapping("INT",
            Schema.create(Schema.Type.INT).setNullable(false)),
        new TypeMapping("BIGINT",
            Schema.create(Schema.Type.BIGINT).setNullable(false)),
        new TypeMapping("FLOAT",
            Schema.create(Schema.Type.FLOAT).setNullable(false)),
        new TypeMapping("DOUBLE",
            Schema.create(Schema.Type.DOUBLE).setNullable(false)),
        new TypeMapping("DECIMAL(10,2)",
            Schema.createDecimal(10, 2).setNullable(false)),
        new TypeMapping("DECIMAL(15,5)",
            Schema.createDecimal(15, 5).setNullable(false)),
        new TypeMapping("STRING",
            Schema.createString().setNullable(false)),
        new TypeMapping("VARCHAR(50)",
            Schema.createVarchar(50).setNullable(false)),
        new TypeMapping("CHAR(10)",
            Schema.createChar(10).setNullable(false)),
        new TypeMapping("BYTES",
            Schema.createBytes().setNullable(false)),
        new TypeMapping("VARBINARY(50)",
            Schema.createVarbinary(50).setNullable(false)),
        new TypeMapping("BINARY(10)",
            Schema.createBinary(10).setNullable(false)),
        new TypeMapping("DATE",
            Schema.create(Schema.Type.DATE).setNullable(false)),
        new TypeMapping("TIME(3)",
            Schema.createTime(3).setNullable(false)),
        new TypeMapping("TIME(6)",
            Schema.createTime(6).setNullable(false)),
        new TypeMapping("TIMESTAMP(3)",
            Schema.createTimestamp(3).setNullable(false)),
        new TypeMapping("TIMESTAMP(6)",
            Schema.createTimestamp(6).setNullable(false)),
        new TypeMapping("TIMESTAMP(9)",
            Schema.createTimestamp(9).setNullable(false)),
        new TypeMapping("TIMESTAMP_LTZ(3)",
            Schema.createTimestampLtz(3).setNullable(false)),
        new TypeMapping("TIMESTAMP_LTZ(6)",
            Schema.createTimestampLtz(6).setNullable(false)),
        new TypeMapping("TIMESTAMP_LTZ(9)",
            Schema.createTimestampLtz(9).setNullable(false)));
  }

  private CommonMappings() {}
}
