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

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * V1 Avro emission mappings ported from the Flink converters'
 * cc-flink-schema-converters/src/test/.../avro/CommonMappings.java.
 *
 * <p>Each entry pairs an LT input with the exact Avro schema the Flink
 * writer used to emit. LT V1 emission must produce {@code Schema.equals}
 * output for every pair.
 */
public final class AvroMappingsV1 {

  static final String FLINK_PROPERTY_VERSION = "flink.version";
  static final String FLINK_PROPERTY_CURRENT_VERSION = "1";
  static final String FLINK_PRECISION = "flink.precision";
  static final String FLINK_MIN_LENGTH = "flink.minLength";
  static final String FLINK_MAX_LENGTH = "flink.maxLength";
  static final String FLINK_TYPE = "flink.type";
  static final String FLINK_MULTISET_TYPE = "multiset";

  /** A mapping between an LT Schema and the Flink-equivalent Avro Schema. */
  public static final class TypeMapping {
    private final org.apache.avro.Schema avroSchema;
    private final Schema logicalType;

    public TypeMapping(org.apache.avro.Schema avroSchema, Schema logicalType) {
      this.avroSchema = avroSchema;
      this.logicalType = logicalType;
    }

    public org.apache.avro.Schema getAvroSchema() {
      return avroSchema;
    }

    public Schema getLogicalType() {
      return logicalType;
    }

    @Override
    public String toString() {
      return "avroSchema=" + avroSchema + ", logicalType=" + logicalType;
    }
  }

  public static Stream<TypeMapping> get() {
    // Build two independent lists. Schema.withNullable mutates in place, so
    // a single getNotNull() reused across both branches would leak nullable=true
    // back onto the not-null mappings via shared references.
    return Stream.concat(
        getNotNull().stream(),
        getNotNull().stream().map(t -> new TypeMapping(
            nullable(t.getAvroSchema()),
            t.getLogicalType().setNullable(true))));
  }

  public static List<TypeMapping> getNotNull() {
    return Arrays.asList(
        // RECORD with field comment
        new TypeMapping(
            SchemaBuilder.builder()
                .record("io.confluent.row")
                .fields()
                .name("f0")
                .doc("field comment")
                .type()
                .longType()
                .noDefault()
                .endRecord(),
            Schema.createStruct(Arrays.asList(
                new Field("f0",
                    Schema.create(Schema.Type.BIGINT).setNullable(false),
                    0, null, false, "field comment", null, null)))
                .setNullable(false)),
        // DOUBLE
        new TypeMapping(
            SchemaBuilder.builder().doubleType(),
            Schema.create(Schema.Type.DOUBLE).setNullable(false)),
        // BIGINT
        new TypeMapping(
            SchemaBuilder.builder().longType(),
            Schema.create(Schema.Type.BIGINT).setNullable(false)),
        // INT
        new TypeMapping(
            SchemaBuilder.builder().intType(),
            Schema.create(Schema.Type.INT).setNullable(false)),
        // BOOLEAN
        new TypeMapping(
            SchemaBuilder.builder().booleanType(),
            Schema.create(Schema.Type.BOOLEAN).setNullable(false)),
        // FLOAT
        new TypeMapping(
            SchemaBuilder.builder().floatType(),
            Schema.create(Schema.Type.FLOAT).setNullable(false)),
        // VARBINARY (unbounded)
        new TypeMapping(
            SchemaBuilder.builder().bytesType(),
            Schema.createBytes().setNullable(false)),
        // VARBINARY(123)
        new TypeMapping(
            SchemaBuilder.builder()
                .bytesBuilder()
                .prop(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION)
                .prop(FLINK_MAX_LENGTH, 123)
                .endBytes(),
            Schema.createVarbinary(123).setNullable(false)),
        // BINARY(123) — Avro FIXED. Flink's writer uses rowName; for the
        // type-level test the rowName is "io.confluent.row" which Avro splits
        // into name="row", namespace="io.confluent".
        new TypeMapping(
            SchemaBuilder.builder()
                .fixed("row")
                .namespace("io.confluent")
                .size(123),
            Schema.createBinary(123).setNullable(false)),
        // VARCHAR (unbounded)
        new TypeMapping(
            SchemaBuilder.builder().stringType(),
            Schema.createString().setNullable(false)),
        // VARCHAR(123)
        new TypeMapping(
            SchemaBuilder.builder()
                .stringBuilder()
                .prop(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION)
                .prop(FLINK_MAX_LENGTH, 123)
                .endString(),
            Schema.createVarchar(123).setNullable(false)),
        // CHAR(123)
        new TypeMapping(
            SchemaBuilder.builder()
                .stringBuilder()
                .prop(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION)
                .prop(FLINK_MIN_LENGTH, 123)
                .prop(FLINK_MAX_LENGTH, 123)
                .endString(),
            Schema.createChar(123).setNullable(false)),
        // ARRAY<BIGINT>
        new TypeMapping(
            SchemaBuilder.builder().array().items(SchemaBuilder.builder().longType()),
            Schema.createArray(
                Schema.create(Schema.Type.BIGINT).setNullable(false))
                .setNullable(false)),
        // MULTISET<BIGINT> — non-string-keyed encoding (array of MapEntry).
        new TypeMapping(
            SchemaBuilder.array()
                .prop(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION)
                .prop(FLINK_TYPE, FLINK_MULTISET_TYPE)
                .items(connectCustomMapType(
                    SchemaBuilder.builder().longType(),
                    SchemaBuilder.builder().intType())),
            Schema.createMultiset(
                Schema.create(Schema.Type.BIGINT).setNullable(false))
                .setNullable(false)),
        // MULTISET<VARCHAR> — string-keyed encoding (Avro map).
        new TypeMapping(
            SchemaBuilder.map()
                .prop(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION)
                .prop(FLINK_TYPE, FLINK_MULTISET_TYPE)
                .values()
                .intType(),
            Schema.createMultiset(
                Schema.createString().setNullable(false))
                .setNullable(false)),
        // MAP<INT, VARBINARY> — non-string-keyed map.
        new TypeMapping(
            SchemaBuilder.array()
                .items(connectCustomMapType(
                    SchemaBuilder.builder().intType(),
                    SchemaBuilder.builder().bytesType())),
            Schema.createMap(
                Schema.create(Schema.Type.INT).setNullable(false),
                Schema.createBytes().setNullable(false))
                .setNullable(false)),
        // MAP<VARCHAR, BOOLEAN> — string-keyed map.
        new TypeMapping(
            SchemaBuilder.map().values(SchemaBuilder.builder().booleanType()),
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.create(Schema.Type.BOOLEAN).setNullable(false))
                .setNullable(false)),
        // TIME(3) — millis precision, default.
        new TypeMapping(
            LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType()),
            Schema.createTime(3).setNullable(false)),
        // TIME(2) — non-default millis precision.
        new TypeMapping(
            LogicalTypes.timeMillis()
                .addToSchema(SchemaBuilder.builder()
                    .intBuilder()
                    .prop(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION)
                    .prop(FLINK_PRECISION, 2)
                    .endInt()),
            Schema.createTime(2).setNullable(false)),
        // TIMESTAMP_LTZ(3) — millis with TZ.
        new TypeMapping(
            LogicalTypes.timestampMillis()
                .addToSchema(SchemaBuilder.builder().longType()),
            Schema.createTimestampLtz(3).setNullable(false)),
        // TIMESTAMP_LTZ(2)
        new TypeMapping(
            LogicalTypes.timestampMillis()
                .addToSchema(SchemaBuilder.builder()
                    .longBuilder()
                    .prop(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION)
                    .prop(FLINK_PRECISION, 2)
                    .endLong()),
            Schema.createTimestampLtz(2).setNullable(false)),
        // TIMESTAMP_LTZ(5) — micros precision with override.
        new TypeMapping(
            LogicalTypes.timestampMicros()
                .addToSchema(SchemaBuilder.builder()
                    .longBuilder()
                    .prop(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION)
                    .prop(FLINK_PRECISION, 5)
                    .endLong()),
            Schema.createTimestampLtz(5).setNullable(false)),
        // TIMESTAMP(3) — millis without TZ (local timestamp).
        new TypeMapping(
            LogicalTypes.localTimestampMillis()
                .addToSchema(SchemaBuilder.builder().longType()),
            Schema.createTimestamp(3).setNullable(false)),
        // DECIMAL(6,3)
        new TypeMapping(
            LogicalTypes.decimal(6, 3)
                .addToSchema(SchemaBuilder.builder().bytesType()),
            Schema.createDecimal(6, 3).setNullable(false)));
  }

  public static org.apache.avro.Schema nullable(org.apache.avro.Schema schema) {
    return SchemaBuilder.unionOf().nullType().and().type(schema).endUnion();
  }

  /** Canonical Flink-style non-string-keyed map entry (record-of-key-value). */
  public static org.apache.avro.Schema connectCustomMapType(
      org.apache.avro.Schema keyType, org.apache.avro.Schema valueType) {
    return SchemaBuilder.record("MapEntry")
        .namespace("io.confluent.connect.avro")
        .fields()
        .name("key").type(keyType).noDefault()
        .name("value").type(valueType).noDefault()
        .endRecord();
  }

  private AvroMappingsV1() {}
}
