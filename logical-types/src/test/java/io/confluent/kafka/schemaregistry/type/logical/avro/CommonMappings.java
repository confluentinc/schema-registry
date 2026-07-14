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

package io.confluent.kafka.schemaregistry.type.logical.avro;

import io.confluent.avro.type.LogicalMap;
import io.confluent.avro.type.VariantConversion;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/** Common data to use in schema mapping tests. */
public final class CommonMappings {

  /** A mapping between corresponding Avro and logical type schemas. */
  public static class TypeMapping {

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
    return getNotNull().stream()
        .flatMap(
            t -> {
              // Re-convert the nullable Avro schema to get a fresh Schema object
              Schema nullableLogicalType =
                  AvroToLogicalTypeConverter.toRootSchema(
                      new AvroSchema(nullable(t.getAvroSchema())));
              return Stream.of(
                  t,
                  new TypeMapping(nullable(t.getAvroSchema()), nullableLogicalType));
            });
  }

  public static List<TypeMapping> getNotNull() {
    org.apache.avro.Schema anonymousRecord = SchemaBuilder.builder()
        .record("io.confluent.row")
        .fields()
        .name("f0")
        .doc("field comment")
        .type()
        .longType()
        .noDefault()
        .endRecord();
    // Mark as anonymous so the reader recovers it as inline STRUCT (without
    // the marker, an unmarked record is recovered as a NAMED_TYPE_REF).
    anonymousRecord.addProp(CommonConstants.LOGICAL_ANONYMOUS_PROP, "true");
    return Arrays.asList(
        // Record with field comment
        new TypeMapping(
            anonymousRecord,
            Schema.createStruct(Arrays.asList(
                new Field("f0",
                    Schema.create(Schema.Type.BIGINT).setNullable(false),
                    0, null, false, "field comment", null, null)))
                .setNullable(false)),
        // Double
        new TypeMapping(
            SchemaBuilder.builder().doubleType(),
            Schema.create(Schema.Type.DOUBLE).setNullable(false)),
        // Long
        new TypeMapping(
            SchemaBuilder.builder().longType(),
            Schema.create(Schema.Type.BIGINT).setNullable(false)),
        // Int
        new TypeMapping(
            SchemaBuilder.builder().intType(),
            Schema.create(Schema.Type.INT).setNullable(false)),
        // Boolean
        new TypeMapping(
            SchemaBuilder.builder().booleanType(),
            Schema.create(Schema.Type.BOOLEAN).setNullable(false)),
        // Float
        new TypeMapping(
            SchemaBuilder.builder().floatType(),
            Schema.create(Schema.Type.FLOAT).setNullable(false)),
        // Bytes (unbounded)
        new TypeMapping(
            SchemaBuilder.builder().bytesType(),
            Schema.createBytes().setNullable(false)),
        // Bytes (with max length)
        new TypeMapping(
            SchemaBuilder.builder()
                .bytesBuilder()
                .prop(CommonConstants.FLINK_PROPERTY_VERSION,
                    CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
                .prop(CommonConstants.FLINK_MAX_LENGTH, 123)
                .endBytes(),
            Schema.createVarbinary(123).setNullable(false)),
        // Fixed
        new TypeMapping(
            SchemaBuilder.builder()
                .fixed("row")
                .namespace("io.confluent")
                .size(123),
            Schema.createBinary(123).setNullable(false)),
        // String (unbounded)
        new TypeMapping(
            SchemaBuilder.builder().stringType(),
            Schema.createString().setNullable(false)),
        // String (with max length)
        new TypeMapping(
            SchemaBuilder.builder()
                .stringBuilder()
                .prop(CommonConstants.FLINK_PROPERTY_VERSION,
                    CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
                .prop(CommonConstants.FLINK_MAX_LENGTH, 123)
                .endString(),
            Schema.createVarchar(123).setNullable(false)),
        // Char
        new TypeMapping(
            SchemaBuilder.builder()
                .stringBuilder()
                .prop(CommonConstants.FLINK_PROPERTY_VERSION,
                    CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
                .prop(CommonConstants.FLINK_MIN_LENGTH, 123)
                .prop(CommonConstants.FLINK_MAX_LENGTH, 123)
                .endString(),
            Schema.createChar(123).setNullable(false)),
        // Array of Long
        new TypeMapping(
            SchemaBuilder.builder().array().items(SchemaBuilder.builder().longType()),
            Schema.createArray(
                Schema.create(Schema.Type.BIGINT).setNullable(false))
                .setNullable(false)),
        // Multiset (custom map encoding)
        new TypeMapping(
            withMultisetProps(connectCustomMapType(
                SchemaBuilder.builder().longType(),
                SchemaBuilder.builder().intType())),
            Schema.createMultiset(
                Schema.create(Schema.Type.BIGINT).setNullable(false))
                .setNullable(false)),
        // Multiset via Map
        new TypeMapping(
            SchemaBuilder.map()
                .prop(CommonConstants.FLINK_PROPERTY_VERSION,
                    CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
                .prop(CommonConstants.FLINK_TYPE, CommonConstants.FLINK_MULTISET_TYPE)
                .values()
                .intType(),
            Schema.createMultiset(
                Schema.createString().setNullable(false))
                .setNullable(false)),
        // Map with custom entry (non-string key)
        new TypeMapping(
            connectCustomMapType(
                SchemaBuilder.builder().intType(),
                SchemaBuilder.builder().bytesType()),
            Schema.createMap(
                Schema.create(Schema.Type.INT).setNullable(false),
                Schema.createBytes().setNullable(false))
                .setNullable(false)),
        // Map (standard, string key)
        new TypeMapping(
            SchemaBuilder.map().values(SchemaBuilder.builder().booleanType()),
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.create(Schema.Type.BOOLEAN).setNullable(false))
                .setNullable(false)),
        // Time (millis, default precision)
        new TypeMapping(
            LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType()),
            Schema.createTime(3).setNullable(false)),
        // Time (millis, custom precision)
        new TypeMapping(
            LogicalTypes.timeMillis()
                .addToSchema(SchemaBuilder.builder()
                    .intBuilder()
                    .prop(CommonConstants.FLINK_PROPERTY_VERSION,
                        CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
                    .prop(CommonConstants.FLINK_PRECISION, 2)
                    .endInt()),
            Schema.createTime(2).setNullable(false)),
        // Timestamp (millis, local time zone)
        new TypeMapping(
            LogicalTypes.timestampMillis()
                .addToSchema(SchemaBuilder.builder().longType()),
            Schema.createTimestampLtz(3).setNullable(false)),
        // Timestamp (millis, local time zone, custom precision)
        new TypeMapping(
            LogicalTypes.timestampMillis()
                .addToSchema(SchemaBuilder.builder()
                    .longBuilder()
                    .prop(CommonConstants.FLINK_PROPERTY_VERSION,
                        CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
                    .prop(CommonConstants.FLINK_PRECISION, 2)
                    .endLong()),
            Schema.createTimestampLtz(2).setNullable(false)),
        // Timestamp (micros, local time zone, custom precision)
        new TypeMapping(
            LogicalTypes.timestampMicros()
                .addToSchema(SchemaBuilder.builder()
                    .longBuilder()
                    .prop(CommonConstants.FLINK_PROPERTY_VERSION,
                        CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
                    .prop(CommonConstants.FLINK_PRECISION, 5)
                    .endLong()),
            Schema.createTimestampLtz(5).setNullable(false)),
        // Timestamp (millis, without time zone)
        new TypeMapping(
            LogicalTypes.localTimestampMillis()
                .addToSchema(SchemaBuilder.builder().longType()),
            Schema.createTimestamp(3).setNullable(false)),
        // Decimal
        new TypeMapping(
            LogicalTypes.decimal(6, 3).addToSchema(SchemaBuilder.builder().bytesType()),
            Schema.createDecimal(6, 3).setNullable(false)),
        // Variant
        new TypeMapping(
            new VariantConversion().getRecommendedSchema(),
            Schema.create(Schema.Type.VARIANT).setNullable(false)));
  }

  public static org.apache.avro.Schema nullable(org.apache.avro.Schema schema) {
    return SchemaBuilder.unionOf().nullType().and().type(schema).endUnion();
  }

  /**
   * Build the array-of-key-value form used to encode non-string-keyed
   * MAP/MULTISET. Returns the array (not just the element record), so
   * callers should NOT wrap it in another array. The array carries the
   * {@code logicalType=map} marker via {@link LogicalMap}.
   */
  public static org.apache.avro.Schema connectCustomMapType(
      org.apache.avro.Schema keyType, org.apache.avro.Schema valueType) {
    return LogicalMap.createMap("MapEntry", keyType, valueType);
  }

  /** Add the multiset marker props to an array schema. */
  public static org.apache.avro.Schema withMultisetProps(org.apache.avro.Schema arraySchema) {
    arraySchema.addProp(CommonConstants.FLINK_PROPERTY_VERSION,
        CommonConstants.FLINK_PROPERTY_CURRENT_VERSION);
    arraySchema.addProp(CommonConstants.FLINK_TYPE, CommonConstants.FLINK_MULTISET_TYPE);
    return arraySchema;
  }

  private CommonMappings() {}
}
