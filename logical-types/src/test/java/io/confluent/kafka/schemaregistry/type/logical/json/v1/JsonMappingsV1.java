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

package io.confluent.kafka.schemaregistry.type.logical.json.v1;

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * V1 JSON Schema emission mappings ported from the Flink converters'
 * cc-flink-schema-converters/src/test/.../json/CommonMappings.java.
 *
 * <p>Each mapping pairs an LT input with the expected JSON Schema text the
 * V1 emission must produce. The expected text is captured from running V1
 * once and is the canonical V1 form (Flink-compat, insertion-order keys,
 * no $schema).
 *
 * <p>Equality is checked via {@code JsonNode} tree comparison so prop order
 * outside of arrays doesn't affect the result.
 */
public final class JsonMappingsV1 {

  /** A mapping between an LT root schema and its expected JSON Schema text. */
  public static final class TypeMapping {
    private final String name;
    private final Schema rootSchema;
    private final String expectedJson;

    public TypeMapping(String name, Schema rootSchema, String expectedJson) {
      this.name = name;
      this.rootSchema = rootSchema;
      this.expectedJson = expectedJson;
    }

    public String getName() {
      return name;
    }

    public Schema getRootSchema() {
      return rootSchema;
    }

    public String getExpectedJson() {
      return expectedJson;
    }

    public LogicalType toLogicalType() {
      return new LogicalType(rootSchema);
    }

    @Override
    public String toString() {
      return name;
    }
  }

  // Helpers ---------------------------------------------------------------

  private static Schema notNull(Schema s) {
    return s.setNullable(false);
  }

  private static Schema nullable(Schema s) {
    return s.setNullable(true);
  }

  private static Field f(int pos, String name, Schema schema) {
    return new Field(name, schema, pos, null, false, null, null, null);
  }

  private static Field f(int pos, String name, Schema schema, String doc) {
    return new Field(name, schema, pos, null, false, doc, null, null);
  }

  private static Schema struct(Field... fields) {
    return Schema.createStruct(Arrays.asList(fields)).setNullable(false);
  }

  // Cases -----------------------------------------------------------------

  public static List<TypeMapping> getNotNull() {
    return Arrays.asList(
        new TypeMapping("DOUBLE",
            notNull(Schema.create(Schema.Type.DOUBLE)),
            "{\"type\":\"number\",\"connect.type\":\"float64\"}"),
        new TypeMapping("BIGINT",
            notNull(Schema.create(Schema.Type.BIGINT)),
            "{\"type\":\"number\",\"connect.type\":\"int64\"}"),
        new TypeMapping("INT",
            notNull(Schema.create(Schema.Type.INT)),
            "{\"type\":\"number\",\"connect.type\":\"int32\"}"),
        new TypeMapping("BOOLEAN",
            notNull(Schema.create(Schema.Type.BOOLEAN)),
            "{\"type\":\"boolean\"}"),
        new TypeMapping("FLOAT",
            notNull(Schema.create(Schema.Type.FLOAT)),
            "{\"type\":\"number\",\"connect.type\":\"float32\"}"),
        new TypeMapping("VARBINARY_UNBOUNDED",
            notNull(Schema.createBytes()),
            "{\"type\":\"string\",\"connect.type\":\"bytes\"}"),
        new TypeMapping("VARCHAR_123",
            notNull(Schema.createVarchar(123)),
            "{\"type\":\"string\",\"maxLength\":123}"),
        new TypeMapping("VARBINARY_123",
            notNull(Schema.createVarbinary(123)),
            "{\"type\":\"string\",\"flink.maxLength\":123,\"flink.version\":\"1\","
                + "\"connect.type\":\"bytes\"}"),
        new TypeMapping("CHAR_123",
            notNull(Schema.createChar(123)),
            "{\"type\":\"string\",\"minLength\":123,\"maxLength\":123}"),
        new TypeMapping("BINARY_123",
            notNull(Schema.createBinary(123)),
            "{\"type\":\"string\",\"flink.minLength\":123,\"flink.maxLength\":123,"
                + "\"flink.version\":\"1\",\"connect.type\":\"bytes\"}"),
        new TypeMapping("TIME_3",
            notNull(Schema.createTime(3)),
            "{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Time\","
                + "\"connect.type\":\"int32\"}"),
        new TypeMapping("TIME_2",
            notNull(Schema.createTime(2)),
            "{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Time\","
                + "\"connect.type\":\"int32\",\"flink.version\":\"1\","
                + "\"flink.precision\":2}"),
        new TypeMapping("TIMESTAMP_LTZ_3",
            notNull(Schema.createTimestampLtz(3)),
            "{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Timestamp\","
                + "\"connect.type\":\"int64\"}"),
        new TypeMapping("TIMESTAMP_LTZ_2",
            notNull(Schema.createTimestampLtz(2)),
            "{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Timestamp\","
                + "\"connect.type\":\"int64\",\"flink.version\":\"1\","
                + "\"flink.precision\":2}"),
        new TypeMapping("TIMESTAMP_3",
            notNull(Schema.createTimestamp(3)),
            "{\"type\":\"number\",\"connect.type\":\"int64\","
                + "\"flink.version\":\"1\",\"flink.type\":\"timestamp\"}"),
        new TypeMapping("TIMESTAMP_2",
            notNull(Schema.createTimestamp(2)),
            "{\"type\":\"number\",\"connect.type\":\"int64\","
                + "\"flink.version\":\"1\",\"flink.precision\":2,"
                + "\"flink.type\":\"timestamp\"}"),
        // ROW with mixed nullability.
        new TypeMapping("ROW_int8_string",
            struct(
                f(0, "int8",
                    Schema.create(Schema.Type.TINYINT).setNullable(true)),
                f(1, "string",
                    Schema.createString().setNullable(false), "string field")),
            "{\"type\":\"object\",\"properties\":{"
                + "\"int8\":{\"oneOf\":[{\"type\":\"null\"},"
                + "{\"type\":\"number\",\"connect.type\":\"int8\"}],"
                + "\"connect.index\":0},"
                + "\"string\":{\"type\":\"string\","
                + "\"connect.index\":1,\"description\":\"string field\"}},"
                + "\"required\":[\"string\"],\"additionalProperties\":false,"
                + "\"title\":\"io.confluent.row\"}"),
        // ROW with a nullable DECIMAL — exercises the "decimalSchema" SQL-1593 path.
        new TypeMapping("ROW_decimal",
            struct(
                f(0, "decimal",
                    Schema.createDecimal(10, 2).setNullable(true))),
            "{\"type\":\"object\",\"properties\":{"
                + "\"decimal\":{\"oneOf\":[{\"type\":\"null\"},"
                + "{\"type\":\"number\","
                + "\"title\":\"org.apache.kafka.connect.data.Decimal\","
                + "\"connect.type\":\"bytes\","
                + "\"connect.parameters\":{"
                + "\"connect.decimal.precision\":\"10\",\"scale\":\"2\"}}],"
                + "\"connect.index\":0}},"
                + "\"additionalProperties\":false,"
                + "\"title\":\"io.confluent.row\"}"));
  }

  public static List<TypeMapping> getCollections() {
    return Arrays.asList(
        // MULTISET<BIGINT> non-string-key — array-of-{key,value}.
        new TypeMapping("MULTISET_BIGINT",
            notNull(Schema.createMultiset(
                notNull(Schema.create(Schema.Type.BIGINT)))),
            "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{"
                + "\"key\":{\"type\":\"number\",\"connect.type\":\"int64\"},"
                + "\"value\":{\"type\":\"number\",\"connect.type\":\"int32\"}},"
                + "\"additionalProperties\":false},"
                + "\"connect.type\":\"map\","
                + "\"flink.version\":\"1\",\"flink.type\":\"multiset\"}"),
        // MULTISET<VARCHAR> string-key — additionalProperties on object.
        new TypeMapping("MULTISET_VARCHAR",
            notNull(Schema.createMultiset(
                notNull(Schema.createString()))),
            "{\"type\":\"object\","
                + "\"additionalProperties\":{\"type\":\"number\","
                + "\"connect.type\":\"int32\"},"
                + "\"connect.type\":\"map\","
                + "\"flink.version\":\"1\",\"flink.type\":\"multiset\"}"),
        // MAP<INT, BIGINT> non-string-key.
        new TypeMapping("MAP_INT_BIGINT",
            notNull(Schema.createMap(
                notNull(Schema.create(Schema.Type.INT)),
                notNull(Schema.create(Schema.Type.BIGINT)))),
            "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{"
                + "\"key\":{\"type\":\"number\",\"connect.type\":\"int32\"},"
                + "\"value\":{\"type\":\"number\",\"connect.type\":\"int64\"}},"
                + "\"additionalProperties\":false},"
                + "\"connect.type\":\"map\"}"),
        // MAP<VARCHAR, BIGINT> string-key.
        new TypeMapping("MAP_VARCHAR_BIGINT",
            notNull(Schema.createMap(
                notNull(Schema.createString()),
                notNull(Schema.create(Schema.Type.BIGINT)))),
            "{\"type\":\"object\","
                + "\"additionalProperties\":{\"type\":\"number\","
                + "\"connect.type\":\"int64\"},"
                + "\"connect.type\":\"map\"}"));
  }

  public static Stream<TypeMapping> get() {
    return Stream.concat(getNotNull().stream(), getCollections().stream());
  }

  private JsonMappingsV1() {}
}
