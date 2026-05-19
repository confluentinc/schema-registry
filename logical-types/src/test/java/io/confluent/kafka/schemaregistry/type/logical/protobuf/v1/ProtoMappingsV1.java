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

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * V1 Proto emission mappings ported from the Flink converters'
 * cc-flink-schema-converters/src/test/.../protobuf/CommonMappings.java.
 *
 * <p>Each mapping pairs an LT input with the exact proto schema text the
 * Flink writer used to emit. LT V1 emission must produce a normalized
 * proto string equal to the expected fixture.
 *
 * <p>Skipped from the original Flink set: {@code getDefaultsCase} (proto2
 * with defaults — V1 does not emit defaults; reachable via reader-only
 * tests), {@code getOneOfCase} (oneof — V1 throws on UNION; covered by
 * {@code LogicalTypeV1NegativePathTest}), and {@code PURCHASE}/{@code
 * PAGEVIEW} (subject-name tests, not emission contract).
 */
public final class ProtoMappingsV1 {

  public static final String PACKAGE_NAME = "io.confluent.protobuf.generated";

  /** A mapping between an LT root schema and its expected proto text. */
  public static final class TypeMapping {
    private final String name;
    private final String expectedProto;
    private final Schema rootSchema;
    private final String packageName;

    public TypeMapping(String name, String expectedProto, Schema rootSchema) {
      this(name, expectedProto, rootSchema, PACKAGE_NAME);
    }

    public TypeMapping(String name, String expectedProto, Schema rootSchema,
                       String packageName) {
      this.name = name;
      this.expectedProto = expectedProto;
      this.rootSchema = rootSchema;
      this.packageName = packageName;
    }

    public String getExpectedProto() {
      return expectedProto;
    }

    public Schema getRootSchema() {
      return rootSchema;
    }

    public String getPackageName() {
      return packageName;
    }

    public LogicalType toLogicalType() {
      return new LogicalType(packageName, rootSchema,
          Map.of(), List.of(), Map.of());
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public static Stream<TypeMapping> get() {
    return Stream.of(
        nestedRowsCase(),
        nestedRowsSameNameCase(),
        allSimpleTypesCase(),
        collectionsCase(),
        stringTypesCase(),
        notNullMessageTypesCase(),
        nestedRowNotNullCase(),
        nullableArraysCase(),
        nullableCollectionsCase(),
        multisetCase());
  }

  // ---------- helpers ----------

  /** Build a non-nullable LT struct from positional fields. */
  static Schema struct(Field... fields) {
    return Schema.createStruct(Arrays.asList(fields)).setNullable(false);
  }

  /** Build a nullable LT struct from positional fields. */
  static Schema nullableStruct(Field... fields) {
    return Schema.createStruct(Arrays.asList(fields)).setNullable(true);
  }

  /** Build a Field with no doc/default/tags/params. */
  static Field f(int pos, String name, Schema schema) {
    return new Field(name, schema, pos, null, false, null, null, null);
  }

  /** Build a Field with a doc string. */
  static Field f(int pos, String name, Schema schema, String doc) {
    return new Field(name, schema, pos, null, false, doc, null, null);
  }

  static String readResource(String path) {
    try (InputStream in =
             ProtoMappingsV1.class.getClassLoader().getResourceAsStream(path)) {
      if (in == null) {
        throw new IllegalStateException("Missing resource: " + path);
      }
      byte[] bytes = in.readAllBytes();
      return new String(bytes, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // ---------- mappings ----------

  public static TypeMapping nestedRowsCase() {
    return new TypeMapping(
        "NESTED_ROWS_CASE",
        "syntax = \"proto3\";\n"
            + "package io.confluent.protobuf.generated;\n"
            + "\n"
            + "message Row {\n"
            + "  optional meta_Row meta = 1;\n"
            + "\n"
            + "  message meta_Row {\n"
            + "    optional tags_Row tags = 1;\n"
            + "  \n"
            + "    message tags_Row {\n"
            + "      float a = 1;\n"
            + "      float b = 2;\n"
            + "    }\n"
            + "  }\n"
            + "}\n",
        struct(
            f(0, "meta", nullableStruct(
                f(0, "tags", nullableStruct(
                    f(0, "a", Schema.create(Schema.Type.FLOAT).setNullable(false)),
                    f(1, "b", Schema.create(Schema.Type.FLOAT).setNullable(false))))))));
  }

  public static TypeMapping nestedRowsSameNameCase() {
    return new TypeMapping(
        "NESTED_ROWS_SAME_NAME",
        "syntax = \"proto3\";\n"
            + "package io.confluent.protobuf.generated;\n"
            + "\n"
            + "message Row {\n"
            + "  optional b_Row b = 1;\n"
            + "\n"
            + "  message b_Row {\n"
            + "    optional b_Row b = 1 [(confluent.field_meta) = {\n"
            + "      doc: \"nested_b comment\"\n"
            + "    }];\n"
            + "  \n"
            + "    message b_Row {\n"
            + "      optional float a = 1;\n"
            + "    }\n"
            + "  }\n"
            + "}\n",
        struct(
            f(0, "b", nullableStruct(
                f(0, "b", nullableStruct(
                    f(0, "a", Schema.create(Schema.Type.FLOAT).setNullable(true))),
                    "nested_b comment")))));
  }

  public static TypeMapping allSimpleTypesCase() {
    return new TypeMapping(
        "ALL_SIMPLE_TYPES_CASE",
        readResource("schema/proto/all_simple_types.proto"),
        struct(
            f(0, "booleanNotNull",
                Schema.create(Schema.Type.BOOLEAN).setNullable(false)),
            f(1, "boolean",
                Schema.create(Schema.Type.BOOLEAN).setNullable(false)),
            f(2, "tinyIntNotNull",
                Schema.create(Schema.Type.TINYINT).setNullable(false)),
            f(3, "tinyInt",
                Schema.create(Schema.Type.TINYINT).setNullable(true), "tinyInt comment"),
            f(4, "smallIntNotNull",
                Schema.create(Schema.Type.SMALLINT).setNullable(false)),
            f(5, "smallInt",
                Schema.create(Schema.Type.SMALLINT).setNullable(true), "smallInt comment"),
            f(6, "intNotNull",
                Schema.create(Schema.Type.INT).setNullable(false)),
            f(7, "int",
                Schema.create(Schema.Type.INT).setNullable(true), "int comment"),
            f(8, "bigintNotNull",
                Schema.create(Schema.Type.BIGINT).setNullable(false)),
            f(9, "bigint",
                Schema.create(Schema.Type.BIGINT).setNullable(true), "bigint comment"),
            f(10, "doubleNotNull",
                Schema.create(Schema.Type.DOUBLE).setNullable(false)),
            f(11, "double",
                Schema.create(Schema.Type.DOUBLE).setNullable(true), "double comment"),
            f(12, "floatNotNull",
                Schema.create(Schema.Type.FLOAT).setNullable(false)),
            f(13, "float",
                Schema.create(Schema.Type.FLOAT).setNullable(true), "float comment"),
            f(14, "date",
                Schema.create(Schema.Type.DATE).setNullable(true), "date comment"),
            f(15, "decimal",
                Schema.createDecimal(5, 1).setNullable(true), "decimal comment"),
            f(16, "timestamp_ltz",
                Schema.createTimestampLtz(9).setNullable(true), "timestamp comment"),
            f(17, "timestamp_ltz_3",
                Schema.createTimestampLtz(3).setNullable(true)),
            f(18, "timestamp",
                Schema.createTimestamp(9).setNullable(true)),
            f(19, "timestamp_3",
                Schema.createTimestamp(3).setNullable(true)),
            f(20, "time",
                Schema.createTime(3).setNullable(true), "time comment"),
            f(21, "time_2",
                Schema.createTime(2).setNullable(true))));
  }

  public static TypeMapping collectionsCase() {
    return new TypeMapping(
        "COLLECTIONS_CASE",
        "syntax = \"proto3\";\n"
            + "package io.confluent.protobuf.generated;\n"
            + "\n"
            + "message Row {\n"
            + "  repeated int64 array = 1 [(confluent.field_meta) = {\n"
            + "    doc: \"array comment\"\n"
            + "  }];\n"
            + "  repeated MapEntry map = 2 [(confluent.field_meta) = {\n"
            + "    doc: \"map comment\"\n"
            + "  }];\n"
            + "\n"
            + "  message MapEntry {\n"
            + "    optional string key = 1;\n"
            + "    optional int64 value = 2;\n"
            + "  }\n"
            + "}",
        struct(
            f(0, "array",
                Schema.createArray(
                    Schema.create(Schema.Type.BIGINT).setNullable(false))
                    .setNullable(false), "array comment"),
            f(1, "map",
                Schema.createMap(
                    Schema.createString().setNullable(true),
                    Schema.create(Schema.Type.BIGINT).setNullable(true))
                    .setNullable(false), "map comment")));
  }

  public static TypeMapping stringTypesCase() {
    return new TypeMapping(
        "STRING_TYPES_CASE",
        readResource("schema/proto/strings.proto"),
        struct(
            f(0, "string",
                Schema.createString().setNullable(true), "string comment"),
            f(1, "charMax",
                Schema.createChar(Schema.MAX_LENGTH).setNullable(true)),
            f(2, "bytes",
                Schema.createBytes().setNullable(true)),
            f(3, "binaryMax",
                Schema.createBinary(Schema.MAX_LENGTH).setNullable(true)),
            f(4, "varchar",
                Schema.createVarchar(123).setNullable(true)),
            f(5, "varbinary",
                Schema.createVarbinary(123).setNullable(true)),
            f(6, "char",
                Schema.createChar(123).setNullable(true)),
            f(7, "binary",
                Schema.createBinary(123).setNullable(true))));
  }

  public static TypeMapping notNullMessageTypesCase() {
    return new TypeMapping(
        "NOT_NULL_MESSAGE_TYPES_CASE",
        readResource("schema/proto/not_null_message_types.proto"),
        struct(
            f(0, "date",
                Schema.create(Schema.Type.DATE).setNullable(false)),
            f(1, "decimal",
                Schema.createDecimal(5, 1).setNullable(false)),
            f(2, "timestamp_ltz",
                Schema.createTimestampLtz(9).setNullable(false)),
            f(3, "timestamp",
                Schema.createTimestamp(9).setNullable(false)),
            f(4, "time",
                Schema.createTime(3).setNullable(false))));
  }

  public static TypeMapping nestedRowNotNullCase() {
    return new TypeMapping(
        "NESTED_ROW_NOT_NULL_CASE",
        "syntax = \"proto3\";\n"
            + "package io.confluent.protobuf.generated;\n"
            + "\n"
            + "message Row {\n"
            + "  meta_Row meta = 1 [(confluent.field_meta) = {\n"
            + "    params: [\n"
            + "      {\n"
            + "        key: \"flink.version\",\n"
            + "        value: \"1\"\n"
            + "      },\n"
            + "      {\n"
            + "        key: \"flink.notNull\",\n"
            + "        value: \"true\"\n"
            + "      }\n"
            + "    ]\n"
            + "  }];\n"
            + "\n"
            + "  message meta_Row {\n"
            + "    float a = 1;\n"
            + "    float b = 2;\n"
            + "  }\n"
            + "}\n",
        struct(
            f(0, "meta", struct(
                f(0, "a", Schema.create(Schema.Type.FLOAT).setNullable(false)),
                f(1, "b", Schema.create(Schema.Type.FLOAT).setNullable(false))))));
  }

  public static TypeMapping nullableArraysCase() {
    return new TypeMapping(
        "NULLABLE_ARRAYS_CASE",
        readResource("schema/proto/nullable_arrays.proto"),
        struct(
            f(0, "arrayNullable",
                Schema.createArray(
                    Schema.create(Schema.Type.BIGINT).setNullable(false))
                    .setNullable(true), "arrayNullable comment"),
            f(1, "elementNullable",
                Schema.createArray(
                    Schema.create(Schema.Type.BIGINT).setNullable(true))
                    .setNullable(false), "elementNullable comment"),
            f(2, "arrayAndElementNullable",
                Schema.createArray(
                    Schema.create(Schema.Type.BIGINT).setNullable(true))
                    .setNullable(true), "arrayAndElementNullable comment")));
  }

  public static TypeMapping nullableCollectionsCase() {
    return new TypeMapping(
        "NULLABLE_COLLECTIONS_CASE",
        readResource("schema/proto/nullable_collections.proto"),
        struct(
            f(0, "nullableMap",
                Schema.createMap(
                    Schema.create(Schema.Type.BIGINT).setNullable(false),
                    Schema.create(Schema.Type.BIGINT).setNullable(false))
                    .setNullable(true), "nullableMap comment"),
            f(1, "arrayOfMaps",
                Schema.createArray(
                    Schema.createMap(
                        Schema.create(Schema.Type.BIGINT).setNullable(false),
                        Schema.create(Schema.Type.BIGINT).setNullable(false))
                        .setNullable(false))
                    .setNullable(false), "arrayOfMaps comment"),
            f(2, "nullableArrayOfNullableMaps",
                Schema.createArray(
                    Schema.createMap(
                        Schema.create(Schema.Type.BIGINT).setNullable(false),
                        Schema.create(Schema.Type.BIGINT).setNullable(false))
                        .setNullable(true))
                    .setNullable(true), "nullableArrayOfNullableMaps comment"),
            f(3, "mapOfNullableArrays",
                Schema.createMap(
                    Schema.create(Schema.Type.BIGINT).setNullable(false),
                    Schema.createArray(
                        Schema.create(Schema.Type.BIGINT).setNullable(false))
                        .setNullable(true))
                    .setNullable(false), "mapOfNullableArrays comment")));
  }

  public static TypeMapping multisetCase() {
    return new TypeMapping(
        "MULTISET_CASE",
        "syntax = \"proto3\";\n"
            + "package io.confluent.protobuf.generated;\n"
            + "\n"
            + "message Row {\n"
            + "  repeated MultisetEntry multiset = 1 [(confluent.field_meta) = {\n"
            + "    params: [\n"
            + "      {\n"
            + "        key: \"flink.version\",\n"
            + "        value: \"1\"\n"
            + "      },\n"
            + "      {\n"
            + "        key: \"flink.type\",\n"
            + "        value: \"multiset\"\n"
            + "      }\n"
            + "    ]\n"
            + "  }];\n"
            + "\n"
            + "  message MultisetEntry {\n"
            + "    optional string key = 1;\n"
            + "    int32 value = 2;\n"
            + "  }\n"
            + "}",
        struct(
            f(0, "multiset",
                Schema.createMultiset(
                    Schema.createString().setNullable(true))
                    .setNullable(false))));
  }

  private ProtoMappingsV1() {}
}
