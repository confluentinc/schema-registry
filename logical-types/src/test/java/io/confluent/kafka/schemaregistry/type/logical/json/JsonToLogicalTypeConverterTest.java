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

package io.confluent.kafka.schemaregistry.type.logical.json;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonToLogicalTypeConverterTest {

  @Test
  void testBooleanType() {
    org.everit.json.schema.Schema jsonSchema = BooleanSchema.builder().build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.BOOLEAN, result.getType());
    assertFalse(result.isNullable());
  }

  @Test
  void testIntegerType() {
    org.everit.json.schema.Schema jsonSchema = NumberSchema.builder()
        .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.INT, result.getType());
  }

  @Test
  void testStringType() {
    org.everit.json.schema.Schema jsonSchema = StringSchema.builder().build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.VARCHAR, result.getType());
  }

  @Test
  void testNullableType() {
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NullSchema.INSTANCE)
        .subschema(BooleanSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.BOOLEAN, result.getType());
    assertTrue(result.isNullable());
  }

  @Test
  void testEnumType() {
    org.everit.json.schema.Schema jsonSchema = EnumSchema.builder()
        .possibleValues(Arrays.asList("RED", "GREEN", "BLUE"))
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.ENUM, result.getType());
    assertEquals(3, result.getEnumValues().size());
    assertEquals("RED", result.getEnumValues().get(0).getSymbol());
  }

  @Test
  void testUnionType() {
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
            .build())
        .subschema(StringSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.UNION, result.getType());
    assertEquals(2, result.getBranches().size());
  }

  @Test
  void testObjectType() {
    org.everit.json.schema.Schema jsonSchema = ObjectSchema.builder()
        .addPropertySchema("name", StringSchema.builder().build())
        .addPropertySchema("age", NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
            .build())
        .addRequiredProperty("name")
        .additionalProperties(false)
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.STRUCT, result.getType());
    assertEquals(2, result.getFields().size());
  }

  @Test
  void testVariantType() {
    org.everit.json.schema.Schema jsonSchema = EmptySchema.builder().build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.VARIANT, result.getType());
    assertFalse(result.isNullable());
  }

  @Test
  void testArrayType() {
    org.everit.json.schema.Schema jsonSchema = org.everit.json.schema.ArraySchema.builder()
        .allItemSchema(StringSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.ARRAY, result.getType());
    assertEquals(Schema.Type.VARCHAR, result.getElementType().getType());
  }

  @Test
  void testTypeMappings() {
    // Matrix-driven coverage. Each TypeMapping in CommonMappings goes
    // LT -> JSON -> LT and the result must equal the original. Adding a new
    // primitive Schema.Type without registering a mapping here surfaces as a
    // failing test until coverage is added.
    for (CommonMappings.TypeMapping mapping : CommonMappings.get()) {
      Schema original = mapping.asRootStruct();
      Schema rt = JsonToLogicalTypeConverter.toRootSchema(
          LogicalTypeToJsonConverter.fromLogicalType(
              new io.confluent.kafka.schemaregistry.type.logical.LogicalType(original),
              "Holder"));
      assertEquals(original, rt, "Round trip failed for " + mapping);
    }
  }

  @Test
  void testNullableProperUnion() {
    // Three-way union of null + two non-null types becomes a nullable UNION
    // with 2 non-null branches.
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NullSchema.INSTANCE)
        .subschema(NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
            .build())
        .subschema(StringSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.UNION, result.getType());
    assertTrue(result.isNullable());
    assertEquals(2, result.getBranches().size());
  }

  @Test
  void testUnionWithManyBranches() {
    // 4-branch union (no null) — order preserved.
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
            .build())
        .subschema(StringSchema.builder().build())
        .subschema(BooleanSchema.builder().build())
        .subschema(NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap("connect.type", "float64"))
            .build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.UNION, result.getType());
    assertEquals(4, result.getBranches().size());
  }

  @Test
  void testSingletonOneOfCollapsesToMemberType() {
    // oneOf:[T] is semantically equivalent to T in JSON Schema (the value
    // satisfies exactly one schema, but there's only one option). Reader
    // collapses it to the member type — matches Avro behavior.
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(StringSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.VARCHAR, result.getType());
    assertFalse(result.isNullable());
  }

  @Test
  void testSingletonOneOfWithNullCollapsesToNullableMember() {
    // oneOf:[null, T] also collapses (equivalent to nullable T). Existing
    // behavior — verified explicitly here for parity with the singleton case.
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NullSchema.INSTANCE)
        .subschema(StringSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.VARCHAR, result.getType());
    assertTrue(result.isNullable());
  }

  @Test
  void testEmptyObjectIsStruct() {
    // An object schema with no properties round-trips as an empty STRUCT.
    org.everit.json.schema.Schema jsonSchema = ObjectSchema.builder()
        .additionalProperties(false)
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.STRUCT, result.getType());
    assertTrue(result.getFields().isEmpty());
  }

  @Test
  void testSingletonObjectOneOfIsUnionInV1ButCollapsesInV2() {
    // Mirrors getNullableReference: a single-member oneOf of an object stays a
    // 1-branch union under V1 (old Flink's union-wrapper) but collapses to the
    // member under V2 (canonical).
    org.everit.json.schema.Schema oneOf = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(ObjectSchema.builder()
            .addPropertySchema("x", NumberSchema.builder().requiresInteger(true).build())
            .build())
        .build();

    Schema v1 = JsonToLogicalTypeConverter
        .toLogicalType(new JsonSchema(oneOf), LogicalTypeVersion.V1).getRootSchema();
    assertEquals(Schema.Type.UNION, v1.getType());
    assertEquals(1, v1.getBranches().size());
    assertEquals(Schema.Type.STRUCT, v1.getBranches().get(0).getSchema().getType());

    Schema v2 = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(oneOf));
    assertEquals(Schema.Type.STRUCT, v2.getType());
  }

  @Test
  void testSingletonOneOfFieldIsUnionInV1ButCollapsesInV2() {
    // Mirrors testSchemaReference (nested references): a field whose type is a
    // single-member oneOf stays a union field under V1, collapses under V2.
    org.everit.json.schema.Schema root = ObjectSchema.builder()
        .addPropertySchema("f1", CombinedSchema.builder()
            .criterion(CombinedSchema.ONE_CRITERION)
            .subschema(ObjectSchema.builder()
                .addPropertySchema("x", StringSchema.builder().build()).build())
            .build())
        .addRequiredProperty("f1")
        .build();

    Schema f1V1 = JsonToLogicalTypeConverter
        .toLogicalType(new JsonSchema(root), LogicalTypeVersion.V1)
        .getRootSchema().getField("f1").getSchema();
    assertEquals(Schema.Type.UNION, f1V1.getType());

    Schema f1V2 = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(root))
        .getField("f1").getSchema();
    assertEquals(Schema.Type.STRUCT, f1V2.getType());
  }

  @Test
  void testSingletonOneOfOfReferenceIsUnionInV1() {
    // Mirrors testRoundTrip[5] / the reference cases: a single-member oneOf of a
    // $ref stays a 1-branch union (wrapping a NAMED_TYPE_REF) under V1 and
    // collapses to the NAMED_TYPE_REF under V2.
    String json = "{\"type\":\"object\","
        + "\"properties\":{\"f1\":{\"oneOf\":[{\"$ref\":\"#/definitions/T\"}]}},"
        + "\"required\":[\"f1\"],"
        + "\"definitions\":{\"T\":{\"type\":\"object\","
        + "\"properties\":{\"x\":{\"type\":\"string\"}}}}}";

    Schema f1V1 = JsonToLogicalTypeConverter
        .toLogicalType(new JsonSchema(json), LogicalTypeVersion.V1)
        .getRootSchema().getField("f1").getSchema();
    assertEquals(Schema.Type.UNION, f1V1.getType());

    Schema f1V2 = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(json))
        .getField("f1").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, f1V2.getType());
  }

  @Test
  void testReferenceToNullableObjectStaysNullable() {
    // Mirrors getNonRequiredReference: a required field referencing a nullable
    // object (type:[object,null]) must keep the referenced type nullable, so the
    // Flink projection reads it as a nullable row rather than a union.
    String json = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\","
        + "\"properties\":{\"ref1\":{\"$ref\":\"#/definitions/opt\"}},"
        + "\"required\":[\"ref1\"],"
        + "\"definitions\":{\"opt\":{\"type\":[\"object\",\"null\"],"
        + "\"properties\":{\"x\":{\"type\":\"string\"}}}}}";

    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(
        new JsonSchema(json), LogicalTypeVersion.V1);
    Schema opt = lt.getNamedTypes().get("opt");
    assertTrue(opt != null && opt.isNullable(),
        "referenced nullable object must remain nullable: " + lt.getNamedTypes());
  }
}
