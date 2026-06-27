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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroToLogicalTypeConverterTest {

  @Test
  void testTypeMappings() {
    for (CommonMappings.TypeMapping mapping :
        CommonMappings.get().collect(Collectors.toList())) {
      Schema result = AvroToLogicalTypeConverter.toRootSchema(
          new AvroSchema(mapping.getAvroSchema()));
      assertEquals(mapping.getLogicalType(), result, "Failed for: " + mapping);
    }
  }

  @Test
  void testRecordWithCycles() {
    // Build a truly cyclic schema. Cycles round-trip cleanly through the LT
    // model now that every unmarked Avro record becomes a NAMED_TYPE_REF +
    // entry in localNamedTypes — the self-reference resolves via the named
    // type rather than infinite recursion.
    org.apache.avro.Schema recordSchema =
        org.apache.avro.Schema.createRecord("CyclicRecord", null, null, false);
    recordSchema.setFields(
        java.util.Collections.singletonList(
            new org.apache.avro.Schema.Field("self",
                org.apache.avro.Schema.createUnion(
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                    recordSchema),
                null, null)));

    io.confluent.kafka.schemaregistry.type.logical.LogicalType lt =
        AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(recordSchema));
    assertEquals(Schema.Type.NAMED_TYPE_REF, lt.getRootSchema().getType());
    assertEquals("CyclicRecord", lt.getRootSchema().getQualifiedName());
    Schema cyclic = lt.getNamedTypes().get("CyclicRecord");
    assertEquals(Schema.Type.STRUCT, cyclic.getType());
    assertEquals(1, cyclic.getFields().size());
    Schema selfField = cyclic.getFields().get(0).getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, selfField.getType());
    assertEquals("CyclicRecord", selfField.getQualifiedName());
    assertTrue(selfField.isNullable());
  }

  @Test
  void testEnumConversion() {
    // Unmarked Avro enum is recovered as a NAMED_TYPE_REF; the actual ENUM
    // body lives in localNamedTypes keyed by the Avro full name.
    org.apache.avro.Schema enumSchema = SchemaBuilder.enumeration("Color")
        .symbols("RED", "GREEN", "BLUE");
    io.confluent.kafka.schemaregistry.type.logical.LogicalType lt =
        AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(enumSchema));
    assertEquals(Schema.Type.NAMED_TYPE_REF, lt.getRootSchema().getType());
    assertEquals("Color", lt.getRootSchema().getQualifiedName());
    Schema named = lt.getNamedTypes().get("Color");
    assertEquals(Schema.Type.ENUM, named.getType());
    assertEquals(3, named.getEnumValues().size());
    assertEquals("RED", named.getEnumValues().get(0).getSymbol());
    assertEquals("GREEN", named.getEnumValues().get(1).getSymbol());
    assertEquals("BLUE", named.getEnumValues().get(2).getSymbol());
  }

  @Test
  void testProperUnionConversion() {
    org.apache.avro.Schema unionSchema = org.apache.avro.Schema.createUnion(
        SchemaBuilder.builder().intType(),
        SchemaBuilder.builder().stringType());
    Schema result = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(unionSchema));
    assertEquals(Schema.Type.UNION, result.getType());
    assertEquals(2, result.getBranches().size());
    assertEquals("int", result.getBranches().get(0).getName());
    assertEquals(Schema.Type.INT, result.getBranches().get(0).getSchema().getType());
    assertEquals("string", result.getBranches().get(1).getName());
    assertEquals(Schema.Type.VARCHAR, result.getBranches().get(1).getSchema().getType());
  }

  @Test
  void testNullableUnionConversion() {
    org.apache.avro.Schema nullableString = SchemaBuilder.unionOf()
        .nullType().and().stringType().endUnion();
    Schema result = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(nullableString));
    assertEquals(Schema.Type.VARCHAR, result.getType());
    assertTrue(result.isNullable());
  }

  @Test
  void testNullableProperUnion() {
    org.apache.avro.Schema nullableUnion = org.apache.avro.Schema.createUnion(
        SchemaBuilder.builder().nullType(),
        SchemaBuilder.builder().intType(),
        SchemaBuilder.builder().stringType());
    Schema result = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(nullableUnion));
    assertEquals(Schema.Type.UNION, result.getType());
    assertTrue(result.isNullable());
    assertEquals(2, result.getBranches().size());
  }

  // =========================================================================
  // Legacy-form MapEntry recognition (Flink-emitted and AvroData-emitted).
  //
  // Flink and AvroData (anonymous Connect schemas) use the canonical entry-
  // record name io.confluent.connect.avro.MapEntry. AvroData (named Connect
  // schemas) uses an arbitrary record name with the connect.internal.type=
  // MapEntry prop. The LT reader accepts both as fallbacks alongside its
  // primary LogicalMap-on-array recognition.
  // =========================================================================

  @Test
  void testReadsLegacyCanonicalMapEntry() {
    // Schema as emitted by Flink-Avro and AvroData (anonymous): array of
    // canonical MapEntry record, no logicalType=map marker.
    org.apache.avro.Schema entry = SchemaBuilder.record("MapEntry")
        .namespace("io.confluent.connect.avro")
        .fields()
        .name("key").type().intType().noDefault()
        .name("value").type().stringType().noDefault()
        .endRecord();
    org.apache.avro.Schema arr = SchemaBuilder.array().items(entry);
    Schema result = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(arr));
    assertEquals(Schema.Type.MAP, result.getType());
    assertEquals(Schema.Type.INT, result.getKeyType().getType());
    assertEquals(Schema.Type.VARCHAR, result.getValueType().getType());
  }

  @Test
  void testReadsLegacyConnectInternalTypeProp() {
    // Schema as emitted by AvroData when the Connect schema has its own name:
    // user-named record with connect.internal.type=MapEntry prop.
    org.apache.avro.Schema entry = SchemaBuilder.record("MyEntry")
        .namespace("com.example")
        .prop("connect.internal.type", "MapEntry")
        .fields()
        .name("key").type().longType().noDefault()
        .name("value").type().booleanType().noDefault()
        .endRecord();
    org.apache.avro.Schema arr = SchemaBuilder.array().items(entry);
    Schema result = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(arr));
    assertEquals(Schema.Type.MAP, result.getType());
    assertEquals(Schema.Type.BIGINT, result.getKeyType().getType());
    assertEquals(Schema.Type.BOOLEAN, result.getValueType().getType());
  }
}
