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

import io.confluent.avro.type.VariantLogicalType;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogicalTypeToAvroConverterTest {

  @Test
  void testEnumConversion() {
    Schema enumSchema = Schema.createEnum(Arrays.asList(
        new EnumValue("RED"), new EnumValue("GREEN"), new EnumValue("BLUE")))
        .setNullable(false);
    AvroSchema result = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(enumSchema), "Color");
    assertEquals(org.apache.avro.Schema.Type.ENUM, result.rawSchema().getType());
    assertEquals(Arrays.asList("RED", "GREEN", "BLUE"), result.rawSchema().getEnumSymbols());
  }

  @Test
  void testNullableEnumConversion() {
    Schema enumSchema = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B")))
        .setNullable(true);
    AvroSchema result = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(enumSchema), "AB");
    assertTrue(result.rawSchema().isUnion());
    assertEquals(2, result.rawSchema().getTypes().size());
    assertEquals(org.apache.avro.Schema.Type.NULL, result.rawSchema().getTypes().get(0).getType());
    assertEquals(org.apache.avro.Schema.Type.ENUM, result.rawSchema().getTypes().get(1).getType());
  }

  @Test
  void testUnionConversion() {
    Schema unionSchema = Schema.createUnion(Arrays.asList(
        new UnionBranch("int", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("string", Schema.createString().setNullable(false))))
        .setNullable(false);
    AvroSchema result = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(unionSchema), "MyUnion");
    assertTrue(result.rawSchema().isUnion());
    assertEquals(2, result.rawSchema().getTypes().size());
    assertEquals(org.apache.avro.Schema.Type.INT, result.rawSchema().getTypes().get(0).getType());
    assertEquals(org.apache.avro.Schema.Type.STRING, result.rawSchema().getTypes().get(1).getType());
  }

  @Test
  void testNullableUnionConversion() {
    Schema unionSchema = Schema.createUnion(Arrays.asList(
        new UnionBranch("int", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("string", Schema.createString().setNullable(false))))
        .setNullable(true);
    AvroSchema result = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(unionSchema), "MyUnion");
    assertTrue(result.rawSchema().isUnion());
    assertEquals(3, result.rawSchema().getTypes().size());
    assertEquals(org.apache.avro.Schema.Type.NULL, result.rawSchema().getTypes().get(0).getType());
    assertEquals(org.apache.avro.Schema.Type.INT, result.rawSchema().getTypes().get(1).getType());
    assertEquals(org.apache.avro.Schema.Type.STRING, result.rawSchema().getTypes().get(2).getType());
  }

  @Test
  void testInvalidFieldNames() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("invalid.name",
            Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    assertThrows(ValidationException.class, () ->
        LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(struct), "TestRecord"));
  }

  @Test
  void testRoundTripPrimitives() {
    for (Schema.Type type : new Schema.Type[]{
        Schema.Type.BOOLEAN, Schema.Type.INT, Schema.Type.BIGINT,
        Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.DATE}) {
      Schema original = Schema.create(type).setNullable(false);
      AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(original), "row");
      Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);
      assertEquals(original, roundTripped, "Round trip failed for " + type);
    }
    // STRING and BYTES are aliases for VARCHAR(MAX) and VARBINARY(MAX)
    Schema stringOriginal = Schema.createString().setNullable(false);
    Schema stringRoundTripped = AvroToLogicalTypeConverter.toRootSchema(
        LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(stringOriginal), "row"));
    assertEquals(stringOriginal, stringRoundTripped, "Round trip failed for STRING");

    Schema bytesOriginal = Schema.createBytes().setNullable(false);
    Schema bytesRoundTripped = AvroToLogicalTypeConverter.toRootSchema(
        LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(bytesOriginal), "row"));
    assertEquals(bytesOriginal, bytesRoundTripped, "Round trip failed for BYTES");
  }

  @Test
  void testRoundTripUnionBranchNamesInStruct() {
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("amount", Schema.create(Schema.Type.INT).setNullable(true)),
        new UnionBranch("description", Schema.createString().setNullable(true))))
        .setNullable(false);
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("payment", union, 0)))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(struct), "TestRecord");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    assertEquals(Schema.Type.UNION, roundTripped.getField("payment").getSchema().getType());
    assertEquals("amount",
        roundTripped.getField("payment").getSchema().getBranches().get(0).getName());
    assertEquals("description",
        roundTripped.getField("payment").getSchema().getBranches().get(1).getName());
  }

  @Test
  void testRoundTripUnionBranchDocInStruct() {
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("amt", Schema.create(Schema.Type.INT).setNullable(true),
            "the amount", null),
        new UnionBranch("desc", Schema.createString().setNullable(true),
            "the description", null)))
        .setNullable(false);
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("payment", union, 0)))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(struct), "TestRecord");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    assertEquals("the amount",
        roundTripped.getField("payment").getSchema().getBranches().get(0).getDoc());
    assertEquals("the description",
        roundTripped.getField("payment").getSchema().getBranches().get(1).getDoc());
  }

  @Test
  void testRoundTripUnionBranchParamsInStruct() {
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("format", "currency");
    params.put("precision", "2");
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("amt", Schema.create(Schema.Type.INT).setNullable(true),
            null, params),
        new UnionBranch("desc", Schema.createString().setNullable(true))))
        .setNullable(false);
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("payment", union, 0)))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(struct), "TestRecord");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    Map<String, Object> rtParams =
        roundTripped.getField("payment").getSchema().getBranches().get(0).getParams();
    assertEquals("currency", rtParams.get("format"));
    assertEquals("2", rtParams.get("precision"));
    assertTrue(roundTripped.getField("payment").getSchema().getBranches().get(1)
        .getParams().isEmpty());
  }

  @Test
  void testRoundTripRootUnionBranchNames() {
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("amount", Schema.create(Schema.Type.INT).setNullable(true)),
        new UnionBranch("description", Schema.createString().setNullable(true))))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(union), "root");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    assertEquals(Schema.Type.UNION, roundTripped.getType());
    assertEquals("amount", roundTripped.getBranches().get(0).getName());
    assertEquals("description", roundTripped.getBranches().get(1).getName());
  }

  @Test
  void testRoundTripRootUnionBranchDocAndParams() {
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("key", "value");
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("amt", Schema.create(Schema.Type.INT).setNullable(true),
            "amount doc", params),
        new UnionBranch("desc", Schema.createString().setNullable(true),
            "desc doc", null)))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(union), "root");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    assertEquals("amount doc", roundTripped.getBranches().get(0).getDoc());
    assertEquals("desc doc", roundTripped.getBranches().get(1).getDoc());
    assertEquals("value", roundTripped.getBranches().get(0).getParams().get("key"));
    assertTrue(roundTripped.getBranches().get(1).getParams().isEmpty());
  }

  @Test
  void testRoundTripStructDocTagsParams() {
    Map<String, Object> schemaParams = new LinkedHashMap<>();
    schemaParams.put("version", "2");
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0)))
        .setNullable(false)
        .setDoc("a person record")
        .setTags(Arrays.asList("PII", "SENSITIVE"))
        .setParams(schemaParams);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(struct), "Person");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    assertEquals("a person record", roundTripped.getDoc());
    assertEquals(Arrays.asList("PII", "SENSITIVE"), roundTripped.getTags());
    assertEquals("2", roundTripped.getParams().get("version"));
  }

  @Test
  void testRoundTripEnumDocTagsParams() {
    Map<String, Object> schemaParams = new LinkedHashMap<>();
    schemaParams.put("source", "api");
    Schema enumSchema = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B")))
        .setNullable(false)
        .setDoc("status codes")
        .setTags(Arrays.asList("INTERNAL"))
        .setParams(schemaParams);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(enumSchema), "Status");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    assertEquals("status codes", roundTripped.getDoc());
    assertEquals(Arrays.asList("INTERNAL"), roundTripped.getTags());
    assertEquals("api", roundTripped.getParams().get("source"));
  }

  @Test
  void testRoundTripEnumValueDocAndParams() {
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("rgb", "#FF0000");
    Schema enumSchema = Schema.createEnum(Arrays.asList(
        new EnumValue("RED", "the color red", params),
        new EnumValue("GREEN", "the color green", null),
        new EnumValue("BLUE")))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(enumSchema), "Color");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    assertEquals("the color red", roundTripped.getEnumValues().get(0).getDoc());
    assertEquals("#FF0000", roundTripped.getEnumValues().get(0).getParams().get("rgb"));
    assertEquals("the color green", roundTripped.getEnumValues().get(1).getDoc());
    assertTrue(roundTripped.getEnumValues().get(1).getParams().isEmpty());
    assertNull(roundTripped.getEnumValues().get(2).getDoc());
  }

  @Test
  void testRoundTripFieldTagsAndParams() {
    Map<String, Object> fieldParams = new LinkedHashMap<>();
    fieldParams.put("sensitivity", "high");
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("email", Schema.createString().setNullable(false), 0,
            null, false, null, Arrays.asList("PII", "EMAIL"), fieldParams),
        new Field("name", Schema.createString().setNullable(false), 1)))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(struct), "Person");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    assertEquals(Arrays.asList("PII", "EMAIL"), roundTripped.getField("email").getTags());
    assertEquals("high", roundTripped.getField("email").getParams().get("sensitivity"));
    assertTrue(roundTripped.getField("name").getTags().isEmpty());
    assertTrue(roundTripped.getField("name").getParams().isEmpty());
  }

  @Test
  void testRoundTripMapWithVarcharKeyLength() {
    Schema map = Schema.createMap(
        Schema.createVarchar(100).setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(false);
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("data", map, 0)))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(struct), "TestRecord");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    Schema rtMap = roundTripped.getField("data").getSchema();
    assertEquals(Schema.Type.MAP, rtMap.getType());
    assertEquals(Schema.Type.VARCHAR, rtMap.getKeyType().getType());
    assertEquals(100, rtMap.getKeyType().getLength());
  }

  @Test
  void testVariantConversion() {
    Schema variant = Schema.create(Schema.Type.VARIANT).setNullable(false);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(variant), "row");
    assertEquals(org.apache.avro.Schema.Type.RECORD, avro.rawSchema().getType());
    assertEquals(VariantLogicalType.NAME, avro.rawSchema().getLogicalType().getName());
    assertEquals(2, avro.rawSchema().getFields().size());
    assertEquals("metadata", avro.rawSchema().getFields().get(0).name());
    assertEquals("value", avro.rawSchema().getFields().get(1).name());
  }

  @Test
  void testRoundTripVariant() {
    Schema original = Schema.create(Schema.Type.VARIANT).setNullable(false);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(original), "row");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);
    assertEquals(original, roundTripped);
  }

  @Test
  void testRoundTripVariantInStruct() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("data", Schema.create(Schema.Type.VARIANT).setNullable(true), 0),
        new Field("name", Schema.createString().setNullable(false), 1)))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(struct), "TestRecord");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    assertEquals(Schema.Type.VARIANT, roundTripped.getField("data").getSchema().getType());
    assertTrue(roundTripped.getField("data").getSchema().isNullable());
    assertEquals(Schema.Type.VARCHAR, roundTripped.getField("name").getSchema().getType());
  }

  @Test
  void testRoundTripNamedTypeRef() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("street", Schema.createString().setNullable(false), 0),
        new Field("city", Schema.createString().setNullable(false), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);

    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0),
        new Field("home", Schema.createNamedTypeRef("Address").setNullable(true), 1),
        new Field("work", Schema.createNamedTypeRef("Address").setNullable(true), 2)))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(struct, namedTypes), "Person");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("work").getSchema().getType());
    // Verify named type definition was recovered
    assertEquals(1, roundTripped.getNamedTypes().size());
    Schema recoveredAddress = roundTripped.getNamedTypes().get("Address");
    assertNotNull(recoveredAddress);
    assertEquals(Schema.Type.STRUCT, recoveredAddress.getType());
    assertEquals(2, recoveredAddress.getFields().size());
    assertEquals("street", recoveredAddress.getFields().get(0).getName());
    assertEquals("city", recoveredAddress.getFields().get(1).getName());
  }

  @Test
  void testRoundTripNamedStructLossless() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("street", Schema.createVarchar(200).setNullable(false), 0),
        new Field("city", Schema.createString().setNullable(false), 1),
        new Field("zip", Schema.create(Schema.Type.INT).setNullable(true), 2)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home", Schema.createNamedTypeRef("Address").setNullable(true), 0),
        new Field("work", Schema.createNamedTypeRef("Address").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, namedTypes);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Person");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    assertEquals(original.getRootSchema(), roundTripped.getRootSchema());
    assertEquals(original.getNamedTypes(), roundTripped.getNamedTypes());
  }

  @Test
  void testRoundTripNamedEnumLossless() {
    Schema statusType = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("ACTIVE"),
        new Schema.EnumValue("INACTIVE")))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Status", statusType);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("status", Schema.createNamedTypeRef("Status").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, namedTypes);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Record");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    assertEquals(original.getRootSchema(), roundTripped.getRootSchema());
    assertEquals(original.getNamedTypes(), roundTripped.getNamedTypes());
  }

  @Test
  void testRoundTripMapWithCharKeyLength() {
    Schema map = Schema.createMap(
        Schema.createChar(10).setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(false);
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("codes", map, 0)))
        .setNullable(false);

    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(struct), "TestRecord");
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(avro);

    Schema rtMap = roundTripped.getField("codes").getSchema();
    assertEquals(Schema.Type.MAP, rtMap.getType());
    assertEquals(Schema.Type.CHAR, rtMap.getKeyType().getType());
    assertEquals(10, rtMap.getKeyType().getLength());
  }

  @Test
  void testRoundTripExternalNamedTypeRef() {
    String addressAvroSchema = "{\"type\":\"record\",\"name\":\"Address\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{\"name\":\"street\",\"type\":\"string\"},"
        + "{\"name\":\"city\",\"type\":\"string\"}]}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Address", "address-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Address", addressAvroSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0),
        new Field("home",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Person");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("com.example.Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
  }

  @Test
  void testRoundTripMultipleExternalRefs() {
    String addressSchema = "{\"type\":\"record\",\"name\":\"Address\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}";
    String moneySchema = "{\"type\":\"record\",\"name\":\"Money\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{\"name\":\"amount\",\"type\":\"double\"}]}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Address", "address-value", 1),
        new SchemaReference("com.example.Money", "money-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Address", addressSchema);
    resolvedReferences.put("com.example.Money", moneySchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 0),
        new Field("payment",
            Schema.createNamedTypeRef("com.example.Money").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Order");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("com.example.Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("payment").getSchema().getType());
    assertEquals("com.example.Money",
        roundTripped.getRootSchema().getField("payment").getSchema().getQualifiedName());
  }

  @Test
  void testRoundTripMixedLocalAndExternalRefs() {
    String externalSchema = "{\"type\":\"record\",\"name\":\"Money\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{\"name\":\"amount\",\"type\":\"double\"}]}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Money", "money-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Money", externalSchema);

    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("city", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home",
            Schema.createNamedTypeRef("Address").setNullable(true), 0),
        new Field("payment",
            Schema.createNamedTypeRef("com.example.Money").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, namedTypes,
        references, resolvedReferences);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Order");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("payment").getSchema().getType());
    assertEquals("com.example.Money",
        roundTripped.getRootSchema().getField("payment").getSchema().getQualifiedName());
    // Local named type should be recovered
    assertNotNull(roundTripped.getNamedTypes().get("Address"));
  }

  @Test
  void testRoundTripExternalNestedRecordRef() {
    // External Avro schema where the top-level record contains a nested record.
    // The LogicalType references the nested type directly via NAMED_TYPE_REF.
    String externalSchema = "{\"type\":\"record\",\"name\":\"Outer\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{\"name\":\"inner\",\"type\":{"
        + "\"type\":\"record\",\"name\":\"Inner\","
        + "\"fields\":[{\"name\":\"label\",\"type\":\"string\"}]}}]}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Outer", "outer-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Outer", externalSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("inner",
            Schema.createNamedTypeRef("com.example.Inner").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    Schema inner = roundTripped.getRootSchema().getField("inner").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, inner.getType());
    assertEquals("com.example.Inner", inner.getQualifiedName());
  }

  @Test
  void testRoundTripDefaultValues() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0,
            "Anonymous", true, null, null, null),
        new Field("age", Schema.create(Schema.Type.INT).setNullable(false), 1,
            18, true, null, null, null)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Person");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    assertEquals("Anonymous",
        roundTripped.getRootSchema().getField("name").getDefaultValue());
    assertEquals(18,
        roundTripped.getRootSchema().getField("age").getDefaultValue());
  }

  @Test
  void testRoundTripNestedDefaultValues() {
    Schema innerStruct = Schema.createStruct(Arrays.asList(
        new Field("street", Schema.createString().setNullable(false), 0,
            "Main St", true, null, null, null),
        new Field("city", Schema.createString().setNullable(false), 1,
            "Springfield", true, null, null, null)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("addr", innerStruct, 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    Schema rtInner = roundTripped.getRootSchema().getField("addr").getSchema();
    assertEquals("Main St", rtInner.getField("street").getDefaultValue());
    assertEquals("Springfield", rtInner.getField("city").getDefaultValue());
  }

  @Test
  void testRoundTripTypedDefaultValues() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("dec",
            Schema.createDecimal(10, 2).setNullable(false), 0,
            new java.math.BigDecimal("12.34"), true, null, null, null),
        new Field("dt",
            Schema.create(Schema.Type.DATE).setNullable(false), 1,
            java.time.LocalDate.of(2026, 4, 17), true, null, null, null)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    assertEquals(new java.math.BigDecimal("12.34"),
        roundTripped.getRootSchema().getField("dec").getDefaultValue());
    assertEquals(java.time.LocalDate.of(2026, 4, 17),
        roundTripped.getRootSchema().getField("dt").getDefaultValue());
  }

  @Test
  void testRoundTripTimePrecision() {
    for (int p : new int[]{0, 1, 3, 5, 6, 7, 9}) {
      Schema rootSchema = Schema.createStruct(Arrays.asList(
          new Field("t", Schema.createTime(p).setNullable(false), 0)))
          .setNullable(false);
      LogicalType original = new LogicalType(rootSchema);
      AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Holder");
      LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
      Schema rt = roundTripped.getRootSchema().getField("t").getSchema();
      assertEquals(Schema.Type.TIME, rt.getType(), "TIME(" + p + ")");
      assertEquals(p, rt.getPrecision(), "TIME precision " + p);
    }
  }

  @Test
  void testRoundTripTimestampPrecision() {
    for (int p : new int[]{0, 1, 3, 5, 6, 7, 9}) {
      Schema rootSchema = Schema.createStruct(Arrays.asList(
          new Field("ts", Schema.createTimestamp(p).setNullable(false), 0),
          new Field("tsltz", Schema.createTimestampLtz(p).setNullable(false), 1)))
          .setNullable(false);
      LogicalType original = new LogicalType(rootSchema);
      AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Holder");
      LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
      Schema ts = roundTripped.getRootSchema().getField("ts").getSchema();
      Schema tsltz = roundTripped.getRootSchema().getField("tsltz").getSchema();
      assertEquals(Schema.Type.TIMESTAMP, ts.getType(), "TIMESTAMP(" + p + ")");
      assertEquals(p, ts.getPrecision(), "TIMESTAMP precision " + p);
      assertEquals(Schema.Type.TIMESTAMP_LTZ, tsltz.getType(), "TIMESTAMP_LTZ(" + p + ")");
      assertEquals(p, tsltz.getPrecision(), "TIMESTAMP_LTZ precision " + p);
    }
  }

  @Test
  void testTimePrecisionOutOfRange() {
    assertThrows(ValidationException.class, () -> {
        Schema.createTime(10);
    });
  }

  @Test
  void testTimestampPrecisionOutOfRange() {
    assertThrows(ValidationException.class, () -> {
        // -1 is NO_PARAM (default sentinel); -2 is genuinely out of range
        Schema.createTimestamp(-2);
    });
  }

  @Test
  void testRoundTripLocalNamedTypeRefPreservesNamespace() {
    // Local NAMED_TYPE_REF whose qualified name has a namespace. The recovered
    // type should keep the full name, not just the Avro simple name.
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("street", Schema.createString().setNullable(false), 0),
        new Field("city", Schema.createString().setNullable(false), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("com.example.Address", addressType);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, namedTypes);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Person");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    Schema home = roundTripped.getRootSchema().getField("home").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, home.getType());
    // Without the getFullName() fix, this would be "Address" (namespace lost).
    assertEquals("com.example.Address", home.getQualifiedName());
    assertNotNull(roundTripped.getNamedTypes().get("com.example.Address"));
  }

  // =========================================================================
  // Defaults on complex types throw
  // =========================================================================

  private static AvroSchema convert(Schema rootSchema) {
    return LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder");
  }

  private static Schema fieldOfTypeWithDefault(Schema fieldType, Object defaultValue) {
    return Schema.createStruct(Arrays.asList(
        new Field("f", fieldType, 0, defaultValue, true, null, null, null)))
        .setNullable(false);
  }

  @Test
  void testDefaultOnArrayThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema arr = Schema.createArray(Schema.create(Schema.Type.INT)).setNullable(false);
        convert(fieldOfTypeWithDefault(arr, Arrays.asList(1, 2, 3)));
    });
  }

  @Test
  void testDefaultOnMapThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema mapType = Schema.createMap(
            Schema.createString().setNullable(false),
            Schema.create(Schema.Type.INT).setNullable(false)).setNullable(false);
        convert(fieldOfTypeWithDefault(mapType, java.util.Map.of("k", 1)));
    });
  }

  @Test
  void testDefaultOnMultisetThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema multisetType = Schema.createMultiset(
            Schema.create(Schema.Type.INT).setNullable(false)).setNullable(false);
        convert(fieldOfTypeWithDefault(multisetType, java.util.Map.of(1, 2)));
    });
  }

  @Test
  void testDefaultOnStructThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema structType = Schema.createStruct(Arrays.asList(
            new Field("inner", Schema.createString().setNullable(false), 0)))
            .setNullable(false);
        convert(fieldOfTypeWithDefault(structType, "anything"));
    });
  }

  @Test
  void testRoundTripEnumDefault() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B"), new EnumValue("C")))
        .setNullable(false);
    Schema rootSchema = fieldOfTypeWithDefault(enumType, "B");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.hasDefaultValue());
    assertEquals("B", f.getDefaultValue());
  }

  @Test
  void testRoundTripNullableEnumWithNonNullDefault() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B"), new EnumValue("C")))
        .setNullable(true);
    Schema rootSchema = fieldOfTypeWithDefault(enumType, "B");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.getSchema().isNullable());
    assertTrue(f.hasDefaultValue());
    assertEquals("B", f.getDefaultValue());
  }

  @Test
  void testRoundTripNullableEnumWithNullDefault() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B")))
        .setNullable(true);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("f", enumType, 0, null, true, null, null, null)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.getSchema().isNullable());
    assertTrue(f.hasDefaultValue());
    assertNull(f.getDefaultValue());
  }

  @Test
  void testDefaultOnVariantThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema variantType = Schema.create(Schema.Type.VARIANT).setNullable(false);
        convert(fieldOfTypeWithDefault(variantType, "anything"));
    });
  }

  @Test
  void testDefaultOnNamedTypeRefThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema addrType = Schema.createStruct(Arrays.asList(
            new Field("city", Schema.createString().setNullable(false), 0)))
            .setNullable(false);
        java.util.Map<String, Schema> namedTypes = new LinkedHashMap<>();
        namedTypes.put("Address", addrType);
        Schema rootSchema = Schema.createStruct(Arrays.asList(
            new Field("a", Schema.createNamedTypeRef("Address").setNullable(false), 0,
                "anything", true, null, null, null)))
            .setNullable(false);
        LogicalTypeToAvroConverter.fromLogicalType(
            new LogicalType(rootSchema, namedTypes), "Holder");
    });
  }

  // =========================================================================
  // MULTISET round-trip
  // =========================================================================

  @Test
  void testRoundTripMultisetOfInt() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("counts",
            Schema.createMultiset(Schema.create(Schema.Type.INT).setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("counts").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.INT, rt.getElementType().getType());
    assertFalse(rt.isNullable());
  }

  @Test
  void testRoundTripMultisetOfString() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("words",
            Schema.createMultiset(Schema.createString().setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("words").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.VARCHAR, rt.getElementType().getType());
  }

  @Test
  void testRoundTripMultisetNullable() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("maybeCounts",
            Schema.createMultiset(Schema.create(Schema.Type.INT).setNullable(false))
                .setNullable(true), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("maybeCounts").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertTrue(rt.isNullable());
  }

  // =========================================================================
  // DECIMAL precision/scale variations
  // =========================================================================

  @Test
  void testRoundTripDecimalVariations() {
    int[][] cases = {{5, 0}, {10, 2}, {38, 18}, {38, 0}};
    for (int[] ps : cases) {
      int precision = ps[0];
      int scale = ps[1];
      Schema rootSchema = Schema.createStruct(Arrays.asList(
          new Field("d", Schema.createDecimal(precision, scale).setNullable(false), 0)))
          .setNullable(false);
      LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
          convert(rootSchema));
      Schema rt = roundTripped.getRootSchema().getField("d").getSchema();
      assertEquals(Schema.Type.DECIMAL, rt.getType());
      assertEquals(precision, rt.getPrecision(), "precision " + precision);
      assertEquals(scale, rt.getScale(), "scale " + scale);
    }
  }

  // =========================================================================
  // Length-constrained primitives round-trip
  // =========================================================================

  @Test
  void testRoundTripCharVarcharLengths() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("c", Schema.createChar(10).setNullable(false), 0),
        new Field("v", Schema.createVarchar(255).setNullable(false), 1)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema c = roundTripped.getRootSchema().getField("c").getSchema();
    Schema v = roundTripped.getRootSchema().getField("v").getSchema();
    assertEquals(Schema.Type.CHAR, c.getType());
    assertEquals(10, c.getLength());
    assertEquals(Schema.Type.VARCHAR, v.getType());
    assertEquals(255, v.getLength());
  }

  @Test
  void testRoundTripBinaryVarbinaryLengths() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("b", Schema.createBinary(8).setNullable(false), 0),
        new Field("vb", Schema.createVarbinary(100).setNullable(false), 1)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema b = roundTripped.getRootSchema().getField("b").getSchema();
    Schema vb = roundTripped.getRootSchema().getField("vb").getSchema();
    assertEquals(Schema.Type.BINARY, b.getType());
    assertEquals(8, b.getLength());
    assertEquals(Schema.Type.VARBINARY, vb.getType());
    assertEquals(100, vb.getLength());
  }

  // =========================================================================
  // Recursive NAMED_TYPE_REF
  // =========================================================================

  @Test
  void testRoundTripRecursiveNamedTypeRef() {
    Schema treeType = Schema.createStruct(Arrays.asList(
        new Field("value", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("child",
            Schema.createNamedTypeRef("Tree").setNullable(true), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Tree", treeType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("root",
            Schema.createNamedTypeRef("Tree").setNullable(false), 0)))
        .setNullable(false);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Schema rootRef = roundTripped.getRootSchema().getField("root").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rootRef.getType());
    assertEquals("Tree", rootRef.getQualifiedName());
    Schema treeDef = roundTripped.getNamedTypes().get("Tree");
    assertNotNull(treeDef);
    Schema childField = treeDef.getField("child").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, childField.getType());
    assertEquals("Tree", childField.getQualifiedName());
  }

  // =========================================================================
  // Nullable complex types
  // =========================================================================

  // =========================================================================
  // ARRAY of complex types
  // =========================================================================

  @Test
  void testRoundTripArrayOfStruct() {
    Schema elementType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("y", Schema.createString().setNullable(false), 1)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("items",
            Schema.createArray(elementType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("items").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.STRUCT, rt.getElementType().getType());
    assertEquals(2, rt.getElementType().getFields().size());
  }

  @Test
  void testRoundTripArrayOfArray() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("matrix",
            Schema.createArray(
                Schema.createArray(
                    Schema.create(Schema.Type.INT).setNullable(false))
                    .setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("matrix").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.ARRAY, rt.getElementType().getType());
    assertEquals(Schema.Type.INT, rt.getElementType().getElementType().getType());
  }

  @Test
  void testRoundTripArrayOfMap() {
    Schema mapType = Schema.createMap(
        Schema.createString().setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(false)).setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("histograms",
            Schema.createArray(mapType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("histograms").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.MAP, rt.getElementType().getType());
  }

  @Test
  void testRoundTripArrayOfNamedTypeRef() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("city", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("addrs",
            Schema.createArray(
                Schema.createNamedTypeRef("Address").setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Schema rt = roundTripped.getRootSchema().getField("addrs").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getElementType().getType());
    assertEquals("Address", rt.getElementType().getQualifiedName());
  }

  // =========================================================================
  // MAP of complex value types
  // =========================================================================

  @Test
  void testRoundTripMapOfStruct() {
    Schema valueType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("byKey",
            Schema.createMap(Schema.createString().setNullable(false), valueType)
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("byKey").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.STRUCT, rt.getValueType().getType());
  }

  @Test
  void testRoundTripMapOfArray() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("groupedItems",
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.createArray(
                    Schema.create(Schema.Type.INT).setNullable(false))
                    .setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("groupedItems").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.ARRAY, rt.getValueType().getType());
  }

  @Test
  void testRoundTripMapOfNamedTypeRef() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("city", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("addrByKey",
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.createNamedTypeRef("Address").setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Schema rt = roundTripped.getRootSchema().getField("addrByKey").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getValueType().getType());
  }

  @Test
  void testRoundTripMapWithIntKeyAndStructValue() {
    Schema valueType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("byId",
            Schema.createMap(
                Schema.create(Schema.Type.INT).setNullable(false), valueType)
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("byId").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.INT, rt.getKeyType().getType());
    assertEquals(Schema.Type.STRUCT, rt.getValueType().getType());
  }

  // =========================================================================
  // Field positions out of order
  // =========================================================================

  @Test
  void testRoundTripFieldIterationOrderPreserved() {
    // Fields are emitted in iteration order (the position arg on Field is
    // informational, not authoritative). A struct constructed with fields in
    // a particular iteration order should round-trip in that same order.
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("c", Schema.createString().setNullable(false), 0),
        new Field("a", Schema.createString().setNullable(false), 1),
        new Field("b", Schema.createString().setNullable(false), 2)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    java.util.List<Field> fields = roundTripped.getRootSchema().getFields();
    assertEquals(3, fields.size());
    assertEquals("c", fields.get(0).getName());
    assertEquals("a", fields.get(1).getName());
    assertEquals("b", fields.get(2).getName());
  }

  // =========================================================================
  // Schema-level metadata at root
  // =========================================================================

  @Test
  void testRoundTripRootSchemaMetadata() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false)
        .setDoc("Root struct doc")
        .setTags(Arrays.asList("PII", "internal"))
        .setParams(java.util.Map.of("owner", "alice"));
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals("Root struct doc", rt.getDoc());
    assertEquals(Arrays.asList("PII", "internal"), rt.getTags());
    assertEquals("alice", rt.getParams().get("owner"));
  }

  // =========================================================================
  // Cross-format collision: duplicate external NAMED_TYPE_REF fullName
  // =========================================================================

  @Test
  void testRoundTripExternalRefDuplicateFullNameThrows() {
    String first = "{\"type\":\"record\",\"name\":\"Address\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{\"name\":\"street\",\"type\":\"string\"}]}";
    String second = "{\"type\":\"record\",\"name\":\"Address\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Address", "addr-a", 1),
        new SchemaReference("com.example.AddressV2", "addr-b", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Address", first);
    resolvedReferences.put("com.example.AddressV2", second);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("addr",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);

    // Avro's own parser throws SchemaParseException for duplicate names; either
    // that or our own ValidationException is acceptable signal of collision.
    assertThrows(Exception.class, () ->
        LogicalTypeToAvroConverter.fromLogicalType(original, "Holder"));
  }

  // =========================================================================
  // Nested external enum NAMED_TYPE_REF
  // =========================================================================

  @Test
  void testRoundTripExternalNestedEnumRef() {
    // External Avro schema with a top-level record containing a nested enum.
    // The LogicalType references the nested enum directly.
    String externalSchema = "{\"type\":\"record\",\"name\":\"Outer\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{\"name\":\"color\",\"type\":{"
        + "\"type\":\"enum\",\"name\":\"Color\","
        + "\"symbols\":[\"RED\",\"GREEN\",\"BLUE\"]}}]}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Outer", "outer-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Outer", externalSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("favorite",
            Schema.createNamedTypeRef("com.example.Color").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Schema fav = roundTripped.getRootSchema().getField("favorite").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, fav.getType());
    assertEquals("com.example.Color", fav.getQualifiedName());
  }

  @Test
  void testRoundTripDefaultNullOnNullablePrimitive() {
    // hasDefault=true, defaultValue=null on a nullable primitive should
    // round-trip via JsonProperties.NULL_VALUE and produce a null-first union.
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("f", Schema.create(Schema.Type.INT).setNullable(true), 0,
            null, true, null, null, null)))
        .setNullable(false);
    AvroSchema avro = convert(rootSchema);
    org.apache.avro.Schema fieldSchema = avro.rawSchema().getField("f").schema();
    assertTrue(fieldSchema.isUnion());
    assertEquals(org.apache.avro.Schema.Type.NULL,
        fieldSchema.getTypes().get(0).getType());
    assertEquals(org.apache.avro.Schema.Type.INT,
        fieldSchema.getTypes().get(1).getType());

    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.hasDefaultValue());
    assertNull(f.getDefaultValue());
  }

  @Test
  void testRoundTripNullableFieldWithNonNullDefault() {
    // hasDefault=true, defaultValue=42 on a nullable INT should emit a
    // non-null-first union ([int, null]) so the default's type matches
    // the first branch, and round-trip preserving both default and nullability.
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("f", Schema.create(Schema.Type.INT).setNullable(true), 0,
            42, true, null, null, null)))
        .setNullable(false);
    AvroSchema avro = convert(rootSchema);
    org.apache.avro.Schema fieldSchema = avro.rawSchema().getField("f").schema();
    assertTrue(fieldSchema.isUnion());
    assertEquals(org.apache.avro.Schema.Type.INT,
        fieldSchema.getTypes().get(0).getType());
    assertEquals(org.apache.avro.Schema.Type.NULL,
        fieldSchema.getTypes().get(1).getType());

    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.getSchema().isNullable());
    assertTrue(f.hasDefaultValue());
    assertEquals(42, f.getDefaultValue());
  }

  // =========================================================================
  // MULTISET inside collections
  // =========================================================================

  @Test
  void testRoundTripArrayOfMultiset() {
    Schema multisetType = Schema.createMultiset(
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("counts",
            Schema.createArray(multisetType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("counts").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.MULTISET, rt.getElementType().getType());
  }

  @Test
  void testRoundTripMapOfMultiset() {
    Schema multisetType = Schema.createMultiset(
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("byKey",
            Schema.createMap(
                Schema.createString().setNullable(false), multisetType)
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("byKey").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.MULTISET, rt.getValueType().getType());
  }

  // =========================================================================
  // Nullable primitive types
  // =========================================================================

  @Test
  void testRoundTripNullablePrimitives() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("nb", Schema.create(Schema.Type.BOOLEAN).setNullable(true), 0),
        new Field("ni", Schema.create(Schema.Type.INT).setNullable(true), 1),
        new Field("nl", Schema.create(Schema.Type.BIGINT).setNullable(true), 2),
        new Field("nf", Schema.create(Schema.Type.FLOAT).setNullable(true), 3),
        new Field("nd", Schema.create(Schema.Type.DOUBLE).setNullable(true), 4),
        new Field("ns", Schema.createString().setNullable(true), 5),
        new Field("nbn", Schema.createBytes().setNullable(true), 6),
        new Field("ndec", Schema.createDecimal(10, 2).setNullable(true), 7),
        new Field("ndt", Schema.create(Schema.Type.DATE).setNullable(true), 8),
        new Field("nts", Schema.createTimestamp(6).setNullable(true), 9)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    for (Field f : rt.getFields()) {
      assertTrue(f.getSchema().isNullable(), "field " + f.getName() + " should be nullable");
    }
  }

  // =========================================================================
  // MULTISET of complex types
  // =========================================================================

  @Test
  void testRoundTripMultisetOfStruct() {
    Schema elementType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("counts",
            Schema.createMultiset(elementType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("counts").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.STRUCT, rt.getElementType().getType());
  }

  @Test
  void testRoundTripMultisetOfNamedTypeRef() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("city", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("addrs",
            Schema.createMultiset(
                Schema.createNamedTypeRef("Address").setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Schema rt = roundTripped.getRootSchema().getField("addrs").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getElementType().getType());
  }

  // =========================================================================
  // VARIANT in non-trivial positions
  // =========================================================================

  @Test
  void testRoundTripVariantInArray() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("vs",
            Schema.createArray(
                Schema.create(Schema.Type.VARIANT).setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("vs").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.VARIANT, rt.getElementType().getType());
  }

  @Test
  void testRoundTripVariantInMap() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("vm",
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.create(Schema.Type.VARIANT).setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("vm").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.VARIANT, rt.getValueType().getType());
  }

  @Test
  void testRoundTripVariantInUnionBranch() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u",
            Schema.createUnion(Arrays.asList(
                new UnionBranch("vbranch",
                    Schema.create(Schema.Type.VARIANT).setNullable(false)),
                new UnionBranch("sbranch",
                    Schema.createString().setNullable(false))))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("u").getSchema();
    assertEquals(Schema.Type.UNION, rt.getType());
    assertEquals(Schema.Type.VARIANT, rt.getBranches().get(0).getSchema().getType());
  }

  // =========================================================================
  // ENUM in non-trivial positions
  // =========================================================================

  @Test
  void testRoundTripEnumInArray() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B"), new EnumValue("C")))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Color", enumType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("colors",
            Schema.createArray(
                Schema.createNamedTypeRef("Color").setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Schema rt = roundTripped.getRootSchema().getField("colors").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getElementType().getType());
  }

  @Test
  void testRoundTripEnumInMapValue() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new EnumValue("RED"), new EnumValue("GREEN")))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Color", enumType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("colorMap",
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.createNamedTypeRef("Color").setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Schema rt = roundTripped.getRootSchema().getField("colorMap").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getValueType().getType());
  }

  @Test
  void testRoundTripEnumInUnionBranch() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new EnumValue("YES"), new EnumValue("NO")))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Answer", enumType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u",
            Schema.createUnion(Arrays.asList(
                new UnionBranch("ebranch",
                    Schema.createNamedTypeRef("Answer").setNullable(false)),
                new UnionBranch("sbranch",
                    Schema.createString().setNullable(false))))
                .setNullable(false), 0)))
        .setNullable(false);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Schema rt = roundTripped.getRootSchema().getField("u").getSchema();
    assertEquals(Schema.Type.UNION, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        rt.getBranches().get(0).getSchema().getType());
  }

  // =========================================================================
  // UNION nested in collections
  // =========================================================================

  @Test
  void testRoundTripUnionInArray() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new UnionBranch("a", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("us",
            Schema.createArray(unionType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("us").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.UNION, rt.getElementType().getType());
    assertEquals(2, rt.getElementType().getBranches().size());
  }

  @Test
  void testRoundTripUnionInMapValue() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new UnionBranch("a", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("um",
            Schema.createMap(
                Schema.createString().setNullable(false), unionType)
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("um").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.UNION, rt.getValueType().getType());
  }

  @Test
  void testRoundTripNullableComplexTypes() {
    Schema namedTypeRef = Schema.createNamedTypeRef("Inner").setNullable(true);
    java.util.Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Inner", Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false));
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("nullableArr",
            Schema.createArray(Schema.create(Schema.Type.INT).setNullable(false))
                .setNullable(true), 0),
        new Field("nullableMap",
            Schema.createMap(Schema.createString().setNullable(false),
                Schema.create(Schema.Type.INT).setNullable(false)).setNullable(true), 1),
        new Field("nullableStruct",
            Schema.createStruct(Arrays.asList(
                new Field("s", Schema.createString().setNullable(false), 0)))
                .setNullable(true), 2),
        new Field("nullableRef", namedTypeRef, 3)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        LogicalTypeToAvroConverter.fromLogicalType(
            new LogicalType(rootSchema, namedTypes), "Holder"));
    Schema rt = roundTripped.getRootSchema();
    assertTrue(rt.getField("nullableArr").getSchema().isNullable());
    assertTrue(rt.getField("nullableMap").getSchema().isNullable());
    assertTrue(rt.getField("nullableStruct").getSchema().isNullable());
    assertTrue(rt.getField("nullableRef").getSchema().isNullable());
  }

  // =========================================================================
  // TINYINT / SMALLINT round-trip
  // =========================================================================

  @Test
  void testRoundTripTinyintSmallint() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("tiny", Schema.create(Schema.Type.TINYINT).setNullable(false), 0),
        new Field("small", Schema.create(Schema.Type.SMALLINT).setNullable(false), 1)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Schema.Type.TINYINT, rt.getField("tiny").getSchema().getType());
    assertEquals(Schema.Type.SMALLINT, rt.getField("small").getSchema().getType());
  }

  // =========================================================================
  // Exhaustive primitives (every Schema.Type primitive)
  // =========================================================================

  @Test
  void testRoundTripPrimitivesExhaustive() {
    Schema[] primitives = new Schema[] {
        Schema.create(Schema.Type.BOOLEAN).setNullable(false),
        Schema.create(Schema.Type.TINYINT).setNullable(false),
        Schema.create(Schema.Type.SMALLINT).setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(false),
        Schema.create(Schema.Type.BIGINT).setNullable(false),
        Schema.create(Schema.Type.FLOAT).setNullable(false),
        Schema.create(Schema.Type.DOUBLE).setNullable(false),
        Schema.createDecimal(15, 5).setNullable(false),
        Schema.createString().setNullable(false),
        Schema.createVarchar(50).setNullable(false),
        Schema.createChar(10).setNullable(false),
        Schema.createBytes().setNullable(false),
        Schema.createVarbinary(50).setNullable(false),
        Schema.createBinary(10).setNullable(false),
        Schema.create(Schema.Type.DATE).setNullable(false),
        Schema.createTime(3).setNullable(false),
        Schema.createTimestamp(6).setNullable(false),
        Schema.createTimestampLtz(6).setNullable(false),
    };
    for (Schema p : primitives) {
      Schema original = Schema.createStruct(Arrays.asList(
          new Field("f", p, 0))).setNullable(false);
      Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(convert(original));
      assertEquals(original, roundTripped, "Round trip failed for " + p.getType());
    }
  }

  // =========================================================================
  // Default-value round-trip — all primitive types
  // =========================================================================

  @Test
  void testRoundTripDefaultValuesAllPrimitives() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("b", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 0,
            Boolean.TRUE, true, null, null, null),
        new Field("ti", Schema.create(Schema.Type.TINYINT).setNullable(false), 1,
            (byte) 7, true, null, null, null),
        new Field("si", Schema.create(Schema.Type.SMALLINT).setNullable(false), 2,
            (short) 1234, true, null, null, null),
        new Field("i", Schema.create(Schema.Type.INT).setNullable(false), 3,
            42, true, null, null, null),
        new Field("bi", Schema.create(Schema.Type.BIGINT).setNullable(false), 4,
            123456789012L, true, null, null, null),
        new Field("fl", Schema.create(Schema.Type.FLOAT).setNullable(false), 5,
            1.5f, true, null, null, null),
        new Field("db", Schema.create(Schema.Type.DOUBLE).setNullable(false), 6,
            2.5d, true, null, null, null),
        new Field("ch", Schema.createChar(10).setNullable(false), 7,
            "hello", true, null, null, null),
        new Field("vc", Schema.createString().setNullable(false), 8,
            "world", true, null, null, null),
        new Field("bin", Schema.createBinary(3).setNullable(false), 9,
            new byte[]{1, 2, 3}, true, null, null, null),
        new Field("vb", Schema.createVarbinary(10).setNullable(false), 10,
            new byte[]{4, 5, 6}, true, null, null, null),
        new Field("dec", Schema.createDecimal(10, 2).setNullable(false), 11,
            new java.math.BigDecimal("12.34"), true, null, null, null),
        new Field("dt", Schema.create(Schema.Type.DATE).setNullable(false), 12,
            java.time.LocalDate.of(2026, 4, 17), true, null, null, null),
        new Field("tm", Schema.createTime(3).setNullable(false), 13,
            java.time.LocalTime.of(12, 34, 56), true, null, null, null),
        new Field("ts", Schema.createTimestamp(6).setNullable(false), 14,
            java.time.LocalDateTime.of(2026, 4, 17, 12, 34, 56), true, null, null, null),
        new Field("tsltz", Schema.createTimestampLtz(3).setNullable(false), 15,
            java.time.Instant.ofEpochMilli(1700000000000L), true, null, null, null)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        LogicalTypeToAvroConverter.fromLogicalType(original, "Holder"));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Boolean.TRUE, rt.getField("b").getDefaultValue());
    assertEquals((byte) 7, rt.getField("ti").getDefaultValue());
    assertEquals((short) 1234, rt.getField("si").getDefaultValue());
    assertEquals(42, rt.getField("i").getDefaultValue());
    assertEquals(123456789012L, rt.getField("bi").getDefaultValue());
    assertEquals(1.5f, rt.getField("fl").getDefaultValue());
    assertEquals(2.5d, rt.getField("db").getDefaultValue());
    assertEquals("hello", rt.getField("ch").getDefaultValue());
    assertEquals("world", rt.getField("vc").getDefaultValue());
    assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) rt.getField("bin").getDefaultValue());
    assertArrayEquals(new byte[]{4, 5, 6}, (byte[]) rt.getField("vb").getDefaultValue());
    assertEquals(new java.math.BigDecimal("12.34"), rt.getField("dec").getDefaultValue());
    assertEquals(java.time.LocalDate.of(2026, 4, 17), rt.getField("dt").getDefaultValue());
    assertEquals(java.time.LocalTime.of(12, 34, 56), rt.getField("tm").getDefaultValue());
    assertEquals(java.time.LocalDateTime.of(2026, 4, 17, 12, 34, 56),
        rt.getField("ts").getDefaultValue());
    assertEquals(java.time.Instant.ofEpochMilli(1700000000000L),
        rt.getField("tsltz").getDefaultValue());
  }

  @Test
  void testRoundTripDefaultValuesNumericEdges() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("imin", Schema.create(Schema.Type.INT).setNullable(false), 0,
            Integer.MIN_VALUE, true, null, null, null),
        new Field("imax", Schema.create(Schema.Type.INT).setNullable(false), 1,
            Integer.MAX_VALUE, true, null, null, null),
        new Field("lmin", Schema.create(Schema.Type.BIGINT).setNullable(false), 2,
            Long.MIN_VALUE, true, null, null, null),
        new Field("lmax", Schema.create(Schema.Type.BIGINT).setNullable(false), 3,
            Long.MAX_VALUE, true, null, null, null),
        new Field("negDec", Schema.createDecimal(12, 4).setNullable(false), 4,
            new java.math.BigDecimal("-12345.6789"), true, null, null, null),
        new Field("smallDec", Schema.createDecimal(8, 6).setNullable(false), 5,
            new java.math.BigDecimal("0.000001"), true, null, null, null)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        LogicalTypeToAvroConverter.fromLogicalType(original, "Holder"));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Integer.MIN_VALUE, rt.getField("imin").getDefaultValue());
    assertEquals(Integer.MAX_VALUE, rt.getField("imax").getDefaultValue());
    assertEquals(Long.MIN_VALUE, rt.getField("lmin").getDefaultValue());
    assertEquals(Long.MAX_VALUE, rt.getField("lmax").getDefaultValue());
    assertEquals(new java.math.BigDecimal("-12345.6789"),
        rt.getField("negDec").getDefaultValue());
    assertEquals(new java.math.BigDecimal("0.000001"),
        rt.getField("smallDec").getDefaultValue());
  }

  // =========================================================================
  // Map with various non-string key types
  // =========================================================================

  @Test
  void testRoundTripMapWithBigintKey() {
    Schema mapType = Schema.createMap(
        Schema.create(Schema.Type.BIGINT).setNullable(false),
        Schema.createString().setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapType, 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("m").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.BIGINT, rt.getKeyType().getType());
  }

  @Test
  void testRoundTripMapWithBooleanKey() {
    Schema mapType = Schema.createMap(
        Schema.create(Schema.Type.BOOLEAN).setNullable(false),
        Schema.createString().setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapType, 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("m").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.BOOLEAN, rt.getKeyType().getType());
  }

  @Test
  void testRoundTripMapWithBytesKey() {
    Schema mapType = Schema.createMap(
        Schema.createBytes().setNullable(false),
        Schema.createString().setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapType, 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("m").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.VARBINARY, rt.getKeyType().getType());
  }

  // =========================================================================
  // Multi-level external references (root -> external A -> external B)
  // =========================================================================

  @Test
  void testRoundTripMultiLevelExternalRefs() {
    // External B defines com.example.Foo
    String externalB = "{\"type\":\"record\",\"name\":\"Foo\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
    // External A references B's type by full name
    String externalA = "{\"type\":\"record\",\"name\":\"Bar\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{\"name\":\"foo\",\"type\":\"com.example.Foo\"}]}";

    // Dependency order matters: Foo must be registered before Bar so the
    // AvroSchema parser can resolve com.example.Foo when parsing Bar.
    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Foo", "foo-value", 1),
        new SchemaReference("com.example.Bar", "bar-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Foo", externalB);
    resolvedReferences.put("com.example.Bar", externalA);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("bar",
            Schema.createNamedTypeRef("com.example.Bar").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);
    Schema barRef = roundTripped.getRootSchema().getField("bar").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, barRef.getType());
    assertEquals("com.example.Bar", barRef.getQualifiedName());
  }

  // =========================================================================
  // Doc/tags/params on STRUCT used as a union branch
  // =========================================================================

  @Test
  void testRoundTripStructInUnionBranchPreservesMetadata() {
    Map<String, Object> innerParams = new LinkedHashMap<>();
    innerParams.put("k", "v");
    Schema innerStruct = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false)
        .setDoc("inner doc")
        .setTags(Arrays.asList("inner-tag"))
        .setParams(innerParams);
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("a", innerStruct),
        new UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rtBranch = roundTripped.getRootSchema().getField("u").getSchema()
        .getBranches().get(0).getSchema();
    assertEquals(Schema.Type.STRUCT, rtBranch.getType());
    assertEquals("inner doc", rtBranch.getDoc());
    assertEquals(Arrays.asList("inner-tag"), rtBranch.getTags());
    assertEquals("v", rtBranch.getParams().get("k"));
  }

  // =========================================================================
  // VARIANT at root
  // =========================================================================

  @Test
  void testRoundTripVariantAtRoot() {
    Schema rootVariant = Schema.create(Schema.Type.VARIANT).setNullable(false);
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(
        LogicalTypeToAvroConverter.fromLogicalType(new LogicalType(rootVariant), "Holder"));
    assertEquals(Schema.Type.VARIANT, roundTripped.getType());
    assertFalse(roundTripped.isNullable());
  }

  // =========================================================================
  // Tags + params combined on the same field/struct
  // =========================================================================

  @Test
  void testRoundTripFieldWithBothTagsAndParams() {
    Map<String, Object> fieldParams = new LinkedHashMap<>();
    fieldParams.put("sensitivity", "high");
    fieldParams.put("owner", "alice");
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("email", Schema.createString().setNullable(false), 0,
            null, false, null, Arrays.asList("PII", "EMAIL"), fieldParams)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Field f = roundTripped.getRootSchema().getField("email");
    assertEquals(Arrays.asList("PII", "EMAIL"), f.getTags());
    assertEquals("high", f.getParams().get("sensitivity"));
    assertEquals("alice", f.getParams().get("owner"));
  }

  @Test
  void testRoundTripStructWithBothSchemaTagsAndParams() {
    Map<String, Object> schemaParams = new LinkedHashMap<>();
    schemaParams.put("version", "2");
    schemaParams.put("source", "api");
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false)
        .setTags(Arrays.asList("PII", "INTERNAL"))
        .setParams(schemaParams);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Arrays.asList("PII", "INTERNAL"), rt.getTags());
    assertEquals("2", rt.getParams().get("version"));
    assertEquals("api", rt.getParams().get("source"));
  }

  // =========================================================================
  // Defaults on UNION-typed fields
  // =========================================================================

  @Test
  void testRoundTripUnionDefault() {
    // Default value must match the FIRST branch's type (Avro spec).
    Schema unionType = Schema.createUnion(Arrays.asList(
        new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", unionType, 0, 5, true, null, null, null)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        LogicalTypeToAvroConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder"));
    Field f = roundTripped.getRootSchema().getField("u");
    assertTrue(f.hasDefaultValue());
    assertEquals(5, f.getDefaultValue());
  }

  @Test
  void testRoundTripNullableUnionWithNonNullDefault() {
    // Exercises maybeMakeNullable's union-splice path with a non-null default.
    // Result wire-form should be [int, string, null] (default-matching branch
    // first, null appended).
    Schema unionType = Schema.createUnion(Arrays.asList(
        new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(true);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", unionType, 0, 5, true, null, null, null)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        LogicalTypeToAvroConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder"));
    Field f = roundTripped.getRootSchema().getField("u");
    assertTrue(f.getSchema().isNullable());
    assertTrue(f.hasDefaultValue());
    assertEquals(5, f.getDefaultValue());
    assertEquals(2, f.getSchema().getBranches().size());
  }

  // =========================================================================
  // MAP with union-typed key
  // =========================================================================

  @Test
  void testRoundTripMapWithUnionKey() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema mapType = Schema.createMap(
        unionType, Schema.createString().setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapType, 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("m").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.UNION, rt.getKeyType().getType());
    assertEquals(2, rt.getKeyType().getBranches().size());
    assertEquals(Schema.Type.VARCHAR, rt.getValueType().getType());
  }

  // =========================================================================
  // Multiset of union (multiset-of-struct and multiset-of-named-type-ref
  // already covered above)
  // =========================================================================

  @Test
  void testRoundTripMultisetOfUnion() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("ms",
            Schema.createMultiset(unionType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("ms").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.UNION, rt.getElementType().getType());
    assertEquals(2, rt.getElementType().getBranches().size());
  }

  // =========================================================================
  // Namespace round-trip
  // =========================================================================

  @Test
  void testRoundTripNamespace() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    LogicalType original = new LogicalType("com.example", rootSchema, Map.of(),
        List.of(), Map.of());
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        LogicalTypeToAvroConverter.fromLogicalType(original, "Holder"));
    assertEquals("com.example", roundTripped.getNamespace());
  }

  @Test
  void testRoundTripNoNamespaceIsNull() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    LogicalType original = new LogicalType(rootSchema);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        LogicalTypeToAvroConverter.fromLogicalType(original, "Holder"));
    assertNull(roundTripped.getNamespace());
  }

  // =========================================================================
  // Reserved-keyword field names
  // =========================================================================

  /**
   * The LT Schema model accepts arbitrary strings as field names; the format
   * converters operate on Schema objects, not DDL text. Field names that
   * happen to match reserved words in Java/Avro/Proto/JSON (class, enum,
   * default, package, type) should round-trip cleanly through the converter.
   */
  @Test
  void testRoundTripReservedKeywordFieldNames() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("class", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("enum", Schema.createString().setNullable(false), 1),
        new Field("default", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 2),
        new Field("package", Schema.createString().setNullable(false), 3),
        new Field("type", Schema.create(Schema.Type.BIGINT).setNullable(false), 4)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(5, rt.getFields().size());
    assertEquals(Schema.Type.INT, rt.getField("class").getSchema().getType());
    assertEquals(Schema.Type.VARCHAR, rt.getField("enum").getSchema().getType());
    assertEquals(Schema.Type.BOOLEAN, rt.getField("default").getSchema().getType());
    assertEquals(Schema.Type.VARCHAR, rt.getField("package").getSchema().getType());
    assertEquals(Schema.Type.BIGINT, rt.getField("type").getSchema().getType());
  }

  // =========================================================================
  // Field.position round-trip
  // =========================================================================

  @Test
  void testRoundTripFieldPosition() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("a", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("b", Schema.createString().setNullable(false), 1),
        new Field("c", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 2)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(0, rt.getField("a").getPosition());
    assertEquals(1, rt.getField("b").getPosition());
    assertEquals(2, rt.getField("c").getPosition());
  }

  // =========================================================================
  // Mutually recursive named types (A -> B -> A)
  // =========================================================================

  @Test
  void testRoundTripMutuallyRecursiveNamedTypes() {
    Schema typeA = Schema.createStruct(Arrays.asList(
        new Field("aVal", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("toB",
            Schema.createNamedTypeRef("B").setNullable(true), 1)))
        .setNullable(false);
    Schema typeB = Schema.createStruct(Arrays.asList(
        new Field("bVal", Schema.createString().setNullable(false), 0),
        new Field("toA",
            Schema.createNamedTypeRef("A").setNullable(true), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("A", typeA);
    namedTypes.put("B", typeB);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("root",
            Schema.createNamedTypeRef("A").setNullable(false), 0)))
        .setNullable(false);
    AvroSchema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(avro);

    Schema rootRef = roundTripped.getRootSchema().getField("root").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rootRef.getType());
    assertEquals("A", rootRef.getQualifiedName());

    Schema rtA = roundTripped.getNamedTypes().get("A");
    assertNotNull(rtA);
    Schema toBRef = rtA.getField("toB").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, toBRef.getType());
    assertEquals("B", toBRef.getQualifiedName());

    Schema rtB = roundTripped.getNamedTypes().get("B");
    assertNotNull(rtB);
    Schema toARef = rtB.getField("toA").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, toARef.getType());
    assertEquals("A", toARef.getQualifiedName());
  }

  // =========================================================================
  // Field-level doc round-trip
  // =========================================================================

  @Test
  void testRoundTripFieldDoc() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("a", Schema.createString().setNullable(false), 0,
            null, false, "name of the user", null, null),
        new Field("b", Schema.create(Schema.Type.INT).setNullable(false), 1,
            null, false, "age in years", null, null)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals("name of the user", rt.getField("a").getDoc());
    assertEquals("age in years", rt.getField("b").getDoc());
  }

  // =========================================================================
  // Empty + single-field struct
  // =========================================================================

  @Test
  void testEmptyStructRoundTrip() {
    // Avro permits records with zero fields; verify it round-trips.
    Schema empty = Schema.createStruct(Collections.emptyList()).setNullable(false);
    Schema roundTripped = AvroToLogicalTypeConverter.toRootSchema(convert(empty));
    assertEquals(Schema.Type.STRUCT, roundTripped.getType());
    assertTrue(roundTripped.getFields().isEmpty());
  }

  @Test
  void testRoundTripSingleFieldStruct() {
    Schema single = Schema.createStruct(Arrays.asList(
        new Field("only", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Schema rt = AvroToLogicalTypeConverter.toRootSchema(convert(single));
    assertEquals(1, rt.getFields().size());
    assertEquals("only", rt.getFields().get(0).getName());
    assertEquals(Schema.Type.VARCHAR, rt.getFields().get(0).getSchema().getType());
  }

  // =========================================================================
  // Multi-branch unions (>2 branches)
  // =========================================================================

  @Test
  void testRoundTripUnionWithManyBranches() {
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("s", Schema.createString().setNullable(false)),
        new UnionBranch("b", Schema.create(Schema.Type.BOOLEAN).setNullable(false)),
        new UnionBranch("d", Schema.create(Schema.Type.DOUBLE).setNullable(false)),
        new UnionBranch("by", Schema.createBytes().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("u").getSchema();
    assertEquals(Schema.Type.UNION, rt.getType());
    assertEquals(5, rt.getBranches().size());
    assertEquals("i", rt.getBranches().get(0).getName());
    assertEquals("s", rt.getBranches().get(1).getName());
    assertEquals("b", rt.getBranches().get(2).getName());
    assertEquals("d", rt.getBranches().get(3).getName());
    assertEquals("by", rt.getBranches().get(4).getName());
  }

  // =========================================================================
  // Deeply nested type combinations (3 levels)
  // =========================================================================

  @Test
  void testRoundTripDeeplyNestedArrayMapStruct() {
    Schema inner = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema mapType = Schema.createMap(
        Schema.createString().setNullable(false), inner).setNullable(false);
    Schema arrayOfMap = Schema.createArray(mapType).setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("nested", arrayOfMap, 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("nested").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.MAP, rt.getElementType().getType());
    assertEquals(Schema.Type.STRUCT, rt.getElementType().getValueType().getType());
    assertEquals(Schema.Type.INT,
        rt.getElementType().getValueType().getField("x").getSchema().getType());
  }

  // =========================================================================
  // Nested-struct doc/tags/params round-trip
  // =========================================================================

  @Test
  void testRoundTripNestedStructDocTagsParams() {
    Map<String, Object> innerParams = new LinkedHashMap<>();
    innerParams.put("inner-key", "inner-val");
    Schema inner = Schema.createStruct(Arrays.asList(
        new Field("v", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false)
        .setDoc("inner doc")
        .setTags(Arrays.asList("inner-tag"))
        .setParams(innerParams);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("inner", inner, 0)))
        .setNullable(false);
    LogicalType roundTripped = AvroToLogicalTypeConverter.toLogicalType(
        convert(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("inner").getSchema();
    assertEquals("inner doc", rt.getDoc());
    assertEquals(Arrays.asList("inner-tag"), rt.getTags());
    assertEquals("inner-val", rt.getParams().get("inner-key"));
  }

  // =========================================================================
  // Multiple non-string-keyed maps in one schema
  //
  // Each map gets a unique entry-record name (synthesized from the parent +
  // field path). The marker is the LogicalMap logical type on the array, so
  // entry records can have any name without collision.
  // =========================================================================

  @Test
  void testRoundTripMultipleNonStringKeyMaps() {
    Schema m1 = Schema.createMap(
        Schema.create(Schema.Type.INT).setNullable(false),
        Schema.createString().setNullable(false))
        .setNullable(false);
    Schema m2 = Schema.createMap(
        Schema.create(Schema.Type.INT).setNullable(false),
        Schema.create(Schema.Type.BOOLEAN).setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m1", m1, 0),
        new Field("m2", m2, 1))).setNullable(false);
    LogicalType rt = AvroToLogicalTypeConverter.toLogicalType(convert(rootSchema));
    Schema rtRoot = rt.getRootSchema();
    Schema rtm1 = rtRoot.getField("m1").getSchema();
    Schema rtm2 = rtRoot.getField("m2").getSchema();
    assertEquals(Schema.Type.MAP, rtm1.getType());
    assertEquals(Schema.Type.INT, rtm1.getKeyType().getType());
    assertEquals(Schema.Type.VARCHAR, rtm1.getValueType().getType());
    assertEquals(Schema.Type.MAP, rtm2.getType());
    assertEquals(Schema.Type.INT, rtm2.getKeyType().getType());
    assertEquals(Schema.Type.BOOLEAN, rtm2.getValueType().getType());
  }

  // =========================================================================
  // V1 emission mode (Flink-compatible)
  // =========================================================================

  @Test
  void testV1NonStringKeyMapUsesCanonicalMapEntry() {
    Schema mapField = Schema.createMap(
        Schema.create(Schema.Type.INT).setNullable(false),
        Schema.createString().setNullable(false)).setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapField, 0))).setNullable(false);
    org.apache.avro.Schema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1).rawSchema();
    org.apache.avro.Schema entry = avro.getField("m").schema().getElementType();
    // V1: canonical name `io.confluent.connect.avro.MapEntry`, no LogicalMap.
    assertEquals("MapEntry", entry.getName());
    assertEquals("io.confluent.connect.avro", entry.getNamespace());
    assertNull(avro.getField("m").schema().getLogicalType());
    // No connect.internal.type prop in V1 (only in V2 to disambiguate from
    // the V1 form using non-canonical names).
    assertNull(entry.getProp("connect.internal.type"));
  }

  @Test
  void testV1ThrowsOnUnion() {
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("a", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0))).setNullable(false);
    ValidationException ex = assertThrows(ValidationException.class, () ->
        LogicalTypeToAvroConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1));
    assertTrue(ex.getMessage().contains("UNION"));
  }

  @Test
  void testV1ThrowsOnVariant() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("v", Schema.create(Schema.Type.VARIANT).setNullable(false), 0)))
        .setNullable(false);
    ValidationException ex = assertThrows(ValidationException.class, () ->
        LogicalTypeToAvroConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1));
    assertTrue(ex.getMessage().contains("VARIANT"));
  }

  @Test
  void testV1ThrowsOnTimestampPrecision7() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("ts", Schema.createTimestampLtz(7).setNullable(false), 0)))
        .setNullable(false);
    ValidationException ex = assertThrows(ValidationException.class, () ->
        LogicalTypeToAvroConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1));
    assertTrue(ex.getMessage().contains("precision > 6"));
  }

  @Test
  void testV1SkipsConfluentTagsAndParams() {
    Map<String, Object> userParams = new LinkedHashMap<>();
    userParams.put("k", "v");
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false)
        .setTags(Arrays.asList("PII"))
        .setParams(userParams);
    org.apache.avro.Schema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1).rawSchema();
    assertNull(avro.getObjectProp("confluent:tags"));
    assertNull(avro.getObjectProp("confluent:params"));
  }

  @Test
  void testEditionMetadataV1AndV2() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    AvroSchema v1 = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1);
    AvroSchema v2 = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V2);
    assertEquals("1", v1.metadata().getProperties().get("confluent:edition"));
    assertEquals("2", v2.metadata().getProperties().get("confluent:edition"));
  }

  /**
   * {@code logical.*} keys are emitted as top-level schema properties, not nested
   * inside {@code confluent:params}. Anonymous-struct case: a STRUCT nested inside
   * a parent surfaces as inline (not a NAMED_TYPE_REF), and the marker lives at
   * the schema's top level.
   */
  @Test
  void testLogicalAnonymousAtTopLevel() {
    Schema inlineStruct = Schema.createStruct(Arrays.asList(
        new Field("a", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("nested", inlineStruct, 0)))
        .setNullable(false);
    org.apache.avro.Schema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V2).rawSchema();

    org.apache.avro.Schema nestedRecord = avro.getField("nested").schema();
    assertEquals("true",
        nestedRecord.getObjectProp(CommonConstants.LOGICAL_ANONYMOUS_PROP),
        "logical.anonymous must be a top-level prop on the nested record");
    assertNull(nestedRecord.getObjectProp("confluent:params"),
        "confluent:params must not be emitted when only logical.* metadata exists");
  }

  /**
   * {@code logical.key.length} / {@code logical.key.type} for a CHAR-keyed MAP are
   * emitted as top-level schema properties on the map, not nested.
   */
  @Test
  void testLogicalMapKeyMetadataAtTopLevel() {
    Schema mapSchema = Schema.createMap(
        Schema.createChar(8).setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(true)).setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapSchema, 0)))
        .setNullable(false);
    org.apache.avro.Schema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V2).rawSchema();

    org.apache.avro.Schema avroMap = avro.getField("m").schema();
    assertEquals(8, avroMap.getObjectProp(CommonConstants.LOGICAL_KEY_LENGTH_PROP),
        "logical.key.length must be a top-level prop on the map schema");
    assertEquals("CHAR", avroMap.getObjectProp(CommonConstants.LOGICAL_KEY_TYPE_PROP),
        "logical.key.type must be a top-level prop on the map schema");
    assertNull(avroMap.getObjectProp("confluent:params"),
        "confluent:params must not be emitted when only logical.* metadata exists");
  }

  /**
   * The dotted-nesting convention is a proto-only structural concern. For Avro,
   * a localNamedType keyed {@code Outer.Inner} just becomes a flat top-level
   * record whose full name is {@code Outer.Inner} (split by Avro into
   * namespace {@code Outer} + simple name {@code Inner}). No nesting structure
   * is invented in the Avro output.
   */
  @Test
  void testNestedDottedNameEmittedAsFlatRecord() {
    Schema innerType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema outerType = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0),
        new Field("inner",
            Schema.createNamedTypeRef("Outer.Inner").setNullable(false), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Outer", outerType);
    namedTypes.put("Outer.Inner", innerType);

    Schema root = Schema.createNamedTypeRef("Outer").setNullable(false);
    org.apache.avro.Schema avro = LogicalTypeToAvroConverter.fromLogicalType(
        new LogicalType(root, namedTypes), "ignored").rawSchema();

    // Outer is the root record; nothing is nested at the Avro layer.
    assertEquals(org.apache.avro.Schema.Type.RECORD, avro.getType());
    assertEquals("Outer", avro.getFullName());
    // The nested-named type's full name carries the dot through Avro's
    // namespace/name split.
    org.apache.avro.Schema innerAvro = avro.getField("inner").schema();
    assertEquals("Outer.Inner", innerAvro.getFullName());
    assertEquals("Outer", innerAvro.getNamespace());
    assertEquals("Inner", innerAvro.getName());
  }
}
