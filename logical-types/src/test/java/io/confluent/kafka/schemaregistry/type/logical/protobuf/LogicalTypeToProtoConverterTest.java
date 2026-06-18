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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
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

class LogicalTypeToProtoConverterTest {

  @Test
  void testSimplePrimitives() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("bool_field", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 0),
        new Field("int_field", Schema.create(Schema.Type.INT).setNullable(false), 1),
        new Field("long_field", Schema.create(Schema.Type.BIGINT).setNullable(false), 2),
        new Field("float_field", Schema.create(Schema.Type.FLOAT).setNullable(false), 3),
        new Field("double_field", Schema.create(Schema.Type.DOUBLE).setNullable(false), 4),
        new Field("string_field", Schema.createString().setNullable(false), 5),
        new Field("bytes_field", Schema.createBytes().setNullable(false), 6)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();
    assertNotNull(descriptor);
    assertEquals("TestMessage", descriptor.getName());
    assertEquals(7, descriptor.getFields().size());

    assertEquals(FieldDescriptor.Type.BOOL, descriptor.findFieldByName("bool_field").getType());
    assertEquals(FieldDescriptor.Type.INT32, descriptor.findFieldByName("int_field").getType());
    assertEquals(FieldDescriptor.Type.INT64, descriptor.findFieldByName("long_field").getType());
    assertEquals(FieldDescriptor.Type.FLOAT, descriptor.findFieldByName("float_field").getType());
    assertEquals(FieldDescriptor.Type.DOUBLE,
        descriptor.findFieldByName("double_field").getType());
    assertEquals(FieldDescriptor.Type.STRING,
        descriptor.findFieldByName("string_field").getType());
    assertEquals(FieldDescriptor.Type.BYTES, descriptor.findFieldByName("bytes_field").getType());
  }

  @Test
  void testEnumField() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("color",
            Schema.createEnum(Arrays.asList(
                new EnumValue("RED"),
                new EnumValue("GREEN"),
                new EnumValue("BLUE")))
                .setNullable(false),
            0)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();
    FieldDescriptor colorField = descriptor.findFieldByName("color");
    assertNotNull(colorField);
    assertEquals(FieldDescriptor.Type.ENUM, colorField.getType());
    assertEquals(3, colorField.getEnumType().getValues().size());
    assertEquals("RED", colorField.getEnumType().getValues().get(0).getName());
    assertEquals("GREEN", colorField.getEnumType().getValues().get(1).getName());
    assertEquals("BLUE", colorField.getEnumType().getValues().get(2).getName());
  }

  @Test
  void testUnionToOneof() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("id",
            Schema.createUnion(Arrays.asList(
                new UnionBranch("str_id",
                    Schema.createString().setNullable(true)),
                new UnionBranch("int_id",
                    Schema.create(Schema.Type.INT).setNullable(true))))
                .setNullable(true),
            0)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();

    // Should have a oneof named "id"
    List<OneofDescriptor> oneofs = descriptor.getRealOneofs();
    assertEquals(1, oneofs.size());
    assertEquals("id", oneofs.get(0).getName());

    // Oneof should have two fields
    List<FieldDescriptor> oneofFields = oneofs.get(0).getFields();
    assertEquals(2, oneofFields.size());
    assertEquals("str_id", oneofFields.get(0).getName());
    assertEquals(FieldDescriptor.Type.STRING, oneofFields.get(0).getType());
    assertEquals("int_id", oneofFields.get(1).getName());
    assertEquals(FieldDescriptor.Type.INT32, oneofFields.get(1).getType());
  }

  @Test
  void testArrayField() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("tags",
            Schema.createArray(
                Schema.createString().setNullable(false))
                .setNullable(false),
            0)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();
    FieldDescriptor tagsField = descriptor.findFieldByName("tags");
    assertNotNull(tagsField);
    assertTrue(tagsField.isRepeated());
    assertEquals(FieldDescriptor.Type.STRING, tagsField.getType());
  }

  @Test
  void testNestedStruct() {
    Schema inner = Schema.createStruct(Arrays.asList(
        new Field("value", Schema.createString().setNullable(false), 0)))
        .setNullable(true);
    Schema outer = Schema.createStruct(Arrays.asList(
        new Field("inner", inner, 0)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(outer), "TestMessage").toDescriptor();
    FieldDescriptor innerField = descriptor.findFieldByName("inner");
    assertNotNull(innerField);
    assertEquals(FieldDescriptor.Type.MESSAGE, innerField.getType());

    Descriptor innerDescriptor = innerField.getMessageType();
    assertNotNull(innerDescriptor.findFieldByName("value"));
  }

  @Test
  void testRoundTripUnionBranchNames() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("id",
            Schema.createUnion(Arrays.asList(
                new UnionBranch("str_id",
                    Schema.createString().setNullable(true)),
                new UnionBranch("int_id",
                    Schema.create(Schema.Type.INT).setNullable(true))))
                .setNullable(true),
            0)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();
    Schema roundTripped = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(descriptor.getFile()));

    Schema.Field idField = roundTripped.getField("id");
    assertEquals(Schema.Type.UNION, idField.getSchema().getType());
    assertEquals("str_id", idField.getSchema().getBranches().get(0).getName());
    assertEquals("int_id", idField.getSchema().getBranches().get(1).getName());
  }

  @Test
  void testRoundTripUnionBranchDoc() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("id",
            Schema.createUnion(Arrays.asList(
                new UnionBranch("str_id",
                    Schema.createString().setNullable(true),
                    "string identifier", null),
                new UnionBranch("int_id",
                    Schema.create(Schema.Type.INT).setNullable(true),
                    "integer identifier", null)))
                .setNullable(true),
            0)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();
    Schema roundTripped = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(descriptor.getFile()));

    assertEquals("string identifier",
        roundTripped.getField("id").getSchema().getBranches().get(0).getDoc());
    assertEquals("integer identifier",
        roundTripped.getField("id").getSchema().getBranches().get(1).getDoc());
  }

  @Test
  void testRoundTripUnionBranchParams() {
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("format", "uuid");
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("id",
            Schema.createUnion(Arrays.asList(
                new UnionBranch("str_id",
                    Schema.createString().setNullable(true),
                    null, params),
                new UnionBranch("int_id",
                    Schema.create(Schema.Type.INT).setNullable(true))))
                .setNullable(true),
            0)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();
    Schema roundTripped = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(descriptor.getFile()));

    Map<String, Object> rtParams =
        roundTripped.getField("id").getSchema().getBranches().get(0).getParams();
    assertNotNull(rtParams);
    assertEquals("uuid", rtParams.get("format"));
    assertTrue(roundTripped.getField("id").getSchema().getBranches().get(1).getParams().isEmpty());
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

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "Person").toDescriptor();
    Schema roundTripped = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(descriptor.getFile()));

    assertEquals("a person record", roundTripped.getDoc());
    assertEquals(Arrays.asList("PII", "SENSITIVE"), roundTripped.getTags());
    assertEquals("2", roundTripped.getParams().get("version"));
  }

  @Test
  void testRoundTripEnumDocTagsParams() {
    Map<String, Object> schemaParams = new LinkedHashMap<>();
    schemaParams.put("source", "api");
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("status",
            Schema.createEnum(Arrays.asList(
                new EnumValue("A"), new EnumValue("B")))
                .setNullable(false)
                .setDoc("status codes")
                .setTags(Arrays.asList("INTERNAL"))
                .setParams(schemaParams),
            0)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();
    Schema roundTripped = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(descriptor.getFile()));

    Schema enumField = roundTripped.getField("status").getSchema();
    assertEquals("status codes", enumField.getDoc());
    assertEquals(Arrays.asList("INTERNAL"), enumField.getTags());
    assertEquals("api", enumField.getParams().get("source"));
  }

  @Test
  void testRoundTripEnumValueDocAndParams() {
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("rgb", "#FF0000");
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("color",
            Schema.createEnum(Arrays.asList(
                new EnumValue("RED", "the color red", params),
                new EnumValue("GREEN", "the color green", null),
                new EnumValue("BLUE")))
                .setNullable(false),
            0)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();
    Schema roundTripped = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(descriptor.getFile()));

    Schema enumField = roundTripped.getField("color").getSchema();
    assertEquals("the color red", enumField.getEnumValues().get(0).getDoc());
    assertEquals("#FF0000", enumField.getEnumValues().get(0).getParams().get("rgb"));
    assertEquals("the color green", enumField.getEnumValues().get(1).getDoc());
    assertTrue(enumField.getEnumValues().get(1).getParams().isEmpty());
    assertNull(enumField.getEnumValues().get(2).getDoc());
  }

  @Test
  void testVariantField() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("data", Schema.create(Schema.Type.VARIANT).setNullable(true), 0)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();
    FieldDescriptor dataField = descriptor.findFieldByName("data");
    assertNotNull(dataField);
    assertEquals(FieldDescriptor.Type.MESSAGE, dataField.getType());
    assertEquals("confluent.type.Variant", dataField.getMessageType().getFullName());
  }

  @Test
  void testRoundTripVariant() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("data", Schema.create(Schema.Type.VARIANT).setNullable(true), 0),
        new Field("name", Schema.createString().setNullable(false), 1)))
        .setNullable(false);

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "TestMessage").toDescriptor();
    Schema roundTripped = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(descriptor.getFile()));

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

    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct, namedTypes), "Person");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Record");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

    assertEquals(original.getRootSchema(), roundTripped.getRootSchema());
    assertEquals(original.getNamedTypes(), roundTripped.getNamedTypes());
  }

  @Test
  void testRoundTripNamedStructLossless() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("street", Schema.createString().setNullable(false), 0),
        new Field("city", Schema.createString().setNullable(false), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home", Schema.createNamedTypeRef("Address").setNullable(true), 0),
        new Field("work", Schema.createNamedTypeRef("Address").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, namedTypes);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Person");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

    assertEquals(original.getRootSchema(), roundTripped.getRootSchema());
    assertEquals(original.getNamedTypes(), roundTripped.getNamedTypes());
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

    Descriptor descriptor = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(struct), "Person").toDescriptor();
    Schema roundTripped = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(descriptor.getFile()));

    assertEquals(Arrays.asList("PII", "EMAIL"), roundTripped.getField("email").getTags());
    assertEquals("high", roundTripped.getField("email").getParams().get("sensitivity"));
    assertTrue(roundTripped.getField("name").getTags().isEmpty());
    assertTrue(roundTripped.getField("name").getParams().isEmpty());
  }

  @Test
  void testRoundTripExternalNamedTypeRef() {
    String addressProtoSchema = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Address {\n"
        + "  string street = 1;\n"
        + "  string city = 2;\n"
        + "}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Address", "address-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Address", addressProtoSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0),
        new Field("home",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Person");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("com.example.Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
  }

  @Test
  void testExternalRefWithMismatchedPackageFails() {
    // Proto schema uses "package example;" but SchemaReference.name is "com.example.Address"
    // The forward converter uses SchemaReference.name as both import and type name,
    // so the proto message's fullName must match for resolution to work.
    String addressProtoSchema = "syntax = \"proto3\";\n"
        + "package example;\n"  // Different from com.example
        + "message Address {\n"
        + "  string street = 1;\n"
        + "  string city = 2;\n"
        + "}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Address", "address-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Address", addressProtoSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0),
        new Field("home",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);

    // Forward fails because the proto file defines "example.Address"
    // but the converter references ".com.example.Address"
    assertThrows(ValidationException.class, () ->
        LogicalTypeToProtoConverter.fromLogicalType(original, "Person"));
  }

  @Test
  void testRoundTripMultipleExternalRefs() {
    String addressProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Address {\n  string city = 1;\n}";
    String moneyProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Money {\n  double amount = 1;\n}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Address", "address-value", 1),
        new SchemaReference("com.example.Money", "money-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Address", addressProto);
    resolvedReferences.put("com.example.Money", moneyProto);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 0),
        new Field("payment",
            Schema.createNamedTypeRef("com.example.Money").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Order");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

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
    String externalProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Money {\n  double amount = 1;\n}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Money", "money-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Money", externalProto);

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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Order");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("payment").getSchema().getType());
    assertEquals("com.example.Money",
        roundTripped.getRootSchema().getField("payment").getSchema().getQualifiedName());
    assertNotNull(roundTripped.getNamedTypes().get("Address"));
  }

  @Test
  void testRoundTripExternalNestedMessageRef() {
    // External proto file containing a top-level message with a nested message
    String externalProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Outer {\n"
        + "  message Inner {\n"
        + "    string label = 1;\n"
        + "  }\n"
        + "}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Outer", "outer-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Outer", externalProto);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("inner",
            Schema.createNamedTypeRef("com.example.Outer.Inner").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

    Schema inner = roundTripped.getRootSchema().getField("inner").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, inner.getType());
    assertEquals("com.example.Outer.Inner", inner.getQualifiedName());
  }

  @Test
  void testRoundTripExternalNestedEnumRef() {
    // External proto file containing a top-level message with a nested enum
    String externalProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Outer {\n"
        + "  enum Color {\n"
        + "    RED = 0;\n"
        + "    GREEN = 1;\n"
        + "    BLUE = 2;\n"
        + "  }\n"
        + "}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Outer", "outer-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Outer", externalProto);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("favorite",
            Schema.createNamedTypeRef("com.example.Outer.Color").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

    Schema favorite = roundTripped.getRootSchema().getField("favorite").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, favorite.getType());
    assertEquals("com.example.Outer.Color", favorite.getQualifiedName());
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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Person");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

    assertEquals("Anonymous",
        roundTripped.getRootSchema().getField("name").getDefaultValue());
    assertEquals(18,
        roundTripped.getRootSchema().getField("age").getDefaultValue());
    // logical.default Meta param should not leak into user params
    assertTrue(roundTripped.getRootSchema().getField("name").getParams() == null
        || !roundTripped.getRootSchema().getField("name").getParams().containsKey("logical.default"));
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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);

    assertEquals(new java.math.BigDecimal("12.34"),
        roundTripped.getRootSchema().getField("dec").getDefaultValue());
    assertEquals(java.time.LocalDate.of(2026, 4, 17),
        roundTripped.getRootSchema().getField("dt").getDefaultValue());
  }

  @Test
  void testRoundTripTimestampPrecisionInProtobuf() {
    for (int p : new int[]{0, 3, 6, 9}) {
      Schema rootSchema = Schema.createStruct(Arrays.asList(
          new Field("t", Schema.createTime(p).setNullable(false), 0),
          new Field("ts", Schema.createTimestamp(p).setNullable(false), 1),
          new Field("tsltz", Schema.createTimestampLtz(p).setNullable(false), 2)))
          .setNullable(false);
      LogicalType original = new LogicalType(rootSchema);
      ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
          original, "Holder");
      LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
      Schema rt = roundTripped.getRootSchema();
      assertEquals(p, rt.getField("t").getSchema().getPrecision());
      assertEquals(p, rt.getField("ts").getSchema().getPrecision());
      assertEquals(p, rt.getField("tsltz").getSchema().getPrecision());
    }
  }

  @Test
  void testRoundTripExternalRefDuplicateFullNameThrows() {
    // Two refs both declaring com.example.Address — collision
    String first = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Address {\n  string street = 1;\n}";
    String second = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Address {\n  string city = 1;\n}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Address", "addr-a", 1),
        new SchemaReference("com.example.Address", "addr-b", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Address", first);
    // Distinct key so the loop iterates twice; both contain the same fullName.
    resolvedReferences.put("com.example.AddressV2", second);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("addr",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);

    assertThrows(ValidationException.class, () ->
        LogicalTypeToProtoConverter.fromLogicalType(original, "Holder"));
  }

  // =========================================================================
  // Defaults on complex types throw
  // =========================================================================

  private static ProtobufSchema convertSchema(Schema rootSchema) {
    return LogicalTypeToProtoConverter.fromLogicalType(
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
        convertSchema(fieldOfTypeWithDefault(arr, Arrays.asList(1, 2, 3)));
    });
  }

  @Test
  void testDefaultOnMapThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema mapType = Schema.createMap(
            Schema.createString().setNullable(false),
            Schema.create(Schema.Type.INT).setNullable(false)).setNullable(false);
        convertSchema(fieldOfTypeWithDefault(mapType, Map.of("k", 1)));
    });
  }

  @Test
  void testDefaultOnMultisetThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema multisetType = Schema.createMultiset(
            Schema.create(Schema.Type.INT).setNullable(false)).setNullable(false);
        convertSchema(fieldOfTypeWithDefault(multisetType, Map.of(1, 2)));
    });
  }

  @Test
  void testDefaultOnStructThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema structType = Schema.createStruct(Arrays.asList(
            new Field("inner", Schema.createString().setNullable(false), 0)))
            .setNullable(false);
        convertSchema(fieldOfTypeWithDefault(structType, "anything"));
    });
  }

  @Test
  void testRoundTripEnumDefault() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B"), new EnumValue("C")))
        .setNullable(false);
    Schema rootSchema = fieldOfTypeWithDefault(enumType, "B");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.getSchema().isNullable());
    assertTrue(f.hasDefaultValue());
    assertEquals("B", f.getDefaultValue());
  }

  @Test
  void testDefaultOnVariantThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema variantType = Schema.create(Schema.Type.VARIANT).setNullable(false);
        convertSchema(fieldOfTypeWithDefault(variantType, "anything"));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("counts").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.INT, rt.getElementType().getType());
  }

  @Test
  void testRoundTripMultisetNullable() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("maybeCounts",
            Schema.createMultiset(Schema.create(Schema.Type.INT).setNullable(false))
                .setNullable(true), 0)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
      LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
          convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("byId").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.INT, rt.getKeyType().getType());
    assertEquals(Schema.Type.STRUCT, rt.getValueType().getType());
  }

  // =========================================================================
  // Field iteration order preserved
  // =========================================================================

  @Test
  void testRoundTripFieldIterationOrderPreserved() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("c", Schema.createString().setNullable(false), 0),
        new Field("a", Schema.createString().setNullable(false), 1),
        new Field("b", Schema.createString().setNullable(false), 2)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
        .setParams(Map.of("owner", "alice"));
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals("Root struct doc", rt.getDoc());
    assertEquals(Arrays.asList("PII", "internal"), rt.getTags());
    assertEquals("alice", rt.getParams().get("owner"));
  }

  @Test
  void testRoundTripDefaultNullOnNullablePrimitive() {
    // Protobuf has no unambiguous null sentinel inside Meta.params; the writer
    // skips emission for null defaults, so a (hasDefault=true, value=null)
    // field round-trips as (hasDefault=false, value=null). Silent loss is the
    // documented behavior for this format.
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("f", Schema.create(Schema.Type.INT).setNullable(true), 0,
            null, true, null, null, null)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Field f = roundTripped.getRootSchema().getField("f");
    assertFalse(f.hasDefaultValue());
    assertNull(f.getDefaultValue());
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
    Schema rt = roundTripped.getRootSchema().getField("u").getSchema();
    assertEquals(Schema.Type.UNION, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        rt.getBranches().get(0).getSchema().getType());
  }

  // =========================================================================
  // UNION nested in collections (Protobuf does not support — UNION maps to oneof
  // which is field-level only; nesting inside ARRAY/MAP throws or mis-converts)
  // =========================================================================

  // testRoundTripUnionInArray, testRoundTripUnionInMapValue:
  // Protobuf's oneof can't appear as a structural type inside ARRAY/MAP.
  // Documented limitation; removed.

  // =========================================================================
  // Nullable complex types
  // =========================================================================

  @Test
  void testRoundTripNullableComplexTypes() {
    Schema namedTypeRef = Schema.createNamedTypeRef("Inner").setNullable(true);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Schema.Type.TINYINT, rt.getField("tiny").getSchema().getType());
    assertEquals(Schema.Type.SMALLINT, rt.getField("small").getSchema().getType());
  }

  // =========================================================================
  // Exhaustive primitives
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
      Schema roundTripped = ProtoToLogicalTypeConverter.toRootSchema(
          convertSchema(original));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(original, "Holder"));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(original, "Holder"));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("m").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.BOOLEAN, rt.getKeyType().getType());
  }

  // =========================================================================
  // Multi-level external references (root -> external A -> external B)
  // =========================================================================

  @Test
  void testRoundTripMultiLevelExternalRefs() {
    String externalB = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "message Foo {\n  int32 x = 1;\n}";
    String externalA = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "import \"com.example.Foo\";\n"
        + "message Bar {\n  com.example.Foo foo = 1;\n}";

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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Holder");
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
    Schema barRef = roundTripped.getRootSchema().getField("bar").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, barRef.getType());
    assertEquals("com.example.Bar", barRef.getQualifiedName());
  }

  // =========================================================================
  // Composite (ARRAY/MAP/MULTISET) branches inside a UNION
  //
  // Proto3 oneof can't contain repeated/map fields directly — composite
  // branches must wrap in a single-message field (`<UpperCamelBranch>RepeatedWrapper`).
  // Without the wrap, the descriptor formally allows the assembly but
  // silently drops `repeated`, losing the collection semantics on read.
  // =========================================================================

  @Test
  void testRoundTripUnionWithArrayBranch() {
    Schema arrayBranch = Schema.createArray(
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(false);
    Schema union = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("xs", arrayBranch),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0))).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rtUnion = rt.getRootSchema().getField("u").getSchema();
    assertEquals(Schema.Type.UNION, rtUnion.getType());
    Schema rtArray = rtUnion.getBranches().get(0).getSchema();
    assertEquals(Schema.Type.ARRAY, rtArray.getType());
    assertEquals(Schema.Type.INT, rtArray.getElementType().getType());
  }

  @Test
  void testRoundTripUnionWithMapBranch() {
    Schema mapBranch = Schema.createMap(
        Schema.createString().setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(false);
    Schema union = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("m", mapBranch),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0))).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rtMap = rt.getRootSchema().getField("u").getSchema()
        .getBranches().get(0).getSchema();
    assertEquals(Schema.Type.MAP, rtMap.getType());
    assertEquals(Schema.Type.VARCHAR, rtMap.getKeyType().getType());
    assertEquals(Schema.Type.INT, rtMap.getValueType().getType());
  }

  @Test
  void testRoundTripUnionWithMultisetBranch() {
    Schema multisetBranch = Schema.createMultiset(
        Schema.createString().setNullable(false))
        .setNullable(false);
    Schema union = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("ms", multisetBranch),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0))).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rtMs = rt.getRootSchema().getField("u").getSchema()
        .getBranches().get(0).getSchema();
    assertEquals(Schema.Type.MULTISET, rtMs.getType());
    assertEquals(Schema.Type.VARCHAR, rtMs.getElementType().getType());
  }

  // =========================================================================
  // ARRAY of UNION / anonymous ENUM
  //
  // UNION and anonymous ENUM are field-level concepts in proto (oneof and a
  // synthesized `Enum` name) — they don't have value-typed representations
  // for use as ARRAY elements. UNION elements wrap as `<UpperCamelField>ElementWrapper`
  // (consistent with all other repeated-element wraps); anonymous ENUMs
  // synthesize a nested enum and use `repeated <enum>` directly.
  // =========================================================================

  @Test
  void testRoundTripArrayOfUnion() {
    Schema union = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("a", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema array = Schema.createArray(union).setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("xs", array, 0))).setNullable(false);
    ProtobufSchema proto = convertSchema(rootSchema);
    Descriptor root = proto.toDescriptor();
    // Wrapped at the repeated-element position → _ElementWrapper.
    assertNotNull(root.findNestedTypeByName("XsElementWrapper"), "Expected nested XsElementWrapper");
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(proto);
    Schema rtArray = rt.getRootSchema().getField("xs").getSchema();
    assertEquals(Schema.Type.ARRAY, rtArray.getType());
    Schema rtUnion = rtArray.getElementType();
    assertEquals(Schema.Type.UNION, rtUnion.getType());
    assertEquals(2, rtUnion.getBranches().size());
    assertEquals(Schema.Type.INT, rtUnion.getBranches().get(0).getSchema().getType());
    assertEquals(Schema.Type.VARCHAR, rtUnion.getBranches().get(1).getSchema().getType());
  }

  // =========================================================================
  // UNION inside UNION (oneof inside oneof)
  //
  // proto3 forbids `oneof` directly inside `oneof`. A UNION branch that is
  // itself a UNION is wrapped as `<UpperCamelBranch>OneofWrapper { value: UNION }` —
  // the wrapper struct re-hosts the inner UNION as a regular field, where
  // fromStructType emits it as a legal nested oneof.
  // =========================================================================

  @Test
  void testRoundTripUnionInUnion() {
    Schema innerUnion = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("s", Schema.createString().setNullable(false)),
        new Schema.UnionBranch("b",
            Schema.create(Schema.Type.BOOLEAN).setNullable(false))))
        .setNullable(false);
    Schema outerUnion = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("i",
            Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("inner", innerUnion)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", outerUnion, 0))).setNullable(false);
    ProtobufSchema proto = convertSchema(rootSchema);
    Descriptor root = proto.toDescriptor();
    // The inner UNION is wrapped as a nested message named after its branch.
    Descriptor wrapper = root.findNestedTypeByName("InnerOneofWrapper");
    assertNotNull(wrapper, "Expected nested InnerOneofWrapper");
    // Inside the wrapper, the inner UNION lives as a oneof named "value".
    List<OneofDescriptor> wrapperOneofs = wrapper.getRealOneofs();
    assertEquals(1, wrapperOneofs.size());
    assertEquals("value", wrapperOneofs.get(0).getName());
    assertEquals(2, wrapperOneofs.get(0).getFields().size());

    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(proto);
    Schema rtOuter = rt.getRootSchema().getField("u").getSchema();
    assertEquals(Schema.Type.UNION, rtOuter.getType());
    assertEquals(2, rtOuter.getBranches().size());
    assertEquals("i", rtOuter.getBranches().get(0).getName());
    assertEquals(Schema.Type.INT,
        rtOuter.getBranches().get(0).getSchema().getType());
    Schema rtInner = rtOuter.getBranches().get(1).getSchema();
    assertEquals(Schema.Type.UNION, rtInner.getType());
    assertEquals(2, rtInner.getBranches().size());
    assertEquals(Schema.Type.VARCHAR,
        rtInner.getBranches().get(0).getSchema().getType());
    assertEquals(Schema.Type.BOOLEAN,
        rtInner.getBranches().get(1).getSchema().getType());
  }

  @Test
  void testRoundTripArrayOfAnonymousEnum() {
    Schema enumSchema = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("RED"),
        new Schema.EnumValue("GREEN"),
        new Schema.EnumValue("BLUE")))
        .setNullable(false);
    Schema array = Schema.createArray(enumSchema).setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("colors", array, 0))).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rtArray = rt.getRootSchema().getField("colors").getSchema();
    assertEquals(Schema.Type.ARRAY, rtArray.getType());
    // Anonymous ENUMs in proto get a synthesized name and become a NAMED_TYPE_REF
    // on round-trip (the synthesized enum is a file-level peer after hoisting).
    Schema rtElement = rtArray.getElementType();
    assertTrue(rtElement.getType() == Schema.Type.ENUM
            || rtElement.getType() == Schema.Type.NAMED_TYPE_REF, "Expected ENUM or NAMED_TYPE_REF, got " + rtElement.getType());
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
        new Schema.UnionBranch("a", innerStruct),
        new Schema.UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rtBranch = roundTripped.getRootSchema().getField("u").getSchema()
        .getBranches().get(0).getSchema();
    assertEquals(Schema.Type.STRUCT, rtBranch.getType());
    assertEquals("inner doc", rtBranch.getDoc());
    assertEquals(Arrays.asList("inner-tag"), rtBranch.getTags());
    assertEquals("v", rtBranch.getParams().get("k"));
  }

  // =========================================================================
  // Wrappable leaf types at root (primitives + variant)
  // =========================================================================

  @Test
  void testRoundTripBooleanAtRoot() {
    Schema root = Schema.create(Schema.Type.BOOLEAN).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
    assertEquals(Schema.Type.BOOLEAN, rt.getRootSchema().getType());
    assertTrue(rt.getRootSchema().isNullable(), "nullability promotion: NOT NULL → NULL via wrapper");
  }

  @Test
  void testRoundTripIntAtRoot() {
    Schema root = Schema.create(Schema.Type.INT).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
    assertEquals(Schema.Type.INT, rt.getRootSchema().getType());
    assertTrue(rt.getRootSchema().isNullable());
  }

  @Test
  void testRoundTripBigintAtRoot() {
    Schema root = Schema.create(Schema.Type.BIGINT).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
    assertEquals(Schema.Type.BIGINT, rt.getRootSchema().getType());
  }

  @Test
  void testRoundTripFloatAtRoot() {
    Schema root = Schema.create(Schema.Type.FLOAT).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
    assertEquals(Schema.Type.FLOAT, rt.getRootSchema().getType());
  }

  @Test
  void testRoundTripDoubleAtRoot() {
    Schema root = Schema.create(Schema.Type.DOUBLE).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
    assertEquals(Schema.Type.DOUBLE, rt.getRootSchema().getType());
  }

  @Test
  void testRoundTripStringAtRoot() {
    Schema root = Schema.createString().setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
    assertEquals(Schema.Type.VARCHAR, rt.getRootSchema().getType());
  }

  @Test
  void testRoundTripBytesAtRoot() {
    Schema root = Schema.createBytes().setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
    assertEquals(Schema.Type.VARBINARY, rt.getRootSchema().getType());
  }

  @Test
  void testRoundTripVariantAtRoot() {
    Schema root = Schema.create(Schema.Type.VARIANT).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
    assertEquals(Schema.Type.VARIANT, rt.getRootSchema().getType());
    assertTrue(rt.getRootSchema().isNullable());
  }

  @Test
  void testRoundTripTinyintAtRootWidensToInt() {
    // TINYINT + SMALLINT both wrap as Int32Value. Round-trip widens to INT,
    // mirroring ProtobufData's INT8/INT16 handling.
    Schema root = Schema.create(Schema.Type.TINYINT).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
    assertEquals(Schema.Type.INT, rt.getRootSchema().getType());
  }

  @Test
  void testRoundTripVarcharAtRootWidensToString() {
    // Bounded VARCHAR loses its length on root wrapping.
    Schema root = Schema.createVarchar(50).setNullable(false);
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
    assertEquals(Schema.Type.VARCHAR, rt.getRootSchema().getType());
  }

  @Test
  void testCharAtRootThrows() {
    // CHAR is fixed-length-padded — semantically distinct from STRING.
    Schema root = Schema.createChar(10).setNullable(false);
    assertThrows(ValidationException.class, () ->
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
  }

  @Test
  void testBinaryAtRootThrows() {
    Schema root = Schema.createBinary(10).setNullable(false);
    assertThrows(ValidationException.class, () ->
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
  }

  @Test
  void testDateAtRootThrows() {
    Schema root = Schema.create(Schema.Type.DATE).setNullable(false);
    assertThrows(ValidationException.class, () ->
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
  }

  @Test
  void testDecimalAtRootThrows() {
    Schema root = Schema.createDecimal(10, 2).setNullable(false);
    assertThrows(ValidationException.class, () ->
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
  }

  @Test
  void testWrappedRootRejectsSchemaMetadata() {
    Schema root = Schema.create(Schema.Type.INT).setNullable(false)
        .setDoc("the count");
    assertThrows(ValidationException.class, () ->
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root), "Holder"));
  }

  @Test
  void testWrappedRootRejectsNamedTypes() {
    Schema root = Schema.create(Schema.Type.INT).setNullable(false);
    Schema named = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Foo", named);
    assertThrows(ValidationException.class, () ->
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(root, namedTypes), "Holder"));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Arrays.asList("PII", "INTERNAL"), rt.getTags());
    assertEquals("2", rt.getParams().get("version"));
    assertEquals("api", rt.getParams().get("source"));
  }

  // =========================================================================
  // Defaults on UNION-typed fields
  // =========================================================================

  @Test
  void testDefaultOnUnionFieldDocumentsBehavior() {
    // Proto's UNION case in the writer (oneof emission) doesn't call
    // applyDefaultIfPresent. Defaults on union-typed fields are silently
    // dropped on the writer side and don't survive round-trip. Pin the
    // current behavior.
    Schema unionType = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", unionType, 0, 5, true, null, null, null)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Field f = roundTripped.getRootSchema().getField("u");
    assertFalse(f.hasDefaultValue(), "default silently dropped for union-typed field");
  }

  // =========================================================================
  // MAP with union-typed key
  // =========================================================================

  @Test
  void testMapEntryNameUsesProtocConvention() {
    // Non-string-keyed maps generate a synthetic XxxEntry message named via
    // protoc's snake_case → CamelCase rule: a field named "user_counts"
    // produces "UserCountsEntry", not "User_countsEntry". Mirrors Set B
    // (FlinkToProtoSchemaConverter) and matches what protoc itself generates.
    Schema mapType = Schema.createMap(
        Schema.create(Schema.Type.INT).setNullable(false),
        Schema.create(Schema.Type.BIGINT).setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("user_counts", mapType, 0)))
        .setNullable(false);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder");
    String descriptorString = proto.toDescriptor().getFile().toProto().toString();
    assertTrue(descriptorString.contains("UserCountsEntry"), "expected synthesized 'UserCountsEntry' message in proto output: "
            + descriptorString);
    assertFalse(descriptorString.contains("User_countsEntry"), "legacy 'User_countsEntry' name should not appear");
  }

  @Test
  void testRoundTripMapWithUnionKey() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema mapType = Schema.createMap(
        unionType, Schema.createString().setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapType, 0)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
        new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("ms",
            Schema.createMultiset(unionType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("ms").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.UNION, rt.getElementType().getType());
    assertEquals(2, rt.getElementType().getBranches().size());
    assertEquals("i", rt.getElementType().getBranches().get(0).getName());
    assertEquals("s", rt.getElementType().getBranches().get(1).getName());
  }

  // =========================================================================
  // Namespace round-trip (LogicalType.namespace ↔ proto file package)
  // =========================================================================

  @Test
  void testRoundTripNamespaceBecomesProtoPackage() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    LogicalType original = new LogicalType("com.example", rootSchema, Map.of(),
        java.util.List.of(), Map.of());
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Holder");
    assertEquals("com.example",
        proto.toDescriptor().getFile().getPackage());
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
    assertEquals("com.example", roundTripped.getNamespace());
  }

  @Test
  void testRoundTripNoNamespaceProducesNoPackage() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    LogicalType original = new LogicalType(rootSchema);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        original, "Holder");
    assertEquals("", proto.toDescriptor().getFile().getPackage());
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(proto);
    assertNull(roundTripped.getNamespace());
  }

  // =========================================================================
  // Root NAMED_TYPE_REF resolution
  // =========================================================================

  /**
   * `TYPE Address` against a `STRUCT Address` declaration produces a
   * LogicalType whose root is NAMED_TYPE_REF("io.confluent.Address") and
   * whose namedTypes contains the actual struct. Proto needs to resolve this
   * to "the file's root message IS Address" — message name from the simple
   * part, package from the namespace.
   */
  @Test
  void testRoundTripNamedTypeRefRoot() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("street", Schema.createString().setNullable(false), 0),
        new Field("state", Schema.createString().setNullable(false), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("io.confluent.Address", addressType);
    Schema root = Schema.createNamedTypeRef("io.confluent.Address")
        .setNullable(false);
    LogicalType lt = new LogicalType("io.confluent", root, namedTypes,
        java.util.List.of(), Map.of());

    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        lt, "ignored");
    assertEquals("io.confluent",
        proto.toDescriptor().getFile().getPackage());
    assertEquals("Address", proto.toDescriptor().getName());
    assertEquals(2, proto.toDescriptor().getFields().size());

    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(proto);
    assertEquals("io.confluent", rt.getNamespace());
  }

  // =========================================================================
  // Reserved-keyword field names
  // =========================================================================

  @Test
  void testRoundTripReservedKeywordFieldNames() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("class", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("enum", Schema.createString().setNullable(false), 1),
        new Field("default", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 2),
        new Field("package", Schema.createString().setNullable(false), 3),
        new Field("type", Schema.create(Schema.Type.BIGINT).setNullable(false), 4)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
            Schema.createNamedTypeRef("A_B").setNullable(true), 1)))
        .setNullable(false);
    Schema typeB = Schema.createStruct(Arrays.asList(
        new Field("bVal", Schema.createString().setNullable(false), 0),
        new Field("toA",
            Schema.createNamedTypeRef("A_A").setNullable(true), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("A_A", typeA);
    namedTypes.put("A_B", typeB);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("root",
            Schema.createNamedTypeRef("A_A").setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(rootSchema, namedTypes), "Holder"));

    Schema rootRef = roundTripped.getRootSchema().getField("root").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rootRef.getType());

    Schema rtA = roundTripped.getNamedTypes().get("A_A");
    assertNotNull(rtA);
    Schema toBRef = rtA.getField("toB").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, toBRef.getType());

    Schema rtB = roundTripped.getNamedTypes().get("A_B");
    assertNotNull(rtB);
    Schema toARef = rtB.getField("toA").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, toARef.getType());
  }

  // =========================================================================
  // Standalone TIME precision (Avro has its own; protobuf needed parity)
  // =========================================================================

  @Test
  void testRoundTripTimePrecision() {
    for (int p = 0; p <= 9; p++) {
      Schema rootSchema = Schema.createStruct(Arrays.asList(
          new Field("t", Schema.createTime(p).setNullable(false), 0)))
          .setNullable(false);
      LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
          convertSchema(rootSchema));
      Schema rt = roundTripped.getRootSchema().getField("t").getSchema();
      assertEquals(Schema.Type.TIME, rt.getType(), "TIME(" + p + ")");
      assertEquals(p, rt.getPrecision(), "TIME precision " + p);
    }
  }

  // =========================================================================
  // All-precision timestamp round-trip (0..9, including sub-millisecond)
  // =========================================================================

  @Test
  void testRoundTripTimestampPrecisionAllValues() {
    for (int p = 0; p <= 9; p++) {
      Schema rootSchema = Schema.createStruct(Arrays.asList(
          new Field("t", Schema.createTime(p).setNullable(false), 0),
          new Field("ts", Schema.createTimestamp(p).setNullable(false), 1),
          new Field("tsltz", Schema.createTimestampLtz(p).setNullable(false), 2)))
          .setNullable(false);
      LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
          convertSchema(rootSchema));
      Schema rt = roundTripped.getRootSchema();
      assertEquals(p, rt.getField("t").getSchema().getPrecision(), "TIME precision " + p);
      assertEquals(p, rt.getField("ts").getSchema().getPrecision(), "TIMESTAMP precision " + p);
      assertEquals(p, rt.getField("tsltz").getSchema().getPrecision(), "TIMESTAMP_LTZ precision " + p);
    }
  }

  // =========================================================================
  // Nullable + non-null default (parity with Avro test)
  // =========================================================================

  @Test
  void testRoundTripNullableFieldWithNonNullDefault() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("f", Schema.create(Schema.Type.INT).setNullable(true), 0,
            42, true, null, null, null)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.getSchema().isNullable());
    assertTrue(f.hasDefaultValue());
    assertEquals(42, f.getDefaultValue());
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals("name of the user", rt.getField("a").getDoc());
    assertEquals("age in years", rt.getField("b").getDoc());
  }

  // =========================================================================
  // Empty + single-field struct
  // =========================================================================

  @Test
  void testEmptyStructRoundTrip() {
    Schema empty = Schema.createStruct(java.util.Collections.emptyList())
        .setNullable(false);
    Schema rt = ProtoToLogicalTypeConverter.toRootSchema(convertSchema(empty));
    assertEquals(Schema.Type.STRUCT, rt.getType());
    assertTrue(rt.getFields().isEmpty());
  }

  @Test
  void testRoundTripSingleFieldStruct() {
    Schema single = Schema.createStruct(Arrays.asList(
        new Field("only", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Schema rt = ProtoToLogicalTypeConverter.toRootSchema(convertSchema(single));
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
        new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false)),
        new Schema.UnionBranch("b", Schema.create(Schema.Type.BOOLEAN).setNullable(false)),
        new Schema.UnionBranch("d", Schema.create(Schema.Type.DOUBLE).setNullable(false)),
        new Schema.UnionBranch("by", Schema.createBytes().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0)))
        .setNullable(false);
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
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
    LogicalType roundTripped = ProtoToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("inner").getSchema();
    assertEquals("inner doc", rt.getDoc());
    assertEquals(Arrays.asList("inner-tag"), rt.getTags());
    assertEquals("inner-val", rt.getParams().get("inner-key"));
  }

  // =========================================================================
  // V1 emission mode (Flink-compatible)
  // =========================================================================

  @Test
  void testV1RowSuffixUsesUnderscoreVerbatim() {
    Schema inner = Schema.createStruct(Arrays.asList(
        new Field("v", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("xs", inner, 0))).setNullable(false);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1);
    Descriptor root = proto.toDescriptor();
    // V1: xs_Row (verbatim + underscore), not XsRow
    assertNotNull(root.findNestedTypeByName("xs_Row"), "Expected nested xs_Row in V1");
    assertNull(root.findNestedTypeByName("XsRow"), "Should NOT have XsRow in V1");
  }

  @Test
  void testV1RepeatedWrapperUsesVerbatim() {
    // nullable ARRAY → wrapped via _RepeatedWrapper
    Schema arr = Schema.createArray(
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(true);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("xs", arr, 0))).setNullable(false);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1);
    Descriptor root = proto.toDescriptor();
    assertNotNull(root.findNestedTypeByName("xsRepeatedWrapper"), "Expected xsRepeatedWrapper in V1");
    assertNull(root.findNestedTypeByName("XsRepeatedWrapper"), "Should NOT have XsRepeatedWrapper in V1");
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
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1));
    assertTrue(ex.getMessage().contains("UNION"));
  }

  @Test
  void testV1ThrowsOnVariant() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("v", Schema.create(Schema.Type.VARIANT).setNullable(false), 0)))
        .setNullable(false);
    ValidationException ex = assertThrows(ValidationException.class, () ->
        LogicalTypeToProtoConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1));
    assertTrue(ex.getMessage().contains("VARIANT"));
  }

  @Test
  void testV1AnonymousEnumAutoPromotesWithUnderscoreNaming() {
    // V1 anonymous ENUM: still emits a nested named enum (auto-promote),
    // with V1 naming: <field>_Enum
    Schema enumSchema = Schema.createEnum(Arrays.asList(
        new EnumValue("RED"), new EnumValue("GREEN"), new EnumValue("BLUE")))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("color", enumSchema, 0))).setNullable(false);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1);
    Descriptor root = proto.toDescriptor();
    assertNotNull(root.findEnumTypeByName("color_Enum"), "Expected color_Enum in V1");
  }

  @Test
  void testV1SkipsLogicalDefault() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false),
            0, "Anonymous", true, null, null, null)))
        .setNullable(false);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1);
    FieldDescriptor name = proto.toDescriptor().findFieldByName("name");
    String defaultStr = name.getOptions()
        .getExtension(io.confluent.protobuf.MetaProto.fieldMeta)
        .getParamsOrDefault("logical.default", null);
    assertNull(defaultStr, "Field default should be skipped in V1");
  }

  @Test
  void testV1EditionMetadata() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    ProtobufSchema v1 = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1);
    ProtobufSchema v2 = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V2);
    assertEquals("1", v1.metadata().getProperties().get("confluent:edition"));
    assertEquals("2", v2.metadata().getProperties().get("confluent:edition"));
  }

  // =========================================================================
  // Nested message emission via dotted-name convention
  //
  // A localNamedType keyed `Outer.Inner` (where `Outer` is also a localNamedType)
  // is emitted as a proto message nested inside `Outer`, not as a file-level
  // peer. Non-proto writers ignore the convention; only the proto writer acts
  // on it structurally.
  // =========================================================================

  @Test
  void testEmitNestedMessageInsideParent() {
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
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(root, namedTypes), "ignored");

    Descriptor outer = proto.toDescriptor();
    assertEquals("Outer", outer.getName());
    // Inner exists as a NESTED type of Outer, not a file-level peer.
    assertEquals(1, outer.getNestedTypes().size());
    assertEquals("Inner", outer.getNestedTypes().get(0).getName());
    // No file-level peer named Inner.
    long fileLevelInners = outer.getFile().getMessageTypes().stream()
        .filter(d -> "Inner".equals(d.getName())).count();
    assertEquals(0, fileLevelInners);
  }

  @Test
  void testEmitDeeplyNestedMessages() {
    Schema deepest = Schema.createStruct(Arrays.asList(
        new Field("z", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema mid = Schema.createStruct(Arrays.asList(
        new Field("y", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("inner",
            Schema.createNamedTypeRef("Outer.Mid.Inner").setNullable(false), 1)))
        .setNullable(false);
    Schema outer = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("mid",
            Schema.createNamedTypeRef("Outer.Mid").setNullable(false), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Outer", outer);
    namedTypes.put("Outer.Mid", mid);
    namedTypes.put("Outer.Mid.Inner", deepest);

    Schema root = Schema.createNamedTypeRef("Outer").setNullable(false);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(root, namedTypes), "ignored");

    Descriptor outerDesc = proto.toDescriptor();
    assertEquals("Outer", outerDesc.getName());
    Descriptor midDesc = outerDesc.findNestedTypeByName("Mid");
    assertNotNull(midDesc, "Mid should be nested in Outer");
    Descriptor innerDesc = midDesc.findNestedTypeByName("Inner");
    assertNotNull(innerDesc, "Inner should be nested in Mid");
  }

  @Test
  void testEmitNestedEnumInsideParent() {
    Schema statusEnum = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("ACTIVE", null, null),
        new Schema.EnumValue("INACTIVE", null, null)))
        .setNullable(false);
    Schema outer = Schema.createStruct(Arrays.asList(
        new Field("status",
            Schema.createNamedTypeRef("Outer.Status").setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Outer", outer);
    namedTypes.put("Outer.Status", statusEnum);

    Schema root = Schema.createNamedTypeRef("Outer").setNullable(false);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(root, namedTypes), "ignored");

    Descriptor outerDesc = proto.toDescriptor();
    assertEquals(1, outerDesc.getEnumTypes().size());
    assertEquals("Status", outerDesc.getEnumTypes().get(0).getName());
  }

  @Test
  void testNestedTypeWithSiblingTopLevel() {
    Schema innerType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema outerType = Schema.createStruct(Arrays.asList(
        new Field("inner",
            Schema.createNamedTypeRef("Outer.Inner").setNullable(false), 0)))
        .setNullable(false);
    Schema peerType = Schema.createStruct(Arrays.asList(
        new Field("y", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Outer", outerType);
    namedTypes.put("Outer.Inner", innerType);
    namedTypes.put("Peer", peerType);

    Schema root = Schema.createNamedTypeRef("Outer").setNullable(false);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(root, namedTypes), "ignored");

    // Outer at file level with Inner nested; Peer also at file level.
    long fileLevelMessages = proto.toDescriptor().getFile().getMessageTypes().size();
    assertEquals(2, fileLevelMessages, "Outer + Peer at file level");
    Descriptor outer = proto.toDescriptor();
    assertNotNull(outer.findNestedTypeByName("Inner"));
  }

  // =========================================================================
  // Round-trip of nested types: writer marks user-declared nested messages with
  // the LOGICAL_NAMED_PROP marker, reader lifts marked ones back into
  // localNamedTypes (via dotted-name keys). Synthesized wrappers (no marker)
  // stay inlined.
  // =========================================================================

  @Test
  void testRoundTripNestedTypeLifted() {
    Schema innerType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema outerType = Schema.createStruct(Arrays.asList(
        new Field("inner",
            Schema.createNamedTypeRef("Outer.Inner").setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Outer", outerType);
    namedTypes.put("Outer.Inner", innerType);
    Schema root = Schema.createNamedTypeRef("Outer").setNullable(false);

    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(root, namedTypes), "ignored");
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(proto);

    // Inner survives as a localNamedType keyed by its dotted nesting path.
    assertNotNull(rt.getNamedTypes().get("Outer.Inner"));
    assertEquals(Schema.Type.STRUCT,
        rt.getNamedTypes().get("Outer.Inner").getType());
    // Outer has nested named types under it (Outer.Inner), so the reader
    // keeps Outer in namedTypes (as NAMED_TYPE_REF root) — preserves the
    // parent-of resolution that the namespace validator needs.
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getRootSchema().getType());
    assertEquals("Outer", rt.getRootSchema().getQualifiedName());
    Schema outerBody = rt.getNamedTypes().get("Outer");
    assertNotNull(outerBody);
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        outerBody.getField("inner").getSchema().getType());
    assertEquals("Outer.Inner",
        outerBody.getField("inner").getSchema().getQualifiedName());
  }

  @Test
  void testRoundTripDeeplyNestedTypesLifted() {
    Schema deepest = Schema.createStruct(Arrays.asList(
        new Field("z", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema mid = Schema.createStruct(Arrays.asList(
        new Field("inner",
            Schema.createNamedTypeRef("Outer.Mid.Inner").setNullable(false), 0)))
        .setNullable(false);
    Schema outer = Schema.createStruct(Arrays.asList(
        new Field("mid",
            Schema.createNamedTypeRef("Outer.Mid").setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Outer", outer);
    namedTypes.put("Outer.Mid", mid);
    namedTypes.put("Outer.Mid.Inner", deepest);
    Schema root = Schema.createNamedTypeRef("Outer").setNullable(false);

    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(root, namedTypes), "ignored");
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(proto);

    assertNotNull(rt.getNamedTypes().get("Outer.Mid"));
    assertNotNull(rt.getNamedTypes().get("Outer.Mid.Inner"));
  }

  @Test
  void testSynthesizedWrappersStayInlined() {
    // ARRAY<STRUCT> forces the writer to synthesize a nested message name
    // (no logical.named marker). The reader must inline it, not lift it as a
    // named type — there's no user-meaningful name to lift.
    Schema element = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("y", Schema.createString().setNullable(false), 1)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("items",
            Schema.createArray(element).setNullable(false), 0)))
        .setNullable(false);
    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder");
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(proto);

    assertTrue(rt.getNamedTypes().isEmpty(),
        "synthesized wrapper should not be lifted into namedTypes");
    Schema elementRT = rt.getRootSchema().getField("items").getSchema()
        .getElementType();
    assertEquals(Schema.Type.STRUCT, elementRT.getType());
  }

  @Test
  void testMultiRootUnionEmittedAsPeerMessages() {
    // The visitor's multi-root sugar produces UNION<NAMED_TYPE_REF(A),
    // NAMED_TYPE_REF(B)> at the root. The proto writer detects this shape
    // and emits A as the primary root + B as a file-level peer (instead of
    // throwing "UNION at root not supported").
    Schema a = Schema.createStruct(Arrays.asList(
        new Field("ax", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema b = Schema.createStruct(Arrays.asList(
        new Field("by", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("com.example.A", a);
    namedTypes.put("com.example.B", b);
    Schema root = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("A",
            Schema.createNamedTypeRef("com.example.A").setNullable(false),
            null, null),
        new Schema.UnionBranch("B",
            Schema.createNamedTypeRef("com.example.B").setNullable(false),
            null, null)
    )).setNullable(false);
    LogicalType lt = new LogicalType("com.example", root, namedTypes,
        java.util.Collections.emptyList(), java.util.Collections.emptyMap());

    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(lt, "ignored");

    Descriptor file = proto.toDescriptor();
    assertEquals("A", file.getName(), "first union member becomes primary root");
    long peerCount = file.getFile().getMessageTypes().size();
    assertEquals(2, peerCount, "both A and B emitted as file-level messages");
    boolean hasB = file.getFile().getMessageTypes().stream()
        .anyMatch(m -> "B".equals(m.getName()));
    assertTrue(hasB, "B emitted as a peer message");
  }

  @Test
  void testNamedTypeMarkerStrippedFromBody() {
    Schema inner = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema outer = Schema.createStruct(Arrays.asList(
        new Field("inner",
            Schema.createNamedTypeRef("Outer.Inner").setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Outer", outer);
    namedTypes.put("Outer.Inner", inner);
    Schema root = Schema.createNamedTypeRef("Outer").setNullable(false);

    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(root, namedTypes), "ignored");
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(proto);

    // The marker is internal to the proto wire layer — it must not appear in
    // the recovered LT model's params (auto-stripped via the logical.* filter).
    Schema recovered = rt.getNamedTypes().get("Outer.Inner");
    assertNotNull(recovered);
    assertFalse(recovered.getParams().containsKey("logical.named"),
        "marker should be stripped from the recovered Schema's params");
  }

  /**
   * Registering an external reference as the root produces an empty public-import proto
   * file: {@code import public "..."} for the referenced file, no local
   * messages.
   */
  @Test
  void testRegisterExternalReferenceEmitsPublicImport() {
    String externalProto = "syntax = \"proto3\";\n"
        + "package com;\n"
        + "message Foo {\n"
        + "  string id = 1;\n"
        + "}\n";
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("com.Foo", externalProto);
    LogicalType lt = new LogicalType(
        "com",
        Schema.createNamedTypeRef("com.Foo").setNullable(false),
        java.util.Collections.emptyMap(),
        java.util.Collections.singletonList(
            new io.confluent.kafka.schemaregistry.client.rest.entities
                .SchemaReference("com.Foo", "foo-subject", 1)),
        resolved);

    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(lt, "ignored");

    String text = proto.canonicalString();
    assertTrue(text.contains("import public"),
        () -> "expected `import public` in canonical proto, got: " + text);
    // No local messages in the public-import wrapper itself — `rawSchema()`
    // is the ProtoFileElement of the file we built, before any public-import
    // resolution. (`toDescriptor().getFile()` would return the imported
    // file's descriptor since that's where the resolved type lives.)
    assertEquals(0, proto.rawSchema().getTypes().size(),
        "public-import wrapper should have no local types");
    // toDescriptor() resolves through the public import (via
    // ProtobufSchema.name()'s fall-through) to com.Foo.
    assertEquals("com.Foo", proto.toDescriptor().getFullName());
  }

  /**
   * Round-trip: writer emits an empty `import public` wrapper; reader recovers
   * a LogicalType whose root is a NAMED_TYPE_REF to the externally-defined
   * type. Confirms writer and reader agree on the convention.
   */
  @Test
  void testRoundTripExternalReferenceRegistration() {
    String externalProto = "syntax = \"proto3\";\n"
        + "package com;\n"
        + "message Foo {\n"
        + "  string id = 1;\n"
        + "}\n";
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("com.Foo", externalProto);
    LogicalType lt = new LogicalType(
        "com",
        Schema.createNamedTypeRef("com.Foo").setNullable(false),
        java.util.Collections.emptyMap(),
        java.util.Collections.singletonList(
            new io.confluent.kafka.schemaregistry.client.rest.entities
                .SchemaReference("com.Foo", "foo-subject", 1)),
        resolved);

    ProtobufSchema proto = LogicalTypeToProtoConverter.fromLogicalType(lt, "ignored");
    LogicalType rt = ProtoToLogicalTypeConverter.toLogicalType(proto);

    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getRootSchema().getType());
    assertEquals("com.Foo", rt.getRootSchema().getQualifiedName());
    assertTrue(rt.getNamedTypes().isEmpty(),
        "public-import round-trip should produce no local namedTypes");
  }
}
