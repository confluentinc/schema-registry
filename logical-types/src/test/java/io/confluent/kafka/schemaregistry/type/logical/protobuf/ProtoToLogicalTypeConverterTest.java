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

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtoToLogicalTypeConverterTest {

  private FileDescriptor buildFileDescriptor(DescriptorProto messageType) throws Exception {
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
        .addMessageType(messageType)
        .setSyntax("proto3")
        .build();
    return FileDescriptor.buildFrom(fileProto, new FileDescriptor[0]);
  }

  @Test
  void testSimplePrimitives() throws Exception {
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("TestMessage")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("bool_field").setNumber(1).setType(Type.TYPE_BOOL))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("int_field").setNumber(2).setType(Type.TYPE_INT32))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("long_field").setNumber(3).setType(Type.TYPE_INT64))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("float_field").setNumber(4).setType(Type.TYPE_FLOAT))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("double_field").setNumber(5).setType(Type.TYPE_DOUBLE))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("string_field").setNumber(6).setType(Type.TYPE_STRING))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("bytes_field").setNumber(7).setType(Type.TYPE_BYTES))
        .build();

    FileDescriptor fd = buildFileDescriptor(message);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));

    assertEquals(Schema.Type.STRUCT, result.getType());
    assertEquals(7, result.getFields().size());
    assertEquals(Schema.Type.BOOLEAN, result.getField("bool_field").getSchema().getType());
    assertEquals(Schema.Type.INT, result.getField("int_field").getSchema().getType());
    assertEquals(Schema.Type.BIGINT, result.getField("long_field").getSchema().getType());
    assertEquals(Schema.Type.FLOAT, result.getField("float_field").getSchema().getType());
    assertEquals(Schema.Type.DOUBLE, result.getField("double_field").getSchema().getType());
    assertEquals(Schema.Type.VARCHAR, result.getField("string_field").getSchema().getType());
    assertEquals(Schema.Type.VARBINARY, result.getField("bytes_field").getSchema().getType());

    // Proto3 primitive fields are NOT NULL by default
    assertFalse(result.getField("int_field").getSchema().isNullable());
  }

  @Test
  void testOptionalField() throws Exception {
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("TestMessage")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("opt_field").setNumber(1).setType(Type.TYPE_INT32)
            .setProto3Optional(true))
        .addOneofDecl(OneofDescriptorProto.newBuilder().setName("_opt_field"))
        .build();

    // Need to set oneof_index for the optional field
    DescriptorProto.Builder msgBuilder = DescriptorProto.newBuilder()
        .setName("TestMessage")
        .addOneofDecl(OneofDescriptorProto.newBuilder().setName("_opt_field"));
    msgBuilder.addField(FieldDescriptorProto.newBuilder()
        .setName("opt_field").setNumber(1).setType(Type.TYPE_INT32)
        .setProto3Optional(true)
        .setOneofIndex(0));

    FileDescriptor fd = buildFileDescriptor(msgBuilder.build());
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));

    assertTrue(result.getField("opt_field").getSchema().isNullable());
  }

  @Test
  void testEnum() throws Exception {
    EnumDescriptorProto enumProto = EnumDescriptorProto.newBuilder()
        .setName("Color")
        .addValue(EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(0))
        .addValue(EnumValueDescriptorProto.newBuilder().setName("GREEN").setNumber(1))
        .addValue(EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(2))
        .build();

    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("TestMessage")
        .addEnumType(enumProto)
        .addField(FieldDescriptorProto.newBuilder()
            .setName("color").setNumber(1).setType(Type.TYPE_ENUM)
            .setTypeName("Color"))
        .build();

    FileDescriptor fd = buildFileDescriptor(message);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));

    Schema colorField = result.getField("color").getSchema();
    assertEquals(Schema.Type.ENUM, colorField.getType());
    assertEquals(3, colorField.getEnumValues().size());
    assertEquals("RED", colorField.getEnumValues().get(0).getSymbol());
    assertEquals("GREEN", colorField.getEnumValues().get(1).getSymbol());
    assertEquals("BLUE", colorField.getEnumValues().get(2).getSymbol());
  }

  @Test
  void testOneof() throws Exception {
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("TestMessage")
        .addOneofDecl(OneofDescriptorProto.newBuilder().setName("id"))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("str_id").setNumber(1).setType(Type.TYPE_STRING)
            .setOneofIndex(0))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("int_id").setNumber(2).setType(Type.TYPE_INT32)
            .setOneofIndex(0))
        .build();

    FileDescriptor fd = buildFileDescriptor(message);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));

    // Oneof becomes a UNION field
    Schema.Field idField = result.getField("id");
    assertEquals(Schema.Type.UNION, idField.getSchema().getType());
    assertTrue(idField.getSchema().isNullable());

    assertEquals(2, idField.getSchema().getBranches().size());
    assertEquals("str_id", idField.getSchema().getBranches().get(0).getName());
    assertEquals(Schema.Type.VARCHAR,
        idField.getSchema().getBranches().get(0).getSchema().getType());
    assertEquals("int_id", idField.getSchema().getBranches().get(1).getName());
    assertEquals(Schema.Type.INT,
        idField.getSchema().getBranches().get(1).getSchema().getType());
  }

  @Test
  void testRepeatedArray() throws Exception {
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("TestMessage")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("tags").setNumber(1).setType(Type.TYPE_STRING)
            .setLabel(Label.LABEL_REPEATED))
        .build();

    FileDescriptor fd = buildFileDescriptor(message);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));

    Schema tagsField = result.getField("tags").getSchema();
    assertEquals(Schema.Type.ARRAY, tagsField.getType());
    assertEquals(Schema.Type.VARCHAR, tagsField.getElementType().getType());
    // Repeated fields are not nullable
    assertFalse(tagsField.isNullable());
  }

  @Test
  void testNestedMessage() throws Exception {
    DescriptorProto innerMessage = DescriptorProto.newBuilder()
        .setName("Inner")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("value").setNumber(1).setType(Type.TYPE_STRING))
        .build();

    DescriptorProto outerMessage = DescriptorProto.newBuilder()
        .setName("Outer")
        .addNestedType(innerMessage)
        .addField(FieldDescriptorProto.newBuilder()
            .setName("inner").setNumber(1).setType(Type.TYPE_MESSAGE)
            .setTypeName("Inner"))
        .build();

    FileDescriptor fd = buildFileDescriptor(outerMessage);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));

    Schema innerField = result.getField("inner").getSchema();
    assertEquals(Schema.Type.STRUCT, innerField.getType());
    // MESSAGE types are nullable by default
    assertTrue(innerField.isNullable());
    assertEquals(Schema.Type.VARCHAR,
        innerField.getField("value").getSchema().getType());
  }

  @Test
  void testExternalEnumRefPreservesNamedTypeRef() {
    // External proto file containing only an enum
    String externalProto = "syntax = \"proto3\";\n"
        + "package com.example;\n"
        + "enum Color {\n"
        + "  RED = 0;\n"
        + "  GREEN = 1;\n"
        + "  BLUE = 2;\n"
        + "}";

    // Importing proto file that references the external enum by full name
    String mainProto = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"com.example.Color\";\n"
        + "message Order {\n"
        + "  com.example.Color favorite = 1;\n"
        + "}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Color", "color-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Color", externalProto);

    ProtobufSchema schema = new ProtobufSchema(
        mainProto, references, resolvedReferences, null, null, null, null);

    LogicalType result = ProtoToLogicalTypeConverter.toLogicalType(schema);
    Schema favorite = result.getRootSchema().getField("favorite").getSchema();

    // Without the external-type check in case ENUM, this would be Schema.Type.ENUM
    // (inlined enum values), losing the external reference identity.
    assertEquals(Schema.Type.NAMED_TYPE_REF, favorite.getType());
    assertEquals("com.example.Color", favorite.getQualifiedName());
  }

  @Test
  void testRecursiveRootMessageSurfacesAsNamedTypeRef() throws Exception {
    // A recursive root message round-trips: the reader pre-registers the
    // root, builds its body (self-references resolve via the placeholder),
    // and surfaces the root as a NAMED_TYPE_REF whose body lives in
    // namedTypes. Non-recursive roots still unwrap to a STRUCT — this
    // NAMED_TYPE_REF wrapping is reserved for cycle-participating roots.
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("Node")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("value").setNumber(1).setType(Type.TYPE_INT32))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("next").setNumber(2).setType(Type.TYPE_MESSAGE)
            .setTypeName("Node"))
        .build();
    FileDescriptor fd = buildFileDescriptor(message);
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(fd));
    Schema root = lt.getRootSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, root.getType());
    assertEquals("Node", root.getQualifiedName());
    Schema body = lt.getNamedTypes().get("Node");
    assertNotNull(body);
    assertEquals(Schema.Type.STRUCT, body.getType());
    Schema next = body.getField("next").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, next.getType());
    assertEquals("Node", next.getQualifiedName());
    assertTrue(next.isNullable(),
        "proto message fields are nullable; LT preserves that");
  }

  @Test
  void testTypeMappings() {
    // Matrix-driven coverage. Each TypeMapping in CommonMappings goes
    // LT -> Proto -> LT and the result must equal the original. Adding a new
    // primitive Schema.Type without registering a mapping here surfaces as a
    // failing test until coverage is added.
    for (CommonMappings.TypeMapping mapping : CommonMappings.get()) {
      Schema original = mapping.asRootStruct();
      Schema rt = ProtoToLogicalTypeConverter.toRootSchema(
          LogicalTypeToProtoConverter.fromLogicalType(
              new LogicalType(original), "Holder"));
      assertEquals(original, rt, "Round trip failed for " + mapping);
    }
  }

  @Test
  void testExternalInt32ValueRootUnwrapsToInt() throws Exception {
    // External producer's proto file: import wrappers.proto and use Int32Value
    // as the schema root via the standard well-known type.
    DescriptorProto message = com.google.protobuf.Int32Value.getDescriptor()
        .toProto();
    FileDescriptor fd = FileDescriptor.buildFrom(
        FileDescriptorProto.newBuilder()
            .setName("google/protobuf/wrappers.proto")
            .setPackage("google.protobuf")
            .setSyntax("proto3")
            .addMessageType(message)
            .build(),
        new FileDescriptor[0]);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));
    assertEquals(Schema.Type.INT, result.getType());
    assertTrue(result.isNullable());
  }

  @Test
  void testExternalBoolValueRootUnwrapsToBoolean() throws Exception {
    DescriptorProto message = com.google.protobuf.BoolValue.getDescriptor()
        .toProto();
    FileDescriptor fd = FileDescriptor.buildFrom(
        FileDescriptorProto.newBuilder()
            .setName("google/protobuf/wrappers.proto")
            .setPackage("google.protobuf")
            .setSyntax("proto3")
            .addMessageType(message)
            .build(),
        new FileDescriptor[0]);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));
    assertEquals(Schema.Type.BOOLEAN, result.getType());
    assertTrue(result.isNullable());
  }

  @Test
  void testExternalStringValueRootUnwrapsToString() throws Exception {
    DescriptorProto message = com.google.protobuf.StringValue.getDescriptor()
        .toProto();
    FileDescriptor fd = FileDescriptor.buildFrom(
        FileDescriptorProto.newBuilder()
            .setName("google/protobuf/wrappers.proto")
            .setPackage("google.protobuf")
            .setSyntax("proto3")
            .addMessageType(message)
            .build(),
        new FileDescriptor[0]);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));
    assertEquals(Schema.Type.VARCHAR, result.getType());
    assertTrue(result.isNullable());
  }

  @Test
  void testExternalVariantRootUnwrapsToVariant() throws Exception {
    DescriptorProto message = io.confluent.protobuf.type.Variant.getDescriptor()
        .toProto();
    FileDescriptor fd = FileDescriptor.buildFrom(
        FileDescriptorProto.newBuilder()
            .setName("confluent/type/variant.proto")
            .setPackage("confluent.type")
            .setSyntax("proto3")
            .addMessageType(message)
            .build(),
        new FileDescriptor[0]);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));
    assertEquals(Schema.Type.VARIANT, result.getType());
    assertTrue(result.isNullable());
  }

  /**
   * Negative case: a user-defined message that *happens* to have the same
   * shape as a wrapper (one int32 "value" field) but a different name should
   * NOT be unwrapped. Recognition is by full name, not by structural shape.
   */
  @Test
  void testNonWrapperRootStaysAsStruct() throws Exception {
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("Wrapper")  // not google.protobuf.Int32Value
        .addField(FieldDescriptorProto.newBuilder()
            .setName("value").setNumber(1).setType(Type.TYPE_INT32))
        .build();
    FileDescriptor fd = buildFileDescriptor(message);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));
    assertEquals(Schema.Type.STRUCT, result.getType(), "non-wrapper full name → stays a STRUCT");
    assertEquals(1, result.getFields().size());
    assertEquals(Schema.Type.INT, result.getField("value").getSchema().getType());
  }

  /**
   * Regression: when a Descriptor's file imports {@code google/protobuf/wrappers.proto},
   * every type from that file is registered as an external reference (because the import
   * shows up in {@code ProtobufSchema.resolvedReferences()}). Wrapper-typed fields must
   * still unwrap to nullable primitives — the unwrap check has to run before the
   * {@code isExternalType} lookup, otherwise the fields would surface as unresolved
   * {@code NAMED_TYPE_REF}s.
   */
  @Test
  void testImportedWrapperFieldsUnwrapToNullablePrimitives() throws Exception {
    FileDescriptor wrappersFile = com.google.protobuf.StringValue.getDescriptor().getFile();
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("Row")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("s").setNumber(1).setType(Type.TYPE_MESSAGE)
            .setTypeName(".google.protobuf.StringValue"))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("i").setNumber(2).setType(Type.TYPE_MESSAGE)
            .setTypeName(".google.protobuf.Int32Value"))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("l").setNumber(3).setType(Type.TYPE_MESSAGE)
            .setTypeName(".google.protobuf.Int64Value"))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("f").setNumber(4).setType(Type.TYPE_MESSAGE)
            .setTypeName(".google.protobuf.FloatValue"))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("d").setNumber(5).setType(Type.TYPE_MESSAGE)
            .setTypeName(".google.protobuf.DoubleValue"))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("b").setNumber(6).setType(Type.TYPE_MESSAGE)
            .setTypeName(".google.protobuf.BoolValue"))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("by").setNumber(7).setType(Type.TYPE_MESSAGE)
            .setTypeName(".google.protobuf.BytesValue"))
        .build();
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
        .setName("test.proto")
        .setSyntax("proto3")
        .addDependency(wrappersFile.getName())
        .addMessageType(message)
        .build();
    FileDescriptor fd = FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{wrappersFile});

    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd.findMessageTypeByName("Row")));

    assertEquals(Schema.Type.STRUCT, result.getType());
    assertWrapperUnwrap(result.getField("s").getSchema(), Schema.Type.VARCHAR);
    assertWrapperUnwrap(result.getField("i").getSchema(), Schema.Type.INT);
    assertWrapperUnwrap(result.getField("l").getSchema(), Schema.Type.BIGINT);
    assertWrapperUnwrap(result.getField("f").getSchema(), Schema.Type.FLOAT);
    assertWrapperUnwrap(result.getField("d").getSchema(), Schema.Type.DOUBLE);
    assertWrapperUnwrap(result.getField("b").getSchema(), Schema.Type.BOOLEAN);
    assertWrapperUnwrap(result.getField("by").getSchema(), Schema.Type.VARBINARY);
  }

  private static void assertWrapperUnwrap(Schema field, Schema.Type expected) {
    assertEquals(expected, field.getType(),
        "wrapper-typed field must unwrap, not surface as a NAMED_TYPE_REF");
    assertTrue(field.isNullable(), "unwrapped wrapper is nullable");
  }

  @Test
  void testNullableOneofWithManyBranches() throws Exception {
    // 4-branch oneof — verify all branches are preserved in order.
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("ManyBranches")
        .addOneofDecl(OneofDescriptorProto.newBuilder().setName("payload"))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("a").setNumber(1).setType(Type.TYPE_STRING).setOneofIndex(0))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("b").setNumber(2).setType(Type.TYPE_INT32).setOneofIndex(0))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("c").setNumber(3).setType(Type.TYPE_BOOL).setOneofIndex(0))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("d").setNumber(4).setType(Type.TYPE_DOUBLE).setOneofIndex(0))
        .build();

    FileDescriptor fd = buildFileDescriptor(message);
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(new ProtobufSchema(fd));

    Schema payload = result.getField("payload").getSchema();
    assertEquals(Schema.Type.UNION, payload.getType());
    assertTrue(payload.isNullable());
    assertEquals(4, payload.getBranches().size());
    assertEquals("a", payload.getBranches().get(0).getName());
    assertEquals("b", payload.getBranches().get(1).getName());
    assertEquals("c", payload.getBranches().get(2).getName());
    assertEquals("d", payload.getBranches().get(3).getName());
  }

  /**
   * Reader handles a re-export-only proto file: no local types, just an
   * {@code import public}. The root becomes a NAMED_TYPE_REF to the first
   * publicly-imported type. References / resolvedReferences are passed
   * through unchanged.
   */
  @Test
  void testReadPublicImportRoot() {
    String externalProto = "syntax = \"proto3\";\n"
        + "package com;\n"
        + "message Foo {\n"
        + "  string id = 1;\n"
        + "}\n";
    String reExportProto = "syntax = \"proto3\";\n"
        + "package com;\n"
        + "import public \"leaf.proto\";\n";

    java.util.Map<String, String> resolved = new java.util.HashMap<>();
    resolved.put("leaf.proto", externalProto);
    ProtobufSchema proto = new ProtobufSchema(
        reExportProto,
        java.util.Collections.singletonList(
            new io.confluent.kafka.schemaregistry.client.rest.entities
                .SchemaReference("leaf.proto", "leaf-subject", 1)),
        resolved,
        1,
        null);

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(proto);

    Schema root = lt.getRootSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, root.getType());
    assertEquals("com.Foo", root.getQualifiedName());
    assertFalse(root.isNullable(), "re-export root should be non-nullable");
    // No local namedTypes — Foo lives in the imported file, not this one.
    assertTrue(lt.getNamedTypes().isEmpty(),
        "re-export file should produce no local namedTypes");
    // References pass through unchanged.
    assertEquals(1, lt.getReferences().size());
    assertEquals("leaf.proto", lt.getReferences().get(0).getName());
  }
}
