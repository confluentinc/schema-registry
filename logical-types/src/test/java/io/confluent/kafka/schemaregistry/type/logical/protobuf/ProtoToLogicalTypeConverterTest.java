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
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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

  /** Build a message carrying one enum field, with the enum's values numbered as given. */
  private Schema enumSchemaWithNumbers(int... numbers) throws Exception {
    EnumDescriptorProto.Builder enumProto = EnumDescriptorProto.newBuilder().setName("Color");
    String[] names = {"RED", "GREEN", "BLUE"};
    for (int i = 0; i < numbers.length; i++) {
      enumProto.addValue(
          EnumValueDescriptorProto.newBuilder().setName(names[i]).setNumber(numbers[i]));
    }
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("TestMessage")
        .addEnumType(enumProto.build())
        .addField(FieldDescriptorProto.newBuilder()
            .setName("color").setNumber(1).setType(Type.TYPE_ENUM).setTypeName("Color"))
        .build();
    Schema result = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(buildFileDescriptor(message)));
    return result.getField("color").getSchema();
  }

  @Test
  void sequentialEnumNumbersAreNotRecorded() throws Exception {
    // 0,1,2 is what the writer reproduces positionally, so nothing needs storing.
    for (Schema.EnumValue ev : enumSchemaWithNumbers(0, 1, 2).getEnumValues()) {
      assertNull(ev.getEnumNumber(), "expected no number recorded for " + ev.getSymbol());
    }
  }

  @Test
  void nonSequentialEnumNumbersAreRecordedForEveryValue() throws Exception {
    List<Schema.EnumValue> values = enumSchemaWithNumbers(0, 5, 9).getEnumValues();
    assertEquals(0, values.get(0).getEnumNumber());
    assertEquals(5, values.get(1).getEnumNumber());
    assertEquals(9, values.get(2).getEnumNumber());
  }

  @Test
  void aliasedEnumNumbersAreRecorded() throws Exception {
    // Two symbols sharing a number is never sequential, so the whole set is recorded.
    List<Schema.EnumValue> values = enumSchemaWithNumbers(0, 1, 1).getEnumValues();
    assertEquals(1, values.get(1).getEnumNumber());
    assertEquals(1, values.get(2).getEnumNumber());
  }

  @Test
  void enumNotStartingAtZeroIsRecorded() throws Exception {
    // proto2 may start at 1. The reader records it faithfully even though the proto3 writer can't
    // emit it — dropping the numbers here would let the writer silently renumber from 0.
    List<Schema.EnumValue> values = enumSchemaWithNumbers(1, 2, 3).getEnumValues();
    assertEquals(1, values.get(0).getEnumNumber());
    assertEquals(2, values.get(1).getEnumNumber());
    assertEquals(3, values.get(2).getEnumNumber());
  }

  @Test
  void enumNotStartingAtZeroFailsOnTheWayBackToProto() throws Exception {
    // The other half of the contract above: the numbering survives into LT and is then rejected
    // loudly by the proto3 writer, rather than coming back as 0,1,2.
    Schema enumSchema = enumSchemaWithNumbers(1, 2, 3);
    Schema struct = Schema.createStruct(
        List.of(new Schema.Field("color", enumSchema, 0))).setNullable(false);
    assertThrows(ValidationException.class,
        () -> LogicalTypeToProtoConverter.fromLogicalType(new LogicalType(struct), "TestMessage"));
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
  void namedLeafRootNameRoundTripsThroughProto() throws Exception {
    // Proto -> LT -> Proto: a single non-recursive message root unwraps to a bare STRUCT carrying
    // its name on read, and the writer re-emits it under the same message name.
    DescriptorProto order = DescriptorProto.newBuilder()
        .setName("Order")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("id").setNumber(1).setType(Type.TYPE_INT32))
        .build();
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(
        new ProtobufSchema(buildFileDescriptor(order)));
    assertEquals(Schema.Type.STRUCT, lt.getRootSchema().getType());
    assertEquals("Order", lt.getName());
    ProtobufSchema out = LogicalTypeToProtoConverter.fromLogicalType(lt, "IGNORED");
    assertEquals("Order", out.toDescriptor().getName());

    // The DDL projection declares the root as a named STRUCT; a single unreferenced root needs no
    // explicit trailing TYPE (first-wins inference recovers it).
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertThat(ddl).contains("STRUCT Order (");
    assertThat(ddl).doesNotContain("TYPE");
  }

  @Test
  void twoIndependentMessagesRoundTripThroughProtoPreservingNames() throws Exception {
    // Proto (2 independent top-level messages) -> LT -> Proto (2 messages). The first message is
    // the root: it unwraps to a bare STRUCT carrying its name on LogicalType.name. The second is a
    // peer named type (a STRUCT in namedTypes). Both message names survive the round-trip.
    DescriptorProto order = DescriptorProto.newBuilder()
        .setName("Order")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("id").setNumber(1).setType(Type.TYPE_INT32))
        .build();
    DescriptorProto customer = DescriptorProto.newBuilder()
        .setName("Customer")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("name").setNumber(1).setType(Type.TYPE_STRING))
        .build();
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
        .addMessageType(order)
        .addMessageType(customer)
        .setSyntax("proto3")
        .build();
    FileDescriptor fd = FileDescriptor.buildFrom(fileProto, new FileDescriptor[0]);

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(fd));
    // Root: bare STRUCT carrying its name.
    assertEquals(Schema.Type.STRUCT, lt.getRootSchema().getType());
    assertEquals("Order", lt.getName());
    // Peer: a named STRUCT held in namedTypes (only the root goes bare + name).
    Schema customerType = lt.getNamedTypes().get("Customer");
    assertNotNull(customerType);
    assertEquals(Schema.Type.STRUCT, customerType.getType());

    // Back to Proto: both messages re-emitted under their original names, fields intact.
    ProtobufSchema out = LogicalTypeToProtoConverter.fromLogicalType(lt, "IGNORED");
    Descriptor rootOut = out.toDescriptor();
    assertEquals("Order", rootOut.getName());
    assertNotNull(rootOut.findFieldByName("id"));
    Descriptor customerOut = rootOut.getFile().findMessageTypeByName("Customer");
    assertNotNull(customerOut);
    assertNotNull(customerOut.findFieldByName("name"));

    // The DDL projection declares BOTH as named STRUCTs. No explicit trailing TYPE is needed:
    // first-wins inference makes the first-declared unreferenced type (Order) the root.
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertThat(ddl).contains("STRUCT Order (");
    assertThat(ddl).contains("STRUCT Customer (");
    assertThat(ddl).doesNotContain("TYPE Order");
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

  @Test
  void testFieldNumberPopulatedFromProto() throws Exception {
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("M")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("a").setNumber(3).setType(Type.TYPE_INT32))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("b").setNumber(7).setType(Type.TYPE_INT32))
        .build();

    Schema result = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(buildFileDescriptor(message)));

    assertThat(result.getField("a").getFieldNumber()).isEqualTo(3);
    assertThat(result.getField("b").getFieldNumber()).isEqualTo(7);
  }

  @Test
  void testFieldNumbersRoundTripPreservesNonSequential() throws Exception {
    // Non-sequential, non-positional numbers: the round-trip must preserve them
    // rather than renumbering positionally (the field-number fidelity fix).
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("M")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("a").setNumber(3).setType(Type.TYPE_INT32))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("b").setNumber(7).setType(Type.TYPE_INT32))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("c").setNumber(100).setType(Type.TYPE_INT32))
        .build();

    Schema srlt = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(buildFileDescriptor(message)));
    Descriptor out = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(srlt), "M").toDescriptor();

    assertThat(out.findFieldByName("a").getNumber()).isEqualTo(3);
    assertThat(out.findFieldByName("b").getNumber()).isEqualTo(7);
    assertThat(out.findFieldByName("c").getNumber()).isEqualTo(100);
  }

  @Test
  void testOneofBranchAndRegularFieldNumbersRoundTrip() throws Exception {
    // Numbers are out of sequence (oneof members 10/20 declared before regular field 3), so the
    // reader records all of them — regular field and oneof branches alike — and the writer restores
    // each via the native slot. Regulars are laid out before oneof members, so the regular field's
    // descriptor index differs from its emission position; the all-or-nothing recording is immune
    // to that skew.
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("M")
        .addOneofDecl(OneofDescriptorProto.newBuilder().setName("o"))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("a").setNumber(10).setType(Type.TYPE_INT32)
            .setLabel(Label.LABEL_OPTIONAL).setOneofIndex(0))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("b").setNumber(20).setType(Type.TYPE_INT32)
            .setLabel(Label.LABEL_OPTIONAL).setOneofIndex(0))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("c").setNumber(3).setType(Type.TYPE_INT32))
        .build();

    Schema srlt = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(buildFileDescriptor(message)));
    assertThat(srlt.getField("c").getFieldNumber()).isEqualTo(3);

    Descriptor out = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(srlt), "M").toDescriptor();
    assertThat(out.findFieldByName("c").getNumber()).isEqualTo(3);
    assertThat(out.findFieldByName("a").getNumber()).isEqualTo(10);
    assertThat(out.findFieldByName("b").getNumber()).isEqualTo(20);
  }

  @Test
  void testOutOfOrderRegularFieldNumbersRoundTrip() throws Exception {
    // Regression for the mixed omit/record hole: with per-field omission, b (number 2 == its
    // position + 1) was omitted while a (5) was recorded, and the writer's positional fallback then
    // handed b the number 1. All-or-nothing recording keeps both.
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("M")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("a").setNumber(5).setType(Type.TYPE_INT32))
        .addField(FieldDescriptorProto.newBuilder()
            .setName("b").setNumber(2).setType(Type.TYPE_INT32))
        .build();

    Schema srlt = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(buildFileDescriptor(message)));
    Descriptor out = LogicalTypeToProtoConverter.fromLogicalType(
        new LogicalType(srlt), "M").toDescriptor();

    assertThat(out.findFieldByName("a").getNumber()).isEqualTo(5);
    assertThat(out.findFieldByName("b").getNumber()).isEqualTo(2);
  }

  @Test
  void simpleRootMessageNameCarriedAndRenderedInDdl() throws Exception {
    // A simple (non-recursive, non-nested) root message is unwrapped to a bare STRUCT; its name
    // would be lost, so it is carried on LogicalType.name and the DDL renders a named declaration.
    DescriptorProto message = DescriptorProto.newBuilder()
        .setName("Order")
        .addField(FieldDescriptorProto.newBuilder()
            .setName("id").setNumber(1).setType(Type.TYPE_STRING))
        .build();

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(
        new ProtobufSchema(buildFileDescriptor(message)));

    assertThat(lt.getRootSchema().getType()).isEqualTo(Schema.Type.STRUCT);
    assertThat(lt.getName()).isEqualTo("Order");
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertThat(ddl).contains("STRUCT Order (");
    assertThat(ddl).doesNotContain("TYPE STRUCT");
  }

  @Test
  void namedInlineRootShownWithUnreferencedLocalPeer() {
    // Two independent top-level messages: root is Bar (the first message), Foo is an unreferenced
    // local peer. The root is emitted as a named declaration FIRST, so first-wins root inference
    // picks Bar on read-back — the name is shown, no anonymous fallback and no spurious UNION.
    String proto = "syntax = \"proto3\";\n"
        + "message Bar {\n  string f2 = 1;\n}\n"
        + "message Foo {\n  string f1 = 1;\n}\n";
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(proto));

    assertThat(lt.getName()).isEqualTo("Bar");
    assertThat(lt.getRootSchema().getType()).isEqualTo(Schema.Type.STRUCT);

    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertThat(ddl).doesNotContain("TYPE STRUCT");
    assertThat(ddl).contains("STRUCT Bar (");
    assertThat(ddl).contains("STRUCT Foo (");
    // The root (Bar) is declared before the peer (Foo) so first-wins picks it.
    assertThat(ddl.indexOf("STRUCT Bar (")).isLessThan(ddl.indexOf("STRUCT Foo ("));
  }

  @Test
  void peerReferencedRootStaysNamedRefAndForcesExplicitType() {
    // Root Order (first message) is referenced by peer Wrapper. Unwrapping Order to a bare inline
    // root would remove its body from namedTypes and dangle Wrapper.order (which every format
    // writer resolves only through namedTypes), so the shared root stays a NAMED_TYPE_REF with
    // Order kept as a named peer. First-wins inference would then pick Wrapper (Order is
    // referenced), so the DDL writer forces the root with an explicit trailing TYPE, NOT NULL.
    String proto = "syntax = \"proto3\";\n"
        + "message Order {\n  string id = 1;\n}\n"
        + "message Wrapper {\n  Order order = 1;\n}\n";
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(proto));

    assertThat(lt.getName()).isNull();
    assertThat(lt.getRootSchema().getType()).isEqualTo(Schema.Type.NAMED_TYPE_REF);
    assertThat(lt.getRootSchema().getQualifiedName()).isEqualTo("Order");
    assertThat(lt.getNamedTypes()).containsKey("Order").containsKey("Wrapper");

    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertThat(ddl).contains("STRUCT Order (");
    assertThat(ddl).contains("TYPE Order NOT NULL;");

    // The format writer resolves Wrapper.order through namedTypes (Order kept there) instead of
    // throwing "Unknown named type reference"; building the descriptor exercises that resolution.
    Descriptor out = LogicalTypeToProtoConverter.fromLogicalType(lt, "Order").toDescriptor();
    assertThat(out.findFieldByName("id")).isNotNull();
    Descriptor wrapper = out.getFile().findMessageTypeByName("Wrapper");
    assertThat(wrapper.findFieldByName("order").getMessageType()).isEqualTo(out);
  }
}
