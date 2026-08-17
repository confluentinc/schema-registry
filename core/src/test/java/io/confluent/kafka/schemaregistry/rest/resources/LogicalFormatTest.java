/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code format=logical} conversion helper. No live cluster is needed: both
 * directions are pure conversions over a mocked {@link SchemaRegistry}.
 */
class LogicalFormatTest {

  private static final String STRUCT_DDL = "STRUCT Widget (id INT NOT NULL, name VARCHAR NOT NULL)";

  // -- isLogical --------------------------------------------------------------------------------

  @Test
  void isLogicalIsCaseInsensitive() {
    assertTrue(LogicalFormat.isLogical("logical"));
    assertTrue(LogicalFormat.isLogical("LOGICAL"));
    assertTrue(LogicalFormat.isLogical("Logical"));
  }

  @Test
  void isLogicalRejectsEverythingElse() {
    assertEquals(false, LogicalFormat.isLogical("resolved"));
    assertEquals(false, LogicalFormat.isLogical(""));
    assertEquals(false, LogicalFormat.isLogical(null));
  }

  // -- convertToNative: happy path, one per schemaType -------------------------------------------

  @Test
  void convertToNativeProducesAvro() throws Exception {
    RegisterSchemaRequest request = requestFor("AVRO", STRUCT_DDL);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

    LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request);

    AvroSchema avroSchema = new AvroSchema(request.getSchema());
    assertNotNull(avroSchema.rawSchema().getField("id"));
    assertNotNull(avroSchema.rawSchema().getField("name"));
    assertEquals(2, avroSchema.rawSchema().getFields().size());
  }

  @Test
  void convertToNativeProducesJson() throws Exception {
    RegisterSchemaRequest request = requestFor("JSON", STRUCT_DDL);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

    LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request);

    JsonSchema jsonSchema = new JsonSchema(request.getSchema());
    assertNotNull(jsonSchema.rawSchema());
    assertTrue(request.getSchema().contains("id"));
    assertTrue(request.getSchema().contains("name"));
  }

  @Test
  void convertToNativeProducesProtobuf() throws Exception {
    RegisterSchemaRequest request = requestFor("PROTOBUF", STRUCT_DDL);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

    LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request);

    ProtobufSchema protobufSchema = new ProtobufSchema(request.getSchema());
    assertNotNull(protobufSchema.toDescriptor().findFieldByName("id"));
    assertNotNull(protobufSchema.toDescriptor().findFieldByName("name"));
  }

  @Test
  void convertToNativeIsCaseInsensitiveOnSchemaType() throws Exception {
    RegisterSchemaRequest request = requestFor("avro", STRUCT_DDL);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

    LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request);

    assertEquals(2, new AvroSchema(request.getSchema()).rawSchema().getFields().size());
  }

  // -- convertToNative: rejections ---------------------------------------------------------------

  @Test
  void convertToNativeRequiresSchemaType() {
    RegisterSchemaRequest request = requestFor(null, STRUCT_DDL);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("schemaType is required"));
  }

  @Test
  void convertToNativeRejectsBlankSchemaType() {
    RegisterSchemaRequest request = requestFor("   ", STRUCT_DDL);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("schemaType is required"));
  }

  @Test
  void convertToNativeRejectsUnsupportedSchemaType() {
    RegisterSchemaRequest request = requestFor("XML", STRUCT_DDL);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("Unsupported schemaType"));
  }

  @Test
  void convertToNativeRejectsMalformedDdl() {
    // Syntactically valid DDL, structurally invalid: a struct may not declare the same field
    // name twice. Distinct from convertToNativeRejectsUnparsableDdl below, which is a syntax
    // error instead.
    RegisterSchemaRequest request = requestFor(
        "AVRO", "STRUCT Widget (id INT NOT NULL, id VARCHAR NOT NULL)");
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("Invalid logical type schema"));
  }

  @Test
  void convertToNativeRejectsUnparsableDdl() {
    RegisterSchemaRequest request = requestFor("AVRO", "not valid ddl at all {{{");
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("Invalid logical type schema"));
  }

  // -- convertToNative: external references --------------------------------------------------

  @Test
  void convertToNativeResolvesExternalReferences() throws Exception {
    // Bare external reference (FQN used directly, no USING TYPE) -- the Avro-compatible shape.
    // USING TYPE populates externalImports, which LogicalTypeToAvroConverter rejects outright as
    // a JSON-only mechanism (see its own validation), so it isn't a valid fixture for AVRO here.
    String ddl = "STRUCT Widget (id INT NOT NULL, addr com.example.Address NOT NULL)";
    RegisterSchemaRequest request = requestFor("AVRO", ddl);
    request.setReferences(List.of(
        new SchemaReference("com.example.Address", "address-value", 1)));

    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    String addressSchema =
        "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"com.example\","
            + "\"fields\":[{\"name\":\"street\",\"type\":\"string\"}]}";
    when(schemaRegistry.get("address-value", 1, false))
        .thenReturn(schemaEntityFor("AVRO", addressSchema));

    LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request);

    // The emitted schema legitimately leaves "addr" as a cross-schema pointer to
    // com.example.Address rather than inlining it -- matching how Avro schema references work in
    // general, so it must be re-parsed the same way the real registration pipeline would: with
    // request.getReferences() and the resolved external body supplied alongside it.
    AvroSchema avroSchema = new AvroSchema(
        request.getSchema(),
        request.getReferences(),
        java.util.Map.of("com.example.Address", addressSchema),
        null, null, null, false);
    assertNotNull(avroSchema.rawSchema().getField("addr"));
  }

  @Test
  void convertToNativeFailsWhenReferenceCannotBeResolved() throws Exception {
    String ddl = "STRUCT Widget (id INT NOT NULL, addr com.example.Address NOT NULL)";
    RegisterSchemaRequest request = requestFor("AVRO", ddl);
    request.setReferences(List.of(
        new SchemaReference("com.example.Address", "address-value", 1)));

    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.get("address-value", 1, false)).thenReturn(null);

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.convertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("Could not resolve reference"));
  }

  // -- convertToLogicalDdl: happy path, one per schemaType -------------------------------------

  @Test
  void convertToLogicalDdlFromAvro() throws Exception {
    String avroSchemaString =
        "{\"type\":\"record\",\"name\":\"Row\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"},"
            + "{\"name\":\"name\",\"type\":\"string\"}]}";
    Schema schema = schemaEntityFor("AVRO", avroSchemaString);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.parseSchema(schema, false, false))
        .thenReturn(new AvroSchema(avroSchemaString));

    String ddl = LogicalFormat.convertToLogical(schemaRegistry, schema);

    assertTrue(ddl.contains("id"));
    assertTrue(ddl.contains("name"));
  }

  @Test
  void convertToLogicalDdlFromJson() throws Exception {
    String jsonSchemaString =
        "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
            + "\"type\":\"object\",\"properties\":{"
            + "\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"}},"
            + "\"required\":[\"id\",\"name\"]}";
    Schema schema = schemaEntityFor("JSON", jsonSchemaString);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.parseSchema(schema, false, false))
        .thenReturn(new JsonSchema(jsonSchemaString));

    String ddl = LogicalFormat.convertToLogical(schemaRegistry, schema);

    assertTrue(ddl.contains("id"));
    assertTrue(ddl.contains("name"));
  }

  @Test
  void convertToLogicalDdlFromProtobuf() throws Exception {
    String protoSchemaString =
        "syntax = \"proto3\";\n"
            + "message Widget {\n"
            + "  int32 id = 1;\n"
            + "  string name = 2;\n"
            + "}\n";
    Schema schema = schemaEntityFor("PROTOBUF", protoSchemaString);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.parseSchema(schema, false, false))
        .thenReturn(new ProtobufSchema(protoSchemaString));

    String ddl = LogicalFormat.convertToLogical(schemaRegistry, schema);

    assertTrue(ddl.contains("id"));
    assertTrue(ddl.contains("name"));
  }

  @Test
  void convertToLogicalDdlRejectsUnsupportedSchemaType() throws Exception {
    Schema schema = schemaEntityFor("XML", "<not-a-real-schema/>");
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.parseSchema(any(Schema.class), anyBoolean(), anyBoolean()))
        .thenThrow(new InvalidSchemaException("unsupported"));

    assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.convertToLogical(schemaRegistry, schema));
  }

  // -- helpers ------------------------------------------------------------------------------

  private static RegisterSchemaRequest requestFor(String schemaType, String ddl) {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchemaType(schemaType);
    request.setSchema(ddl);
    return request;
  }

  private static Schema schemaEntityFor(String schemaType, String schemaString) {
    return new Schema(
        "widgets-value", 1, 1, null, schemaType, Collections.emptyList(), null, null,
        schemaString);
  }
}
