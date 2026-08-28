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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
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

  // -- tryConvertToNative: native-first input auto-detection --------------------------------------

  @Test
  void tryConvertToNativeLeavesABodyThatParsesAsNativeAlone() throws Exception {
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    // Body parses as its declared native schemaType -> not logical, whatever the body text is.
    when(schemaRegistry.parseSchema(any(Schema.class), anyBoolean(), anyBoolean()))
        .thenReturn(mock(ParsedSchema.class));
    RegisterSchemaRequest request = requestFor("AVRO", STRUCT_DDL);

    assertEquals(false, LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request));
    assertEquals(STRUCT_DDL, request.getSchema(), "a native body must not be rewritten");
  }

  @Test
  void tryConvertToNativeConvertsDdlOnceNativeParseFails() throws Exception {
    RegisterSchemaRequest request = requestFor("AVRO", STRUCT_DDL);

    assertTrue(LogicalFormat.tryConvertToNative(
        registryRejectingNative(), "widgets-value", request));
    assertEquals(AvroSchema.TYPE, request.getSchemaType());
  }

  @Test
  void tryConvertToNativeLeavesBodyThatIsNeitherNativeNorDdl() throws Exception {
    // Parses as neither, so it stays native and the native error surfaces downstream.
    RegisterSchemaRequest request = requestFor("AVRO", "not a schema at all {{{");

    assertEquals(false, LogicalFormat.tryConvertToNative(
        registryRejectingNative(), "widgets-value", request));
    assertEquals("not a schema at all {{{", request.getSchema());
  }

  @Test
  void tryConvertToNativeRejectsNativeSchemaTextAsLogical() throws Exception {
    // Even with the native parse failing, real native schema text is not valid DDL, so it is
    // never misread as logical input.
    for (String nativeText : List.of(
        "{\"type\":\"record\",\"name\":\"W\",\"fields\":[]}",     // Avro
        "{\"type\":\"object\",\"properties\":{}}",                 // JSON Schema
        "syntax = \"proto3\"; message W { string id = 1; }",      // Protobuf
        "enum E { A = 0; }")) {                                   // Protobuf enum, not DDL
      RegisterSchemaRequest request = requestFor("AVRO", nativeText);
      assertEquals(false,
          LogicalFormat.tryConvertToNative(registryRejectingNative(), "widgets-value", request),
          "should not be treated as logical: " + nativeText);
      assertEquals(nativeText, request.getSchema());
    }
  }

  @Test
  void tryConvertToNativeRejectsBlankAndNull() throws Exception {
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    assertEquals(false, LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", null));
    assertEquals(false, LogicalFormat.tryConvertToNative(
        schemaRegistry, "widgets-value", requestFor("AVRO", "")));
    assertEquals(false, LogicalFormat.tryConvertToNative(
        schemaRegistry, "widgets-value", requestFor("AVRO", "   ")));
  }

  // -- convertToNative: happy path, one per schemaType -------------------------------------------

  @Test
  void convertToNativeProducesAvro() throws Exception {
    RegisterSchemaRequest request = requestFor("AVRO", STRUCT_DDL);
    SchemaRegistry schemaRegistry = registryRejectingNative();

    LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request);

    AvroSchema avroSchema = new AvroSchema(request.getSchema());
    assertNotNull(avroSchema.rawSchema().getField("id"));
    assertNotNull(avroSchema.rawSchema().getField("name"));
    assertEquals(2, avroSchema.rawSchema().getFields().size());
  }

  @Test
  void convertToNativeProducesJson() throws Exception {
    RegisterSchemaRequest request = requestFor("JSON", STRUCT_DDL);
    SchemaRegistry schemaRegistry = registryRejectingNative();

    LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request);

    JsonSchema jsonSchema = new JsonSchema(request.getSchema());
    assertNotNull(jsonSchema.rawSchema());
    assertTrue(request.getSchema().contains("id"));
    assertTrue(request.getSchema().contains("name"));
  }

  @Test
  void convertToNativeProducesProtobuf() throws Exception {
    RegisterSchemaRequest request = requestFor("PROTOBUF", STRUCT_DDL);
    SchemaRegistry schemaRegistry = registryRejectingNative();

    LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request);

    ProtobufSchema protobufSchema = new ProtobufSchema(request.getSchema());
    assertNotNull(protobufSchema.toDescriptor().findFieldByName("id"));
    assertNotNull(protobufSchema.toDescriptor().findFieldByName("name"));
  }

  @Test
  void convertToNativeIsCaseInsensitiveOnSchemaType() throws Exception {
    RegisterSchemaRequest request = requestFor("avro", STRUCT_DDL);
    SchemaRegistry schemaRegistry = registryRejectingNative();

    LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request);

    assertEquals(2, new AvroSchema(request.getSchema()).rawSchema().getFields().size());
  }

  // -- convertToNative: rejections ---------------------------------------------------------------

  @Test
  void convertToNativeRequiresSchemaType() {
    RegisterSchemaRequest request = requestFor(null, STRUCT_DDL);
    SchemaRegistry schemaRegistry = registryRejectingNative();

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("schemaType is required"));
  }

  @Test
  void convertToNativeRejectsBlankSchemaType() {
    RegisterSchemaRequest request = requestFor("   ", STRUCT_DDL);
    SchemaRegistry schemaRegistry = registryRejectingNative();

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("schemaType is required"));
  }

  @Test
  void convertToNativeRejectsUnsupportedSchemaType() {
    RegisterSchemaRequest request = requestFor("XML", STRUCT_DDL);
    SchemaRegistry schemaRegistry = registryRejectingNative();

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("Unsupported schemaType"));
  }

  @Test
  void convertToNativeRejectsMalformedDdl() {
    // Syntactically valid DDL, structurally invalid: a struct may not declare the same field
    // name twice. Distinct from convertToNativeRejectsUnparsableDdl below, which is a syntax
    // error instead.
    RegisterSchemaRequest request = requestFor(
        "AVRO", "STRUCT Widget (id INT NOT NULL, id VARCHAR NOT NULL)");
    SchemaRegistry schemaRegistry = registryRejectingNative();

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("Invalid logical type schema"));
  }

  @Test
  void convertToNativeIgnoresUnparsableDdl() throws Exception {
    // A DDL *syntax* error means "not logical", not "bad logical": the body is left alone so the
    // native path reports it in its own terms. Contrast convertToNativeRejectsMalformedDdl, where
    // the body parses and so is held to logical-schema rules.
    RegisterSchemaRequest request = requestFor("AVRO", "not valid ddl at all {{{");

    assertEquals(false, LogicalFormat.tryConvertToNative(
        registryRejectingNative(), "widgets-value", request));
    assertEquals("not valid ddl at all {{{", request.getSchema());
  }

  @Test
  void convertToNativeMapsIllegalTargetNameToInvalidSchema() {
    // A root name that is legal DDL (backtick-quoted) but illegal for the target format: Avro's
    // Schema.createRecord rejects the space, throwing SchemaParseException. That is invalid client
    // input, so it must surface as InvalidSchemaException (422) via the conversion catch, not as an
    // uncaught RuntimeException (500).
    RegisterSchemaRequest request = requestFor("AVRO", "STRUCT `bad name` (id INT NOT NULL)");
    SchemaRegistry schemaRegistry = registryRejectingNative();

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("cannot be represented as AVRO"),
        "expected the conversion-failure message, got: " + e.getMessage());
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

    SchemaRegistry schemaRegistry = registryRejectingNative();
    when(schemaRegistry.tenant()).thenReturn(QualifiedSubject.DEFAULT_TENANT);
    String addressSchema =
        "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"com.example\","
            + "\"fields\":[{\"name\":\"street\",\"type\":\"string\"}]}";
    // Reference resolution routes through AbstractSchemaProvider.resolveReferences, which
    // qualifies the subject against the parent context and fetches via getByVersion with
    // lookupDeletedSchema=true: conversion resolves permissively and leaves enforcement of the
    // caller's configured validation mode to the native path that re-resolves downstream.
    when(schemaRegistry.getByVersion("address-value", 1, true))
        .thenReturn(schemaEntityFor("AVRO", addressSchema));

    LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request);

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

    SchemaRegistry schemaRegistry = registryRejectingNative();
    when(schemaRegistry.tenant()).thenReturn(QualifiedSubject.DEFAULT_TENANT);
    when(schemaRegistry.getByVersion("address-value", 1, true)).thenReturn(null);

    InvalidSchemaException e = assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.tryConvertToNative(schemaRegistry, "widgets-value", request));
    assertTrue(e.getMessage().contains("Could not resolve schema references"));
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

  /**
   * A registry whose native parse always fails, which is what a real one does for a DDL body.
   * {@code tryConvertToNative} is native-first, so a bare mock -- whose {@code parseSchema} returns
   * null rather than throwing -- would classify every body as native and convert nothing.
   */
  private static SchemaRegistry registryRejectingNative() {
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    try {
      when(schemaRegistry.parseSchema(any(Schema.class), anyBoolean(), anyBoolean()))
          .thenThrow(new InvalidSchemaException("not a native schema"));
    } catch (InvalidSchemaException e) {
      throw new AssertionError(e); // unreachable: stubbing, not invoking
    }
    return schemaRegistry;
  }

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
