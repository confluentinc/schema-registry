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

package io.confluent.kafka.schemaregistry.type.logical;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import com.sun.net.httpserver.HttpServer;
import org.apache.avro.SchemaParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A schema whose references are in its own format, emitted in another: the references can't come
 * along, so each emitter refuses, naming them and the format they are in, rather than failing
 * in a parser or emitting a reference to a schema of the wrong format. Which format a type's
 * references are in is what its reader recorded, not anything read off their text.
 */
class CrossFormatReferencesTest {

  private static final String ADDRESS = "{\"type\":\"record\",\"name\":\"PostalAddress\","
      + "\"fields\":[{\"name\":\"street\",\"type\":\"string\"}]}";

  // An Avro record with one field of a type its one reference declares
  private static LogicalType avroReferring(String type, String reference, String text) {
    return LogicalTypeConversion.toLogicalType(new AvroSchema(
        "{\"type\":\"record\",\"name\":\"Customer\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"addr\",\"type\":\"" + type + "\"}]}",
        List.of(new SchemaReference(reference, reference, 1)), Map.of(reference, text), null));
  }

  private static LogicalType avroWithReference() {
    return avroReferring("PostalAddress", "PostalAddress", ADDRESS);
  }

  private static LogicalType protobufWithReference() {
    return LogicalTypeConversion.toLogicalType(new ProtobufSchema(
        "syntax = \"proto3\";\nimport \"address.proto\";\n"
            + "message Customer {\n  string id = 1;\n  PostalAddress addr = 2;\n}\n",
        List.of(new SchemaReference("address.proto", "address", 1)),
        Map.of("address.proto",
            "syntax = \"proto3\";\nmessage PostalAddress {\n  string street = 1;\n}\n"),
        null, null));
  }

  // A JSON Schema whose addr refers to {@code ref}, in the one reference address.json
  private static LogicalType jsonReferring(String ref, String text) {
    return LogicalTypeConversion.toLogicalType(new JsonSchema(
        "{\"type\":\"object\",\"title\":\"Customer\",\"properties\":{"
            + "\"id\":{\"type\":\"string\"},\"addr\":{\"$ref\":\"" + ref + "\"}},"
            + "\"required\":[\"id\",\"addr\"],\"additionalProperties\":false}",
        List.of(new SchemaReference("address.json", "address", 1)),
        Map.of("address.json", text), null));
  }

  // A reference into the referenced document's $defs, which reads as an external type
  private static LogicalType jsonWithReference() {
    return jsonReferring("address.json#/$defs/PostalAddress",
        "{\"$defs\":{\"PostalAddress\":{\"type\":\"object\","
            + "\"properties\":{\"street\":{\"type\":\"string\"}},"
            + "\"required\":[\"street\"],\"additionalProperties\":false}}}");
  }

  // Refused for emitting, in one format, references named as being in another
  private static void refused(Executable emit, String target, String source, String names) {
    ValidationException e = assertThrows(ValidationException.class, emit);
    assertEquals("Cannot emit to " + target + ": the schema was read from " + source
        + ", and its references " + names + " are in " + source + " too, which a schema in "
        + target + " cannot refer to. Inline the types they declare, or reference schemas in "
        + target + ".", e.getMessage());
  }

  private static ParsedSchema toAvro(LogicalType lt) {
    return LogicalTypeToAvroConverter.fromLogicalType(lt, "Customer");
  }

  private static ParsedSchema toProtobuf(LogicalType lt) {
    return LogicalTypeToProtoConverter.fromLogicalType(lt, "Customer");
  }

  private static ParsedSchema toJson(LogicalType lt) {
    return LogicalTypeToJsonConverter.fromLogicalType(lt, "Customer");
  }

  @Test
  void avroReferenceRefusedByProtobuf() {
    LogicalType lt = avroWithReference();
    refused(() -> toProtobuf(lt), "Protobuf", "Avro", "[PostalAddress]");
  }

  @Test
  void avroReferenceRefusedByJson() {
    LogicalType lt = avroWithReference();
    refused(() -> toJson(lt), "JSON Schema", "Avro", "[PostalAddress]");
  }

  @Test
  void protobufReferenceRefusedByAvro() {
    LogicalType lt = protobufWithReference();
    refused(() -> toAvro(lt), "Avro", "Protobuf", "[address.proto]");
  }

  @Test
  void protobufReferenceRefusedByJson() {
    LogicalType lt = protobufWithReference();
    refused(() -> toJson(lt), "JSON Schema", "Protobuf", "[address.proto]");
  }

  @Test
  void jsonReferenceRefusedByAvro() {
    LogicalType lt = jsonWithReference();
    refused(() -> toAvro(lt), "Avro", "JSON Schema", "[address.json]");
  }

  @Test
  void jsonReferenceRefusedByProtobuf() {
    LogicalType lt = jsonWithReference();
    refused(() -> toProtobuf(lt), "Protobuf", "JSON Schema", "[address.json]");
  }

  // Whatever the reference's text looks like: these read as neither Avro nor anything else.

  @Test
  void avroReferenceThatIsAnArrayOfARecordRefusedByJson() {
    LogicalType lt = avroReferring("Item", "items", "{\"type\":\"array\",\"items\":"
        + "{\"type\":\"record\",\"name\":\"Item\",\"fields\":["
        + "{\"name\":\"s\",\"type\":\"string\"}]}}");
    refused(() -> toJson(lt), "JSON Schema", "Avro", "[items]");
  }

  @Test
  void avroReferenceThatIsAFixedRefusedByJsonAndProtobuf() {
    LogicalType lt = avroReferring("Md5", "Md5",
        "{\"type\":\"fixed\",\"name\":\"Md5\",\"size\":16}");
    refused(() -> toJson(lt), "JSON Schema", "Avro", "[Md5]");
    refused(() -> toProtobuf(lt), "Protobuf", "Avro", "[Md5]");
  }

  // A reference to a whole document, which also reads as a JSON-specific external import
  @Test
  void jsonReferenceToAWholeDocumentIsNamedToo() {
    LogicalType lt = jsonReferring("address.json", "{\"type\":\"object\","
        + "\"title\":\"PostalAddress\",\"properties\":{\"street\":{\"type\":\"string\"}}}");
    assertFalse(lt.getExternalImports().isEmpty());
    refused(() -> toAvro(lt), "Avro", "JSON Schema", "[address.json]");
    refused(() -> toProtobuf(lt), "Protobuf", "JSON Schema", "[address.json]");
  }

  @Test
  void eachReaderRecordsItsFormatWhichIsNoPartOfTheType() {
    LogicalType avro = avroWithReference();
    assertEquals(SchemaType.AVRO, avro.getSourceFormat());
    assertEquals(SchemaType.PROTOBUF, protobufWithReference().getSourceFormat());
    assertEquals(SchemaType.JSON, jsonWithReference().getSourceFormat());

    LogicalType unrecorded = avro.withSourceFormat(null);
    assertNull(unrecorded.getSourceFormat());
    assertEquals(avro, unrecorded);
    assertEquals(avro.hashCode(), unrecorded.hashCode());
  }

  // A record with one column and these references, none of which the record uses, and no
  // recorded format, as a type built by hand or from a DDL script has
  private static LogicalType withReferences(Map<String, String> resolved) {
    List<SchemaReference> references = resolved.keySet().stream()
        .map(name -> new SchemaReference(name, name, 1)).collect(Collectors.toList());
    Schema root = Schema.createStruct(List.of(
        new Schema.Field("id", Schema.createString().setNullable(false), 0))).setNullable(false);
    return new LogicalType(root, Map.of(), references, resolved);
  }

  @Test
  void aTypeWithNoRecordedFormatIsNotChecked() {
    assertNotNull(toJson(withReferences(Map.of("PostalAddress", ADDRESS))));
  }

  // References in the emitted format are left alone, however they fail or whatever they hold.

  @Test
  void avroReferencesRedefiningANameKeepAvrosError() {
    Map<String, String> resolved = new LinkedHashMap<>();
    // A record with an attribute that is a JSON Schema keyword is an Avro schema all the same.
    resolved.put("Other", "{\"type\":\"record\",\"name\":\"Other\",\"namespace\":\"com.example\","
        + "\"properties\":{\"owner\":\"team\"},\"fields\":[{\"name\":\"n\",\"type\":\"int\"}]}");
    resolved.put("com.example.Address", "{\"type\":\"record\",\"name\":\"Address\","
        + "\"namespace\":\"com.example\",\"fields\":[{\"name\":\"street\",\"type\":\"string\"}]}");
    resolved.put("com.example.AddressV2", "{\"type\":\"record\",\"name\":\"Address\","
        + "\"namespace\":\"com.example\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}");
    LogicalType lt = withReferences(resolved).withSourceFormat(SchemaType.AVRO);

    SchemaParseException e = assertThrows(SchemaParseException.class, () -> toAvro(lt));
    assertTrue(e.getMessage().contains("Can't redefine: com.example.Address"), e.getMessage());
  }

  @Test
  void protobufReferenceThatDoesNotParseKeepsItsError() {
    LogicalType lt = withReferences(Map.of("e.proto",
        "edition = \"2023\";\nmessage M {\n  string s = 1;\n}\n"))
        .withSourceFormat(SchemaType.PROTOBUF);

    Exception e = assertThrows(Exception.class, () -> toProtobuf(lt));
    assertFalse(e instanceof ValidationException, e.toString());
    assertTrue(e.getMessage().contains("edition"), e.getMessage());
  }

  // Only pointed into, the document's own root says nothing of the format it is in.
  @Test
  void jsonReferenceWhoseRootLooksLikeAvroConverts() {
    LogicalType lt = jsonReferring("address.json#/definitions/PostalAddress",
        "{\"type\":\"record\",\"definitions\":{\"PostalAddress\":{\"type\":\"object\","
            + "\"properties\":{\"street\":{\"type\":\"string\"}}}}}");

    assertTrue(toJson(lt).canonicalString().contains("address.json#/$defs/PostalAddress"));
  }

  @Test
  void jsonReferenceToItsSiblingByRelativePathConverts() {
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("defs/a.json", "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"b\":{\"$ref\":\"b.json\"}}}");
    resolved.put("defs/b.json", "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"string\"}");

    assertNotNull(toJson(withReferences(resolved).withSourceFormat(SchemaType.JSON)));
  }

  @Test
  void jsonBooleanReferenceBesideAnObjectOneConverts() {
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("any.json", "true");
    resolved.put("obj.json",
        "{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"string\"}}}");

    assertNotNull(toJson(withReferences(resolved).withSourceFormat(SchemaType.JSON)));
  }

  @Test
  void jsonReferencesRemoteRefIsNotFetched() throws IOException {
    AtomicInteger fetches = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", exchange -> {
      fetches.incrementAndGet();
      exchange.sendResponseHeaders(404, -1);
      exchange.close();
    });
    server.start();
    try {
      String remote = "http://localhost:" + server.getAddress().getPort() + "/x.json";
      assertNotNull(toJson(withReferences(Map.of("r.json", "{\"type\":\"object\","
          + "\"properties\":{\"x\":{\"$ref\":\"" + remote + "\"}}}"))
          .withSourceFormat(SchemaType.JSON)));
      assertEquals(0, fetches.get(), "the reference's remote $ref was fetched");
    } finally {
      server.stop(0);
    }
  }
}
