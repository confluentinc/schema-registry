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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A schema whose references are in its own format, emitted in another: the references can't come
 * along, so each emitter refuses, naming the reference, rather than failing in a parser or
 * emitting a reference to a schema of the wrong format.
 */
class CrossFormatReferencesTest {

  private static LogicalType avroWithReference() {
    return LogicalTypeConversion.toLogicalType(new AvroSchema(
        "{\"type\":\"record\",\"name\":\"Customer\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"addr\",\"type\":[\"null\",\"PostalAddress\"],\"default\":null}]}",
        List.of(new SchemaReference("PostalAddress", "PostalAddress", 1)),
        Map.of("PostalAddress", "{\"type\":\"record\",\"name\":\"PostalAddress\",\"fields\":["
            + "{\"name\":\"street\",\"type\":\"string\"}]}"),
        null));
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

  // A reference into the referenced document's $defs, which reads as an external type
  private static LogicalType jsonWithReference() {
    return LogicalTypeConversion.toLogicalType(new JsonSchema(
        "{\"type\":\"object\",\"title\":\"Customer\",\"properties\":{"
            + "\"id\":{\"type\":\"string\"},"
            + "\"addr\":{\"$ref\":\"address.json#/$defs/PostalAddress\"}},"
            + "\"required\":[\"id\",\"addr\"],\"additionalProperties\":false}",
        List.of(new SchemaReference("address.json", "address", 1)),
        Map.of("address.json", "{\"$defs\":{\"PostalAddress\":{\"type\":\"object\","
            + "\"properties\":{\"street\":{\"type\":\"string\"}},"
            + "\"required\":[\"street\"],\"additionalProperties\":false}}}"),
        null));
  }

  private static void refused(Executable emit, String expected) {
    ValidationException e = assertThrows(ValidationException.class, emit);
    assertTrue(e.getMessage().contains(expected), e.getMessage());
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
    refused(() -> toProtobuf(lt), "referenced schema 'PostalAddress' is not a Protobuf schema");
  }

  @Test
  void avroReferenceRefusedByJson() {
    LogicalType lt = avroWithReference();
    refused(() -> toJson(lt), "referenced schema 'PostalAddress' is not a JSON Schema");
  }

  @Test
  void protobufReferenceRefusedByAvro() {
    LogicalType lt = protobufWithReference();
    refused(() -> toAvro(lt), "referenced schema 'address.proto' is not an Avro schema");
  }

  @Test
  void protobufReferenceRefusedByJson() {
    LogicalType lt = protobufWithReference();
    refused(() -> toJson(lt), "referenced schema 'address.proto' is not a JSON Schema");
  }

  @Test
  void jsonReferenceRefusedByAvro() {
    LogicalType lt = jsonWithReference();
    refused(() -> toAvro(lt), "referenced schema 'address.json' is not an Avro schema");
  }

  @Test
  void jsonReferenceRefusedByProtobuf() {
    LogicalType lt = jsonWithReference();
    refused(() -> toProtobuf(lt), "referenced schema 'address.json' is not a Protobuf schema");
  }

  // References in the emitted format are left alone: only another format is refused.

  // A record with one column and these references, none of which the record uses
  private static LogicalType withReferences(Map<String, String> resolved) {
    List<SchemaReference> references = resolved.keySet().stream()
        .map(name -> new SchemaReference(name, name, 1)).collect(Collectors.toList());
    Schema root = Schema.createStruct(List.of(
        new Schema.Field("id", Schema.createString().setNullable(false), 0))).setNullable(false);
    return new LogicalType(root, Map.of(), references, resolved);
  }

  @Test
  void avroReferencesRedefiningANameKeepAvrosError() {
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("com.example.Address", "{\"type\":\"record\",\"name\":\"Address\","
        + "\"namespace\":\"com.example\",\"fields\":[{\"name\":\"street\",\"type\":\"string\"}]}");
    resolved.put("com.example.AddressV2", "{\"type\":\"record\",\"name\":\"Address\","
        + "\"namespace\":\"com.example\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}");
    LogicalType lt = withReferences(resolved);

    SchemaParseException e = assertThrows(SchemaParseException.class, () -> toAvro(lt));
    assertFalse(e.getMessage().contains("is not an Avro schema"), e.getMessage());
  }

  @Test
  void jsonReferenceToItsSiblingByRelativePathConverts() {
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("defs/a.json", "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"b\":{\"$ref\":\"b.json\"}}}");
    resolved.put("defs/b.json", "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"string\"}");

    assertNotNull(toJson(withReferences(resolved)));
  }

  @Test
  void jsonBooleanReferenceBesideAnObjectOneConverts() {
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("any.json", "true");
    resolved.put("obj.json",
        "{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"string\"}}}");

    assertNotNull(toJson(withReferences(resolved)));
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
          + "\"properties\":{\"x\":{\"$ref\":\"" + remote + "\"}}}"))));
      assertEquals(0, fetches.get(), "the reference's remote $ref was fetched");
    } finally {
      server.stop(0);
    }
  }
}
