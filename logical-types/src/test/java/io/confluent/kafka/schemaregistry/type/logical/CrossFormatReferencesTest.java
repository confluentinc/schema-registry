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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;
import java.util.Map;

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
}
