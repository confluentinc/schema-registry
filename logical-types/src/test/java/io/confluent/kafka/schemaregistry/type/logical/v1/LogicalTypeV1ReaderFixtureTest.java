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

package io.confluent.kafka.schemaregistry.type.logical.v1;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.v1.ProtoMappingsV1;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that LT's reader successfully parses every Flink-style fixture
 * file shipped under {@code src/test/resources/schema/}. For shapes V1
 * emission can produce, also asserts the resulting LT Schema matches the
 * round-trip equivalent in {@link ProtoMappingsV1}/{@link JsonMappingsV1}.
 *
 * <p>For shapes V1 emission cannot produce (proto2 defaults, oneof, JSON
 * one-of unions, multi-message proto), the test only verifies the reader
 * doesn't throw — these are reader-only contract guarantees.
 */
class LogicalTypeV1ReaderFixtureTest {

  // ---------- Proto fixtures ----------

  @Test
  void readsAllSimpleTypesProto() {
    Schema actual = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(readResource("schema/proto/all_simple_types.proto")));
    assertThat(actual).isEqualTo(ProtoMappingsV1.allSimpleTypesCase().getRootSchema());
  }

  @Test
  void readsStringsProto() {
    Schema actual = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(readResource("schema/proto/strings.proto")));
    assertThat(actual).isEqualTo(ProtoMappingsV1.stringTypesCase().getRootSchema());
  }

  @Test
  void readsNotNullMessageTypesProto() {
    Schema actual = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(readResource("schema/proto/not_null_message_types.proto")));
    assertThat(actual).isEqualTo(ProtoMappingsV1.notNullMessageTypesCase().getRootSchema());
  }

  @Test
  void readsNullableArraysProto() {
    Schema actual = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(readResource("schema/proto/nullable_arrays.proto")));
    assertThat(actual).isEqualTo(ProtoMappingsV1.nullableArraysCase().getRootSchema());
  }

  @Test
  void readsNullableCollectionsProto() {
    Schema actual = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(readResource("schema/proto/nullable_collections.proto")));
    assertThat(actual).isEqualTo(ProtoMappingsV1.nullableCollectionsCase().getRootSchema());
  }

  /**
   * proto2 with `required` and `[default = N]` annotations. V1 emission can't
   * produce this shape — but LT's reader must accept it without throwing.
   * Only checks parse success.
   */
  @Test
  void readsDefaultsProtoReaderOnly() {
    Schema actual = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(readResource("schema/proto/defaults.proto")));
    assertThat(actual).isNotNull();
    assertThat(actual.getType()).isEqualTo(Schema.Type.STRUCT);
  }

  /**
   * Proto file with a {@code oneof}. V1 throws on UNION emission — but LT's
   * reader must accept oneof-shaped schemas without throwing.
   */
  @Test
  void readsOneofProtoReaderOnly() {
    Schema actual = ProtoToLogicalTypeConverter.toRootSchema(
        new ProtobufSchema(readResource("schema/proto/oneof.proto")));
    assertThat(actual).isNotNull();
  }

  // ---------- JSON fixtures ----------

  @Test
  void readsNullableTopLevelRecordJsonReaderOnly() {
    Schema actual = JsonToLogicalTypeConverter.toRootSchema(
        new JsonSchema(readResource("schema/json/nullable-top-level-record.json")));
    assertThat(actual).isNotNull();
  }

  @Test
  void readsIgnoredAdditionalPropertiesJsonReaderOnly() {
    Schema actual = JsonToLogicalTypeConverter.toRootSchema(
        new JsonSchema(readResource("schema/json/ignored-additionalProperties.json")));
    assertThat(actual).isNotNull();
  }

  /**
   * Schema with one-of unions — V1 throws on UNION emission. Reader must
   * still accept these legitimate Flink-emitted shapes.
   */
  @Test
  void readsOneOfDecimalTypesJsonReaderOnly() {
    Schema actual = JsonToLogicalTypeConverter.toRootSchema(
        new JsonSchema(readResource("schema/json/one-of-decimal-types.json")));
    assertThat(actual).isNotNull();
  }

  @Test
  void readsNullableReferenceJsonReaderOnly() {
    Schema actual = JsonToLogicalTypeConverter.toRootSchema(
        new JsonSchema(readResource("schema/json/nullable-reference.json")));
    assertThat(actual).isNotNull();
  }

  // ---------- helpers ----------

  private static String readResource(String path) {
    try (InputStream in = LogicalTypeV1ReaderFixtureTest.class
        .getClassLoader().getResourceAsStream(path)) {
      if (in == null) {
        throw new IllegalStateException("Missing resource: " + path);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
