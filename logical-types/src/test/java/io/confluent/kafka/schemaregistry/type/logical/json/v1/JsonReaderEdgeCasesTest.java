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

package io.confluent.kafka.schemaregistry.type.logical.json.v1;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JSON Schema reader edge cases ported from Flink's
 * {@code JsonToFlinkSchemaConverterTest}: cyclic refs, JSON tuples, and
 * mixed-type allOf.
 *
 * <p>Each case asserts the reader throws (matching Flink's behavior). If
 * LT's behavior differs from Flink's, these tests document the divergence.
 */
class JsonReaderEdgeCasesTest {

  /**
   * JSON Schema with a cyclic {@code $ref} chain.
   *
   * <p>Behavioral divergence from Flink: Flink's reader throws
   * {@code ValidationException} ("Cyclic schemas are not supported"). LT
   * accepts cycles by representing each {@code $ref} as a NAMED_TYPE_REF
   * pointing into {@code localNamedTypes}. The cycle becomes a chain of
   * named-type pointers that loop back, never traversed at conversion time.
   *
   * <p>This is consistent with LT's Avro reader, which also models cyclic
   * Avro records via NAMED_TYPE_REF chains rather than rejecting them
   * (verified in {@code AvroToLogicalTypeConverterTest#testRecordWithCycles}).
   */
  @Test
  void testCyclicDependency() {
    // Note: omits $schema declaration. Drafts predating 2019-09 (e.g. draft-07)
    // don't recognize $defs, in which case everit leaves it as an
    // unprocessed property without parsing the entries as schemas — which
    // breaks the localNamedTypes recovery path. Without $schema, everit
    // defaults to a draft that treats $defs as schema definitions.
    String schemaStr =
        "{\n"
            + "  \"title\": \"IdentifyUserMessage\",\n"
            + "  \"type\": \"object\",\n"
            + "  \"$defs\": {\n"
            + "    \"MessageBase\": {\n"
            + "      \"type\": \"object\",\n"
            + "      \"properties\": {\n"
            + "        \"id\": { \"type\": \"string\" },\n"
            + "        \"fallbackMessage\": { \"$ref\": \"#/$defs/MessageBase\" }\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  \"properties\": {\n"
            + "    \"msg\": { \"$ref\": \"#/$defs/MessageBase\" }\n"
            + "  }\n"
            + "}";

    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(
        new JsonSchema(schemaStr));

    // Successful parse — no throw. Root is a STRUCT and the cyclic field
    // resolves to a NAMED_TYPE_REF (rather than infinite recursion).
    assertThat(lt.getRootSchema().getType()).isEqualTo(Schema.Type.STRUCT);
    Schema msgSchema = lt.getRootSchema().getField("msg").getSchema();
    assertThat(msgSchema.getType()).isEqualTo(Schema.Type.NAMED_TYPE_REF);
    assertThat(msgSchema.getQualifiedName()).isEqualTo("MessageBase");
  }

  /**
   * JSON tuple type — array with a positional {@code items} list. Flink's
   * reader throws {@code ValidationException} because it can't represent
   * heterogeneous-element arrays.
   */
  @Test
  void testJsonTuples() {
    String schemaStr =
        "{\n"
            + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
            + "  \"title\": \"IdentifyUserMessage\",\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"msg\": {\n"
            + "      \"type\": \"array\",\n"
            + "      \"items\": [{\"type\": \"string\"}]\n"
            + "    }\n"
            + "  }\n"
            + "}";
    assertThatThrownBy(() -> JsonToLogicalTypeConverter.toRootSchema(
            new JsonSchema(schemaStr)))
        .isInstanceOf(RuntimeException.class);
  }

  /**
   * {@code allOf} mixing string + object — semantically nonsensical. Flink's
   * reader throws {@code IllegalArgumentException} ("Unsupported criterion
   * allOf for ..."); LT's reader should reject too.
   */
  @Test
  void testUnsupportedAllOfCombinedSchema() {
    String schemaStr = readResource("schema/json/mixed-type-all-of.json");
    assertThatThrownBy(() -> JsonToLogicalTypeConverter.toRootSchema(
            new JsonSchema(schemaStr)))
        .isInstanceOf(RuntimeException.class);
  }

  private static String readResource(String path) {
    try (InputStream in = JsonReaderEdgeCasesTest.class
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
