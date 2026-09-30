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

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import org.junit.jupiter.api.Test;

/** What {@link LogicalType#equivalent} counts as the same data, and what only documents it. */
class LogicalTypeEquivalenceTest {

  @Test
  void avroDocsDefaultsAndCustomPropertiesAreIgnored() {
    assertThat(avro(field("a", "\"int\"", null))
        .equivalent(avro("{\"name\":\"a\",\"type\":\"int\",\"doc\":\"the a\",\"default\":7,"
            + "\"connect.name\":\"x\"}"))).isTrue();
  }

  @Test
  void avroNamesAliasesTypesAndNullabilityCount() {
    LogicalType a = avro(field("a", "\"int\"", null));
    assertThat(a.equivalent(avro(field("b", "\"int\"", null)))).isFalse();
    assertThat(a.equivalent(avro("{\"name\":\"a\",\"type\":\"int\",\"aliases\":[\"z\"]}")))
        .isFalse();
    assertThat(a.equivalent(avro(field("a", "\"long\"", null)))).isFalse();
    assertThat(a.equivalent(avro(field("a", "[\"null\",\"int\"]", "null")))).isFalse();
  }

  @Test
  void avroDecimalScaleCounts() {
    String decimal = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,"
        + "\"scale\":%d}";
    assertThat(avro(field("d", String.format(decimal, 2), null))
        .equivalent(avro(field("d", String.format(decimal, 4), null)))).isFalse();
  }

  @Test
  void aRecursiveAvroTypeIsComparedWhereItRecurs() {
    String node = "{\"type\":\"record\",\"name\":\"Node\",\"fields\":[{\"name\":\"next\","
        + "\"type\":[\"null\",\"Node\"],\"default\":null}%s]}";
    assertThat(lt(new AvroSchema(String.format(node, "")))
        .equivalent(lt(new AvroSchema(String.format(node, "").replace("\"Node\",\"fields\"",
            "\"Node\",\"doc\":\"a node\",\"fields\""))))).isTrue();
    assertThat(lt(new AvroSchema(String.format(node, "")))
        .equivalent(lt(new AvroSchema(String.format(node,
            ",{\"name\":\"v\",\"type\":\"int\"}"))))).isFalse();
  }

  @Test
  void jsonDescriptionsAreIgnoredButTitlesAndConstsCount() {
    String u = "{\"type\":\"object\",\"properties\":{\"u\":{\"oneOf\":["
        + "{\"type\":\"object\",%s\"properties\":{\"k\":{\"const\":\"%s\"}}},"
        + "{\"type\":\"string\"}]}}}";
    LogicalType plain = json(String.format(u, "", "a"));
    assertThat(plain.equivalent(json(String.format(u, "\"description\":\"d\",", "a")))).isTrue();
    assertThat(plain.equivalent(json(String.format(u, "\"title\":\"T\",", "a")))).isFalse();
    assertThat(plain.equivalent(json(String.format(u, "", "b")))).isFalse();
  }

  @Test
  void protobufOptionsAndServicesAreIgnoredButFieldNumbersCount() {
    LogicalType plain = proto("int32 id = 1;\n  string memo = 2;", "");
    assertThat(plain.equivalent(proto("option deprecated = true;\n  int32 id = 1;\n"
        + "  string memo = 2 [deprecated = true, json_name = \"MEMO\"];",
        "service S {\n  rpc Get(Row) returns (Row);\n}\n"))).isTrue();
    assertThat(plain.equivalent(proto("int32 id = 1;\n  string memo = 3;", ""))).isFalse();
  }

  private static String field(String name, String type, String defaultValue) {
    return "{\"name\":\"" + name + "\",\"type\":" + type
        + (defaultValue != null ? ",\"default\":" + defaultValue : "") + "}";
  }

  private static LogicalType avro(String field) {
    return lt(new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"fields\":[" + field + "]}"));
  }

  private static LogicalType json(String schema) {
    return JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(schema), LogicalTypeVersion.V1);
  }

  private static LogicalType proto(String members, String trailer) {
    return lt(new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage Row {\n  " + members
        + "\n}\n" + trailer));
  }

  private static LogicalType lt(ParsedSchema schema) {
    return LogicalTypeConversion.toLogicalType(schema);
  }
}
