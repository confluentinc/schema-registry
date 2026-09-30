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

import static io.confluent.kafka.schemaregistry.type.logical.provenance.IdentityPolicy.AVRO;
import static io.confluent.kafka.schemaregistry.type.logical.provenance.IdentityPolicy.JSON;
import static io.confluent.kafka.schemaregistry.type.logical.provenance.IdentityPolicy.PROTOBUF;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import org.junit.jupiter.api.Test;

/** What {@link LogicalType#equivalent} counts as the same data, and what only documents it. */
class LogicalTypeEquivalenceTest {

  @Test
  void avroDocsDefaultsAndCustomPropertiesAreIgnored() {
    assertThat(avro(field("a", "\"int\"", null))
        .equivalent(avro("{\"name\":\"a\",\"type\":\"int\",\"doc\":\"the a\",\"default\":7,"
            + "\"connect.name\":\"x\"}"), AVRO)).isTrue();
  }

  @Test
  void avroNamesAliasesTypesAndNullabilityCount() {
    LogicalType a = avro(field("a", "\"int\"", null));
    assertThat(a.equivalent(avro(field("b", "\"int\"", null)), AVRO)).isFalse();
    assertThat(a.equivalent(avro("{\"name\":\"a\",\"type\":\"int\",\"aliases\":[\"z\"]}"), AVRO))
        .isFalse();
    assertThat(a.equivalent(avro(field("a", "\"long\"", null)), AVRO)).isFalse();
    assertThat(a.equivalent(avro(field("a", "[\"null\",\"int\"]", "null")), AVRO)).isFalse();
  }

  @Test
  void avroDecimalScaleCounts() {
    String decimal = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,"
        + "\"scale\":%d}";
    assertThat(avro(field("d", String.format(decimal, 2), null))
        .equivalent(avro(field("d", String.format(decimal, 4), null)), AVRO)).isFalse();
  }

  @Test
  void aRecursiveAvroTypeIsComparedWhereItRecurs() {
    String node = "{\"type\":\"record\",\"name\":\"Node\",\"fields\":[{\"name\":\"next\","
        + "\"type\":[\"null\",\"Node\"],\"default\":null}%s]}";
    assertThat(lt(new AvroSchema(String.format(node, "")))
        .equivalent(lt(new AvroSchema(String.format(node, "").replace("\"Node\",\"fields\"",
            "\"Node\",\"doc\":\"a node\",\"fields\""))), AVRO)).isTrue();
    assertThat(lt(new AvroSchema(String.format(node, "")))
        .equivalent(lt(new AvroSchema(String.format(node,
            ",{\"name\":\"v\",\"type\":\"int\"}"))), AVRO)).isFalse();
  }

  @Test
  void jsonDescriptionsAreIgnoredButTitlesAndConstsCount() {
    String u = "{\"type\":\"object\",\"properties\":{\"u\":{\"oneOf\":["
        + "{\"type\":\"object\",%s\"properties\":{\"k\":{\"const\":\"%s\"}}},"
        + "{\"type\":\"string\"}]}}}";
    LogicalType plain = json(String.format(u, "", "a"));
    assertThat(plain.equivalent(json(String.format(u, "\"description\":\"d\",", "a")), JSON))
        .isTrue();
    assertThat(plain.equivalent(json(String.format(u, "\"title\":\"T\",", "a")), JSON)).isFalse();
    assertThat(plain.equivalent(json(String.format(u, "", "b")), JSON)).isFalse();
  }

  @Test
  void protobufOptionsAndServicesAreIgnoredButFieldNumbersCount() {
    LogicalType plain = proto("int32 id = 1;\n  string memo = 2;", "");
    assertThat(plain.equivalent(proto("option deprecated = true;\n  int32 id = 1;\n"
        + "  string memo = 2 [deprecated = true, json_name = \"MEMO\"];",
        "service S {\n  rpc Get(Row) returns (Row);\n}\n"), PROTOBUF)).isTrue();
    assertThat(plain.equivalent(proto("int32 id = 1;\n  string memo = 3;", ""), PROTOBUF))
        .isFalse();
  }

  @Test
  void aJsonRootsTitleAndNamespaceAreIgnoredButBranchHintsCount() {
    String row = "{\"type\":\"object\",%s\"properties\":{\"u\":{\"oneOf\":["
        + "{\"type\":\"string\"},{\"type\":\"integer\"}]%s}}}";
    LogicalType plain = json(String.format(row, "", ""));
    assertThat(plain.equivalent(json(String.format(row, "\"title\":\"Row\",", "")), JSON)).isTrue();
    assertThat(plain.equivalent(
        json(String.format(row, "\"confluent:namespace\":\"n\",", "")), JSON)).isTrue();
    assertThat(plain.equivalent(json(String.format(row, "",
        ",\"confluent:union\":[{\"name\":\"s\"},{}]")), JSON)).isFalse();
  }

  @Test
  void aProtobufFilesMessageOrderIsIgnoredButItsRootIsNot() {
    String a = "message A {\n  int32 x = 1;\n}\n";
    String b = "message B {\n  string y = 1;\n}\n";
    LogicalType ab = multi(file(a + b));
    assertThat(ab.equivalent(multi(file(b + a)), PROTOBUF)).isTrue();
    assertThat(ab.equivalent(lt(file(a + b)), PROTOBUF)).isFalse();
  }

  @Test
  void protobufMembersArePairedByNameAndNumberInAnyOrder() {
    // Declared in number order, numbers are implied by position; otherwise they are recorded.
    LogicalType plain = proto("int32 id = 1;\n  string memo = 2;\n  oneof k {\n    int32 n = 3;\n"
        + "    string s = 4;\n  }", "");
    assertThat(plain.equivalent(proto("oneof k {\n    string s = 4;\n    int32 n = 3;\n  }\n"
        + "  string memo = 2;\n  int32 id = 1;", ""), PROTOBUF)).isTrue();
    // The same names in the same order, numbered otherwise: each number is data.
    assertThat(plain.equivalent(proto("int32 id = 2;\n  string memo = 1;\n  oneof k {\n"
        + "    int32 n = 3;\n    string s = 4;\n  }", ""), PROTOBUF)).isFalse();
    assertThat(plain.equivalent(proto("int32 id = 1;\n  string memo = 2;\n  oneof k {\n"
        + "    int32 n = 4;\n    string s = 3;\n  }", ""), PROTOBUF)).isFalse();
  }

  @Test
  void protobufEnumConstantsArePairedByNameAndNumberInAnyOrder() {
    String e = "enum E {\n  %s\n}\nmessage Row {\n  E e = 1;\n}\n";
    LogicalType plain = lt(file(String.format(e, "A = 0;\n  B = 1;\n  C = 2;")));
    assertThat(plain.equivalent(lt(file(String.format(e, "A = 0;\n  C = 2;\n  B = 1;"))),
        PROTOBUF)).isTrue();
    assertThat(plain.equivalent(lt(file(String.format(e, "A = 0;\n  C = 1;\n  B = 2;"))),
        PROTOBUF)).isFalse();
  }

  @Test
  void avroFieldsSymbolsAndBranchesArePairedByNameInAnyOrder() {
    String record = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[%s]}";
    String enm = "{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[%s]}}";
    String union = "{\"name\":\"u\",\"type\":[%s]}";
    String id = "{\"name\":\"id\",\"type\":\"int\"}";
    LogicalType plain = lt(new AvroSchema(String.format(record, id + ","
        + String.format(enm, "\"A\",\"B\"") + "," + String.format(union, "\"int\",\"string\""))));
    assertThat(plain.equivalent(lt(new AvroSchema(String.format(record,
        String.format(union, "\"string\",\"int\"") + "," + String.format(enm, "\"B\",\"A\"")
            + "," + id))), AVRO)).isTrue();
  }

  @Test
  void jsonUnionBranchesStillCountByPosition() {
    String u = "{\"u\":{\"oneOf\":[%s]}}";
    String s = "{\"type\":\"string\"}";
    String i = "{\"type\":\"integer\"}";
    assertThat(json("{\"type\":\"object\",\"properties\":" + String.format(u, s + "," + i) + "}")
        .equivalent(json("{\"type\":\"object\",\"properties\":" + String.format(u, i + "," + s)
            + "}"), JSON)).isFalse();
  }

  @Test
  void aJsonRefAndTheBodyItNamesInlineAreEquivalent() {
    String d = "{\"type\":\"object\",\"properties\":{\"k\":{\"type\":\"string\"}}}";
    String byRef = "{\"type\":\"object\",\"properties\":{\"d\":{\"$ref\":\"#/$defs/D\"}},"
        + "\"$defs\":{\"D\":%s}}";
    String inline = "{\"type\":\"object\",\"properties\":{\"d\":%s}}";
    LogicalType referenced = json(String.format(byRef, d));
    assertThat(referenced.equivalent(json(String.format(inline, d)), JSON)).isTrue();
    assertThat(json(String.format(inline, d)).equivalent(referenced, JSON)).isTrue();
    assertThat(referenced.equivalent(json(String.format(inline,
        d.replace("\"k\"", "\"j\""))), JSON)).isFalse();
  }

  @Test
  void aWrappedUnionsBranchNumbersCount() {
    // A Flink wrapper's oneof is numbered on its own, not in its message's sequence.
    assertThat(multi(wrapped(1, 2)).equivalent(multi(wrapped(2, 1)), PROTOBUF)).isFalse();
    assertThat(multi(wrapped(1, 2)).equivalent(multi(wrapped(1, 2)), PROTOBUF)).isTrue();
    // Declaration order is no identity inside a wrapper either.
    assertThat(multi(wrapped(1, 2)).equivalent(multi(wrappedReversed(1, 2)), PROTOBUF)).isTrue();
  }

  @Test
  void aJsonDefThatOnlyReferencesAnotherStandsForIt() {
    String e = "{\"type\":\"object\",\"properties\":{\"k\":{\"type\":\"string\"}}}";
    String chain = "{\"type\":\"object\",\"properties\":{\"d\":{\"$ref\":\"#/definitions/D\"}},"
        + "\"definitions\":{\"D\":{\"$ref\":\"#/definitions/E\"},\"E\":" + e + "}}";
    String direct = "{\"type\":\"object\",\"properties\":{\"d\":{\"$ref\":\"#/definitions/E\"}},"
        + "\"definitions\":{\"E\":" + e + "}}";
    String inline = "{\"type\":\"object\",\"properties\":{\"d\":" + e + "}}";
    String renamed = direct.replace("/E", "/Q").replace("\"E\"", "\"Q\"");
    assertThat(json(chain).equivalent(json(inline), JSON)).isTrue();
    assertThat(json(inline).equivalent(json(chain), JSON)).isTrue();
    assertThat(json(chain).equivalent(json(direct), JSON)).isTrue();
    assertThat(json(direct).equivalent(json(renamed), JSON)).isFalse();
  }

  @Test
  void protobufEnumNumbersCount() {
    String e = "enum E {\n  A = 0;\n  B = %d;\n}\nmessage Row {\n  E e = 1;\n}\n";
    assertThat(lt(file(String.format(e, 1))).equivalent(lt(file(String.format(e, 2))), PROTOBUF))
        .isFalse();
  }

  @Test
  void aliasesOfNestedAvroTypesCount() {
    String inner = "{\"type\":\"record\",\"name\":\"In\"%s,\"fields\":["
        + "{\"name\":\"v\",\"type\":\"int\"}]}";
    // A branch of a proper union: a lone fixed is a binary, which carries no aliases.
    String fixed = "[\"null\",{\"type\":\"fixed\",\"name\":\"F\",\"size\":4%s},\"int\"]";
    String aliased = ",\"aliases\":[\"Old\"]";
    assertThat(avro(field("i", String.format(inner, ""), null))
        .equivalent(avro(field("i", String.format(inner, aliased), null)), AVRO)).isFalse();
    assertThat(avro(field("f", String.format(fixed, ""), "null"))
        .equivalent(avro(field("f", String.format(fixed, aliased), "null")), AVRO)).isFalse();
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

  private static ProtobufSchema file(String body) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\n" + body);
  }

  // Row holding an array of unions, its Flink wrapper's branches a and b numbered as given.
  private static ProtobufSchema wrapped(int a, int b) {
    return wrapper("string a = " + a + ";\n      string b = " + b + ";");
  }

  // As wrapped, with b declared first.
  private static ProtobufSchema wrappedReversed(int a, int b) {
    return wrapper("string b = " + b + ";\n      string a = " + a + ";");
  }

  private static ProtobufSchema wrapper(String branches) {
    return file("import \"confluent/meta.proto\";\nmessage Row {\n  int32 id = 1;\n"
        + "  repeated UW us = 2 [(confluent.field_meta) = {params: [{key: \"flink.wrapped\", "
        + "value: \"true\"}]}];\n  message UW {\n    oneof value {\n      " + branches + "\n"
        + "    }\n  }\n}\n");
  }

  private static LogicalType multi(ProtobufSchema schema) {
    return ProtoToLogicalTypeConverter.toLogicalType(schema, true);
  }

  private static LogicalType lt(ParsedSchema schema) {
    return LogicalTypeConversion.toLogicalType(schema);
  }
}
