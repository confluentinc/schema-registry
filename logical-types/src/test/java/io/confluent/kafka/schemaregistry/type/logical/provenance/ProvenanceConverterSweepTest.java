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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Every schema either has provenance, with names for every location, or is rejected with a named
 * condition — no logical form, or a recursive type. Anything else a converter throws would reach
 * the registry as a server error, so it is a converter to fix.
 */
class ProvenanceConverterSweepTest {

  private static final String OBJ = "{\"type\":\"object\",\"properties\":";

  static Stream<Arguments> corpus() throws IOException {
    List<Arguments> corpus = new ArrayList<>();
    fixtures("schema/json", ".json", JsonSchema::new, corpus);
    fixtures("schema/proto", ".proto", ProtobufSchema::new, corpus);
    // Avro shapes near the edges of what the converter accepts.
    for (String type : new String[] {
        "\"null\"", "[\"null\"]", "[\"int\"]", "{\"type\":\"int\",\"connect.type\":\"int32\"}",
        "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\",\"flink.precision\":6}",
        "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":\"x\",\"scale\":2}",
        "{\"type\":\"array\",\"logicalType\":\"LogicalMap\",\"items\":{\"type\":\"record\","
            + "\"name\":\"E\",\"fields\":[{\"name\":\"key\",\"type\":\"int\"}]}}",
        "{\"type\":\"array\",\"flink.type\":\"multiset\",\"items\":{\"type\":\"record\","
            + "\"name\":\"MapEntry\",\"namespace\":\"io.confluent.connect.avro\",\"fields\":["
            + "{\"name\":\"key\",\"type\":\"int\"},{\"name\":\"value\",\"type\":\"string\"}]}}",
        "{\"type\":\"fixed\",\"name\":\"D\",\"size\":16,\"logicalType\":\"duration\"}",
        // Annotations of the wrong JSON type: rejected by name, never a failed cast.
        "{\"type\":\"string\",\"flink.maxLength\":\"10\"}",
        "{\"type\":\"string\",\"flink.minLength\":true,\"flink.maxLength\":5}",
        "{\"type\":\"bytes\",\"flink.maxLength\":10.5}",
        "{\"type\":\"enum\",\"name\":\"E1\",\"symbols\":[\"A\"],\"confluent:enum\":[\"x\"]}",
        "{\"type\":\"enum\",\"name\":\"E2\",\"symbols\":[\"A\"],\"confluent:enum\":[{\"doc\":5}]}",
        "{\"type\":\"map\",\"values\":\"int\",\"logical.key.length\":5,"
            + "\"logical.key.type\":7}",
        // A default of a JSON shape its type cannot take, from before defaults were validated.
        "\"bytes\",\"default\":[1]",
        "{\"type\":\"record\",\"name\":\"In\",\"fields\":[{\"name\":\"b\",\"type\":\"int\"}]},"
            + "\"default\":[1]",
        "[{\"type\":\"map\",\"values\":\"int\"},\"null\"],\"default\":[1]",
        "{\"type\":\"record\",\"name\":\"N\",\"fields\":[{\"name\":\"n\",\"type\":[\"null\","
            + "\"N\"]}]}"}) {
      corpus.add(Arguments.of("avro " + type, new AvroSchema(
          "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"f\",\"type\":" + type
              + "}]}")));
    }
    // Protobuf well-known types that are no leaves, and a recursive message.
    for (String field : new String[] {
        "google.protobuf.Struct s = 1;", "google.protobuf.Value v = 1;",
        "google.protobuf.ListValue l = 1;", "google.protobuf.Any a = 1;",
        "google.protobuf.Duration d = 1;", "google.protobuf.FieldMask m = 1;",
        "google.protobuf.Empty e = 1;", "Row next = 1;"}) {
      corpus.add(Arguments.of("proto " + field, new ProtobufSchema("syntax = \"proto3\";\n"
          + "package p;\nimport \"google/protobuf/struct.proto\";\n"
          + "import \"google/protobuf/any.proto\";\nimport \"google/protobuf/duration.proto\";\n"
          + "import \"google/protobuf/field_mask.proto\";\nimport \"google/protobuf/empty.proto\";\n"
          + "message Row {\n  " + field + "\n}\n")));
    }
    // JSON Schema keywords the converter may not model.
    for (String property : new String[] {
        "{\"type\":\"object\",\"connect.type\":\"map\"}",
        "{\"type\":\"array\",\"items\":[{\"type\":\"integer\"},{\"type\":\"string\"}]}",
        "{\"type\":\"array\",\"items\":false}", "{\"type\":\"array\"}",
        "{\"const\":5}", "{\"enum\":[1,\"a\",null]}", "{\"not\":{\"type\":\"string\"}}",
        "{\"if\":{\"type\":\"string\"},\"then\":{\"minLength\":1},\"else\":{\"type\":\"integer\"}}",
        "{\"type\":[\"string\",\"integer\",\"null\"]}", "{\"type\":\"object\"}", "{}", "true",
        "{\"type\":\"object\",\"patternProperties\":{\"^x\":{\"type\":\"integer\"}}}",
        "{\"type\":\"object\",\"additionalProperties\":false}",
        "{\"allOf\":[{\"type\":\"string\"},{\"type\":\"integer\"}]}",
        "{\"anyOf\":[]}", "{\"$ref\":\"#\"}"}) {
      corpus.add(Arguments.of("json " + property,
          new JsonSchema(OBJ + "{\"p\":" + property + "}}")));
    }
    for (String field : new String[] {
        "string s = 1 " + meta("flink.maxLength", "abc") + ";",
        "string s = 1 " + meta("flink.minLength", "abc") + ";",
        "bytes s = 1 " + meta("flink.maxLength", "x") + ";",
        "google.protobuf.Timestamp s = 1 " + meta("flink.precision", "abc") + ";",
        "google.type.TimeOfDay s = 1 " + meta("flink.precision", "abc") + ";",
        "int32 s = 1 " + meta("logical.default", "abc") + ";",
        "bytes s = 1 " + meta("logical.default", "%%%") + ";",
        "google.protobuf.Timestamp s = 1 " + meta("logical.default", "junk") + ";"}) {
      corpus.add(Arguments.of("proto " + field, new ProtobufSchema("syntax = \"proto3\";\n"
          + "package p;\nimport \"confluent/meta.proto\";\n"
          + "import \"google/protobuf/timestamp.proto\";\nimport \"google/type/timeofday.proto\";\n"
          + "message Row {\n  " + field + "\n}\n")));
    }
    return corpus.stream();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("corpus")
  void provenanceIsComputedOrTheSchemaIsRejectedByName(String label, ParsedSchema schema) {
    List<ProvenanceField> fields;
    try {
      fields = ProvenanceHistory.compute("s",
          Collections.singletonList(new ProvenanceHistory.Entry(1, 1, false)),
          Collections.singletonList(schema)).getVersions().get(0).getFields();
    } catch (ValidationException | RecursiveTypeException e) {
      return;
    }
    assertThat(fields).allSatisfy(f -> assertThat(f.getNames()).as("names at %s", f.getPath())
        .isNotNull());
  }

  // -------------------------------------------------------------------------------------------

  private static void fixtures(String dir, String suffix, Function<String, ParsedSchema> parse,
      List<Arguments> corpus) throws IOException {
    Path root = Paths.get("src/test/resources", dir);
    try (Stream<Path> files = Files.list(root)) {
      for (Path file : (Iterable<Path>) files.sorted()::iterator) {
        if (file.toString().endsWith(suffix)) {
          corpus.add(Arguments.of(dir + "/" + file.getFileName(),
              parse.apply(new String(Files.readAllBytes(file), StandardCharsets.UTF_8))));
        }
      }
    }
  }

  private static String meta(String key, String value) {
    return "[(confluent.field_meta) = { params: [{ key: \"" + key + "\" value: \"" + value
        + "\" }] }]";
  }

}
