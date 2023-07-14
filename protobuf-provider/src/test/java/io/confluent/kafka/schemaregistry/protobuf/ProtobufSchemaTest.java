/*
 * Copyright 2020 Confluent Inc.
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
 *
 */

package io.confluent.kafka.schemaregistry.protobuf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.Format;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils.FormatContext;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.protobuf.diff.ResourceLoader;

import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.PROTO3;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProtobufSchemaTest {

  private static ObjectMapper objectMapper = new ObjectMapper();

  private static final String recordSchemaString = "syntax = \"proto3\";\n"
      + "\n"
      + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
      + "option java_outer_classname = \"TestMessageProtos\";\n"
      + "\n"
      + "import \"google/protobuf/descriptor.proto\";\n"
      + "\n"
      + "message TestMessage {\n"
      + "    string test_string = 1 [json_name = \"test_str\"];\n"
      + "    bool test_bool = 2;\n"
      + "    bytes test_bytes = 3;\n"
      + "    double test_double = 4;\n"
      + "    float test_float = 5;\n"
      + "    fixed32 test_fixed32 = 6;\n"
      + "    fixed64 test_fixed64 = 7;\n"
      + "    int32 test_int32 = 8;\n"
      + "    int64 test_int64 = 9;\n"
      + "    sfixed32 test_sfixed32 = 10;\n"
      + "    sfixed64 test_sfixed64 = 11;\n"
      + "    sint32 test_sint32 = 12;\n"
      + "    sint64 test_sint64 = 13;\n"
      + "    uint32 test_uint32 = 14;\n"
      + "    uint64 test_uint64 = 15;\n"
      + "}\n";

  private static final ProtobufSchema recordSchema = new ProtobufSchema(recordSchemaString);

  private static final String arraySchemaString = "syntax = \"proto3\";\n"
      + "\n"
      + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
      + "option java_outer_classname = \"TestArrayProtos\";\n"
      + "\n"
      + "import \"google/protobuf/descriptor.proto\";\n"
      + "\n"
      + "message TestArray {\n"
      + "    repeated string test_array = 1;\n"
      + "}\n";

  private static final ProtobufSchema arraySchema = new ProtobufSchema(arraySchemaString);

  private static final String mapSchemaString = "syntax = \"proto3\";\n"
      + "\n"
      + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
      + "option java_outer_classname = \"TestMapProtos\";\n"
      + "\n"
      + "import \"google/protobuf/descriptor.proto\";\n"
      + "\n"
      + "message TestMap {\n"
      + "    map<string, string> test_map = 1;\n"
      + "}\n";

  private static final ProtobufSchema mapSchema = new ProtobufSchema(mapSchemaString);

  private static final String unionSchemaString = "syntax = \"proto3\";\n"
      + "\n"
      + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
      + "option java_outer_classname = \"TestUnionProtos\";\n"
      + "\n"
      + "import \"google/protobuf/descriptor.proto\";\n"
      + "\n"
      + "message TestUnion {\n"
      + "    oneof test_oneof {\n"
      + "        string name = 1;\n"
      + "        int32 age = 2;\n"
      + "    }\n"
      + "}\n";

  private static final ProtobufSchema unionSchema = new ProtobufSchema(unionSchemaString);

  private static final String enumSchemaString = "syntax = \"proto3\";\n"
      + "\n"
      + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
      + "option java_outer_classname = \"TestEnumProtos\";\n"
      + "\n"
      + "import \"google/protobuf/descriptor.proto\";\n"
      + "\n"
      + "message TestEnum {\n"
      + "    enum Suit {\n"
      + "        SPADES = 0;\n"
      + "        HEARTS = 1;\n"
      + "        DIAMONDS = 2;\n"
      + "        CLUBS = 3;\n"
      + "    }\n"
      + "    Suit suit = 1;\n"
      + "}\n";

  private static final ProtobufSchema enumSchema = new ProtobufSchema(enumSchemaString);

  private static final String enumBeforeMessageSchemaString = "syntax = \"proto3\";\n"
      + "\n"
      + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
      + "option java_outer_classname = \"TestEnumProtos\";\n"
      + "\n"
      + "enum Suit {\n"
      + "  SPADES = 0;\n"
      + "  HEARTS = 1;\n"
      + "  DIAMONDS = 2;\n"
      + "  CLUBS = 3;\n"
      + "}\n"
      + "message TestEnum {\n"
      + "  int32 suit = 1;\n"
      + "}\n";

  private static final String enumAfterMessageSchemaString = "syntax = \"proto3\";\n"
      + "\n"
      + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
      + "option java_outer_classname = \"TestEnumProtos\";\n"
      + "\n"
      + "message TestEnum {\n"
      + "  int32 suit = 1;\n"
      + "}\n"
      + "enum Suit {\n"
      + "  SPADES = 0;\n"
      + "  HEARTS = 1;\n"
      + "  DIAMONDS = 2;\n"
      + "  CLUBS = 3;\n"
      + "}\n";

  private static final ProtobufSchema enumBeforeMessageSchema =
      new ProtobufSchema(enumBeforeMessageSchemaString);

  private static final String invalidSchemaString = "syntax = \"proto3\";\n"
      + "\n"
      + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
      + "option java_outer_classname = \"TestMessageProtos\";\n"
      + "\n"
      + "import \"google/protobuf/descriptor.proto\";\n"
      + "\n"
      + "message TestMessage {\n"
      + "    string test_string = 1 [json_name = \"test_str\"];\n"
      + "    int32 test_int32 = 8.01;\n"
      + "}\n";

  @Test
  public void testRecordToProtobuf() throws Exception {
    String json = "{\n"
        + "    \"test_string\": \"string\",\n"
        + "    \"test_bool\": true,\n"
        + "    \"test_bytes\": \"aGVsbG8=\",\n"
        // base-64 encoded "hello"
        + "    \"test_double\": 800.25,\n"
        + "    \"test_float\": 23.4,\n"
        + "    \"test_fixed32\": 32,\n"
        + "    \"test_fixed64\": 64,\n"
        + "    \"test_int32\": 32,\n"
        + "    \"test_int64\": 64,\n"
        + "    \"test_sfixed32\": 32,\n"
        + "    \"test_sfixed64\": 64,\n"
        + "    \"test_sint32\": 32,\n"
        + "    \"test_sint64\": 64,\n"
        + "    \"test_uint32\": 32,\n"
        + "    \"test_uint64\": 64\n"
        + "}";

    Object result = ProtobufSchemaUtils.toObject(jsonTree(json), recordSchema);
    assertTrue(result instanceof DynamicMessage);
    DynamicMessage resultRecord = (DynamicMessage) result;
    Descriptor desc = resultRecord.getDescriptorForType();
    FieldDescriptor fd = desc.findFieldByName("test_string");
    assertEquals("string", resultRecord.getField(fd));
    fd = desc.findFieldByName("test_bool");
    assertEquals(true, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_bytes");
    assertEquals("hello", ((ByteString) resultRecord.getField(fd)).toStringUtf8());
    fd = desc.findFieldByName("test_double");
    assertEquals(800.25, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_float");
    assertEquals(23.4f, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_fixed32");
    assertEquals(32, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_fixed64");
    assertEquals(64L, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_int32");
    assertEquals(32, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_int64");
    assertEquals(64L, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_sfixed32");
    assertEquals(32, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_sfixed64");
    assertEquals(64L, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_sint32");
    assertEquals(32, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_sint64");
    assertEquals(64L, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_uint32");
    assertEquals(32, resultRecord.getField(fd));
    fd = desc.findFieldByName("test_uint64");
    assertEquals(64L, resultRecord.getField(fd));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testArrayToProtobuf() throws Exception {
    String json = "{ \"test_array\": [\"one\", \"two\", \"three\"] }";

    Object result = ProtobufSchemaUtils.toObject(jsonTree(json), arraySchema);
    assertTrue(result instanceof DynamicMessage);
    DynamicMessage resultRecord = (DynamicMessage) result;
    Descriptor desc = resultRecord.getDescriptorForType();
    FieldDescriptor fd = desc.findFieldByName("test_array");
    assertArrayEquals(new String[]{"one", "two", "three"},
        ((List<String>) resultRecord.getField(fd)).toArray()
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapToProtobuf() throws Exception {
    String json = "{ \"test_map\": {\"first\": \"one\", \"second\": \"two\"} }";

    Object result = ProtobufSchemaUtils.toObject(jsonTree(json), mapSchema);
    assertTrue(result instanceof DynamicMessage);
    DynamicMessage resultRecord = (DynamicMessage) result;
    Descriptor desc = resultRecord.getDescriptorForType();
    FieldDescriptor fd = desc.findFieldByName("test_map");
    assertEquals(2, ((List<DynamicMessage>) resultRecord.getField(fd)).size());
  }

  @Test
  public void testUnionToProtobuf() throws Exception {
    Object result = ProtobufSchemaUtils.toObject(
        jsonTree("{\"name\":\"test string\"}"),
        unionSchema
    );
    assertTrue(result instanceof DynamicMessage);
    DynamicMessage resultRecord = (DynamicMessage) result;
    Descriptor desc = resultRecord.getDescriptorForType();
    FieldDescriptor fd = desc.findFieldByName("name");
    assertEquals("test string", resultRecord.getField(fd));

    result = ProtobufSchemaUtils.toObject(jsonTree("{\"age\":12}"), unionSchema);
    assertTrue(result instanceof DynamicMessage);
    resultRecord = (DynamicMessage) result;
    desc = resultRecord.getDescriptorForType();
    fd = desc.findFieldByName("age");
    assertEquals(12, resultRecord.getField(fd));
  }

  @Test
  public void testEnumToProtobuf() throws Exception {
    Object result = ProtobufSchemaUtils.toObject(jsonTree("{\"suit\":\"SPADES\"}"), enumSchema);
    DynamicMessage resultRecord = (DynamicMessage) result;
    Descriptor desc = resultRecord.getDescriptorForType();
    FieldDescriptor fd = desc.findFieldByName("suit");
    assertEquals("SPADES", resultRecord.getField(fd).toString());
  }

  @Test
  public void testOptionEscape() throws Exception {
    String optionSchemaString = "syntax = \"proto3\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "option java_outer_classname = \"TestOptionEscape\";\n"
        + "option testBackslash = \"backslash\\\\backslash\";\n"
        + "option testDoubleQuote = \"\\\"something\\\"\";\n"
        + "option testSingleQuote = \"\\\'something\\\'\";\n"
        + "option testNewline = \"newline\\n\";\n"
        + "option testBell = \"bell\\a\";\n"
        + "option testBackspace = \"backspace\\b\";\n"
        + "option testFormFeed = \"formFeed\\f\";\n"
        + "option testCarriageReturn = \"carriageReturn\\r\";\n"
        + "option testTab = \"tab\\t\";\n"
        + "option testVerticalTab = \"verticalTab\\v\";\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "message TestOptionEscape {\n"
        + "    option (source_ref) = \"https://someUrl.com\";\n"
        + "\n"
        + "    bool test_bool = 1;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(optionSchemaString);
    String parsed = schema.canonicalString();

    assertTrue(parsed.contains("backslash\\\\backslash"));
    assertTrue(parsed.contains("\\\"something\\\""));
    assertTrue(parsed.contains("\\\'something\\\'"));
    assertTrue(parsed.contains("newline\\n"));
    assertTrue(parsed.contains("bell\\a"));
    assertTrue(parsed.contains("backspace\\b"));
    assertTrue(parsed.contains("formFeed\\f"));
    assertTrue(parsed.contains("carriageReturn\\r"));
    assertTrue(parsed.contains("tab\\t"));
    assertTrue(parsed.contains("verticalTab\\v"));
    assertTrue(parsed.contains("https://someUrl.com"));
  }

  @Test
  public void testEnumOption() {
    String optionSchemaString = "import \"google/protobuf/descriptor.proto\";\n"
        + "enum FooParameterType {\n"
        + "   NUMBER = 1;\n"
        + "   STRING = 2;\n"
        + "}\n"
        + " \n"
        + "message FooOptions {\n"
        + "  optional string name = 1;\n"
        + "  optional FooParameterType type = 2; \n"
        + "} \n"
        + "extend google.protobuf.MessageOptions {\n"
        + "  repeated FooOptions foo = 12345;\n"
        + "}\n"
        + "\n"
        + "message Message {\n"
        + "  option (foo) = {\n"
        + "    name: \"test\"\n"
        + "    type: STRING\n"
        + "  };\n"
        + "  \n"
        + "  option (foo) = {\n"
        + "    name: \"test2\"\n"
        + "    type: NUMBER\n"
        + "  };\n"
        + "  \n"
        + "  optional int32 b = 2;\n"
        + "}";

    ProtobufSchema schema = new ProtobufSchema(optionSchemaString);
    String parsed = schema.canonicalString();

    assertTrue(parsed.contains("type: STRING"));
    assertTrue(parsed.contains("type: NUMBER"));
  }

  @Test
  public void testEnumReserved() {
    String schemaString = "\nimport \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "message FooOptions {\n"
        + "  optional string name = 1;\n"
        + "  optional FooParameterType type = 2;\n"
        + "}\n"
        + "enum FooParameterType {\n"
        + "  reserved 20, 100 to max;\n"
        + "  reserved 10;\n"
        + "  reserved 1 to 3;\n"
        + "  reserved \"BAD\", \"OLD\";\n"
        + "  NUMBER = 1;\n"
        + "  STRING = 2;\n"
        + "}\n";

    String normalizedSchemaString = "\nimport \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "message FooOptions {\n"
        + "  optional string name = 1;\n"
        + "  optional .FooParameterType type = 2;\n"
        + "}\n"
        + "enum FooParameterType {\n"
        + "  reserved 1 to 3;\n"
        + "  reserved 10;\n"
        + "  reserved 20;\n"
        + "  reserved 100 to max;\n"
        + "  reserved \"BAD\";\n"
        + "  reserved \"OLD\";\n"
        + "  NUMBER = 1;\n"
        + "  STRING = 2;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(schemaString);
    String parsed = schema.canonicalString();
    assertEquals(schemaString, parsed);

    String normalized = schema.normalize().canonicalString();
    assertEquals(normalizedSchemaString, normalized);
  }

  @Test
  public void testComplexCustomOptions() {
    String schemaString = "syntax = \"proto3\";\n"
        + "\n"
        + "import \"common/extensions.proto\";\n"
        + "import \"confluent/meta.proto\";\n"
        + "\n"
        + "package com.custom.example.v0;\n"
        + "\n"
        + "option go_package = \"example/v0;example\";\n"
        + "option java_multiple_files = true;\n"
        + "option (com.custom.map).subject = 'user-value';\n"
        + "option (com.custom.map).compatibility_mode = FULL;\n"
        + "option (.confluent.file_meta).doc = \"file meta\";\n"
        + "option (confluent.file_meta).params = [\n"
        + "      {\n"
        + "        value: \"my_value\",\n"
        + "        key: \"my_key\"\n"
        + "      }\n"
        + "    ];\n"
        + "option (confluent.file_meta).tags = [\"PII\", \"PRIVATE\"];\n"
        + "message User {\n"
        + "  string first_name = 1;\n"
        + "  string last_name = 2;\n"
        + "}";
    String canonicalString = "syntax = \"proto3\";\n"
        + "package com.custom.example.v0;\n"
        + "\n"
        + "import \"common/extensions.proto\";\n"
        + "import \"confluent/meta.proto\";\n"
        + "\n"
        + "option go_package = \"example/v0;example\";\n"
        + "option java_multiple_files = true;\n"
        + "option (com.custom.map).subject = \"user-value\";\n"
        + "option (com.custom.map).compatibility_mode = FULL;\n"
        + "option (.confluent.file_meta).doc = \"file meta\";\n"
        + "option (confluent.file_meta).params = [{\n"
        + "  value: \"my_value\",\n"
        + "  key: \"my_key\"\n"
        + "}];\n"
        + "option (confluent.file_meta).tags = [\n"
        + "  \"PII\",\n"
        + "  \"PRIVATE\"\n"
        + "];\n"
        + "\n"
        + "message User {\n"
        + "  string first_name = 1;\n"
        + "  string last_name = 2;\n"
        + "}\n";
    String expectedSchemaString = "syntax = \"proto3\";\n"
        + "package com.custom.example.v0;\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "\n"
        + "option java_multiple_files = true;\n"
        + "option go_package = \"example/v0;example\";\n"
        + "option (confluent.file_meta) = {\n"
        + "  doc: \"file meta\",\n"
        + "  params: [\n"
        + "    {\n"
        + "      key: \"my_key\",\n"
        + "      value: \"my_value\"\n"
        + "    }\n"
        + "  ],\n"
        + "  tags: [\n"
        + "    \"PII\",\n"
        + "    \"PRIVATE\"\n"
        + "  ]\n"
        + "};\n"
        + "\n"
        + "message User {\n"
        + "  string first_name = 1;\n"
        + "  string last_name = 2;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(canonicalString, schema.canonicalString());
    ProtobufSchema schema2 = new ProtobufSchema(schema.toDescriptor());
    assertEquals(expectedSchemaString, schema2.canonicalString());
  }

  @Test
  public void testComplexCustomOptions2() {
    String schemaString = "syntax = \"proto3\";\n"
        + "package com.example.mynamespace;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "option java_outer_classname = \"ComplexProto\";\n"
        + "message SampleRecord {\n"
        + "  int32 my_field1 = 1 [\n"
        + "    (confluent.field_meta).tags = \"foo\"\n"
        + "  ];\n"
        + "  double my_field2 = 2 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  int32 my_field3 = 3 [\n"
        + "    (confluent.field_meta).tags = \"foo\",\n"
        + "    (confluent.field_meta).tags = \"bar\"\n"
        + "  ];\n"
        + "  double my_field4 = 4 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\",\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  string my_field5 = 5 [\n"
        + "    (confluent.field_meta) = {\n"
        + "      tags: [\"foo\"]\n"
        + "    },\n"
        + "    (confluent.field_meta).tags = \"bar\"\n"
        + "  ];\n"
        + "  string my_field6 = 6 [\n"
        + "    (confluent.field_meta).tags = \"foo\",\n"
        + "    (confluent.field_meta).tags = [\"bar\"]\n"
        + "  ];\n"
        + "}";
    String canonicalString = "syntax = \"proto3\";\n"
        + "package com.example.mynamespace;\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "option java_outer_classname = \"ComplexProto\";\n"
        + "\n"
        + "message SampleRecord {\n"
        + "  int32 my_field1 = 1 [(confluent.field_meta).tags = \"foo\"];\n"
        + "  double my_field2 = 2 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  int32 my_field3 = 3 [\n"
        + "    (confluent.field_meta).tags = \"foo\",\n"
        + "    (confluent.field_meta).tags = \"bar\"\n"
        + "  ];\n"
        + "  double my_field4 = 4 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\",\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  string my_field5 = 5 [\n"
        + "    (confluent.field_meta) = {\n"
        + "      tags: [\n"
        + "        \"foo\"\n"
        + "      ]\n"
        + "    },\n"
        + "    (confluent.field_meta).tags = \"bar\"\n"
        + "  ];\n"
        + "  string my_field6 = 6 [\n"
        + "    (confluent.field_meta).tags = \"foo\",\n"
        + "    (confluent.field_meta).tags = [\"bar\"]\n"
        + "  ];\n"
        + "}\n";
    String expectedSchemaString = "syntax = \"proto3\";\n"
        + "package com.example.mynamespace;\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "option java_outer_classname = \"ComplexProto\";\n"
        + "\n"
        + "message SampleRecord {\n"
        + "  int32 my_field1 = 1 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  double my_field2 = 2 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  int32 my_field3 = 3 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\",\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  double my_field4 = 4 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\",\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  string my_field5 = 5 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\",\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  string my_field6 = 6 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\",\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "}\n";
    String normalized = "syntax = \"proto3\";\n"
        + "package com.example.mynamespace;\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "option java_outer_classname = \"ComplexProto\";\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message SampleRecord {\n"
        + "  int32 my_field1 = 1 [(confluent.field_meta) = {\n"
        + "    tags: \"foo\"\n"
        + "  }];\n"
        + "  double my_field2 = 2 [(confluent.field_meta) = {\n"
        + "    tags: \"bar\"\n"
        + "  }];\n"
        + "  int32 my_field3 = 3 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\",\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  double my_field4 = 4 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\",\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  string my_field5 = 5 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\",\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "  string my_field6 = 6 [(confluent.field_meta) = {\n"
        + "    tags: [\n"
        + "      \"foo\",\n"
        + "      \"bar\"\n"
        + "    ]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(canonicalString, schema.canonicalString());
    ProtobufSchema schema2 = new ProtobufSchema(schema.toDescriptor());
    assertEquals(expectedSchemaString, schema2.canonicalString());
    ProtobufSchema normalizedSchema = schema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
    normalizedSchema = schema2.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
  }

  @Test
  public void testComplexCustomOptions3() {
    String schemaString = "syntax = \"proto3\";\n"
        + "package com.example.mynamespace;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "option java_outer_classname = \"ComplexProto\";\n"
        + "message SampleRecord {\n"
        + "  int32 my_field1 = 2 [(confluent.field_meta) = {\n"
        + "    value1: -2.0,\n"
        // The square wire parser does not yet handle the following cases
        /*
        + "    value2: - 2.0,\n"
        + "    value3: - "
        + "      # comment"
        + "      2.0,\n"
        */
        + "    value4: 10 value5: 20,\n"
        + "    value6: 10,value7: 20,\n"
        + "    value8: 10[com.foo.value9]: 20,\n"
        + "    value10: 10f,\n"
        + "    value11: 1.0f,\n"
        + "    value12: 1.23,\n"
        + "    value13: 1.23f,\n"
        + "    value14: \n"
        + "      \"When we got into office, the thing that surprised me most was to find \"\n"
        + "      \"that things were just as bad as we'd been saying they were.\\n\\n\"\n"
        + "      \"  -- John F. Kennedy\",\n"
        + "    value15: \"first part\" 'second part'\n"
        + "      \"third part\",\n"
        + "    value16: \"first\"\"second\"'third''fourth',\n"
        + "    value17: { foo: \"bar\" },\n"
        + "    [com.foo.value18]: 10,\n"
        + "    [com.foo.value19]: { foo: \"bar\" },\n"
        // The square wire parser does not yet handle the following cases
        /*
        + "    value20 {"
        + "      [type.googleapis.com/com.foo.any] { foo: \"bar\" }\n"
        + "    },\n"
        */
        + "    value21 { foo: \"bar\" },\n"
        + "    value22: [{ foo: \"bar\" }],\n"
        // The square wire parser does not yet handle the following cases
        /*
        + "    value23 [{ foo: \"bar\" }],\n"
        + "    value24 < foo: \"bar\" >,\n"
        */
        + "    value25: 1,\n"
        + "    value25: 2,\n"
        + "    value25: [3, 4, 5],\n"
        + "    value25: 6,\n"
        + "    value25: [7, 8, 9],\n"
        + "    value26: { key: \"entry1\", value: 1 },\n"
        + "    value26: { key: \"entry2\", value: 2 },\n"
        + "    value26: ["
        + "      { key: \"entry3\", value: 3 },\n"
        + "      { key: \"entry4\", value: 4 }\n"
        + "    ]"
        + "  }];\n"
        + "}";
    String canonicalString = "syntax = \"proto3\";\n"
        + "package com.example.mynamespace;\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "option java_outer_classname = \"ComplexProto\";\n"
        + "\n"
        + "message SampleRecord {\n"
        + "  int32 my_field1 = 2 [(confluent.field_meta) = {\n"
        + "    value1: -2.0,\n"
        + "    value4: 10,\n"
        + "    value5: 20,\n"
        + "    value6: 10,\n"
        + "    value7: 20,\n"
        + "    value8: 10,\n"
        + "    [com.foo.value9]: 20,\n"
        + "    value10: 10f,\n"
        + "    value11: 1.0f,\n"
        + "    value12: 1.23,\n"
        + "    value13: 1.23f,\n"
        + "    value14: \"When we got into office, the thing that surprised me most was to find that things were just as bad as we\\'d been saying they were.\\n\\n  -- John F. Kennedy\",\n"
        + "    value15: \"first partsecond partthird part\",\n"
        + "    value16: \"firstsecondthirdfourth\",\n"
        + "    value17: {\n"
        + "      foo: \"bar\"\n"
        + "    },\n"
        + "    [com.foo.value18]: 10,\n"
        + "    [com.foo.value19]: {\n"
        + "      foo: \"bar\"\n"
        + "    },\n"
        + "    value21: {\n"
        + "      foo: \"bar\"\n"
        + "    },\n"
        + "    value22: [\n"
        + "      {\n"
        + "        foo: \"bar\"\n"
        + "      }\n"
        + "    ],\n"
        + "    value25: [\n"
        + "      1,\n"
        + "      2,\n"
        + "      3,\n"
        + "      4,\n"
        + "      5,\n"
        + "      6,\n"
        + "      7,\n"
        + "      8,\n"
        + "      9\n"
        + "    ],\n"
        + "    value26: [\n"
        + "      {\n"
        + "        key: \"entry1\",\n"
        + "        value: 1\n"
        + "      },\n"
        + "      {\n"
        + "        key: \"entry2\",\n"
        + "        value: 2\n"
        + "      },\n"
        + "      {\n"
        + "        key: \"entry3\",\n"
        + "        value: 3\n"
        + "      },\n"
        + "      {\n"
        + "        key: \"entry4\",\n"
        + "        value: 4\n"
        + "      }\n"
        + "    ]\n"
        + "  }];\n"
        + "}\n";
    String normalized = "syntax = \"proto3\";\n"
        + "package com.example.mynamespace;\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "option java_outer_classname = \"ComplexProto\";\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message SampleRecord {\n"
        + "  int32 my_field1 = 2 [(confluent.field_meta) = {\n"
        + "    [com.foo.value18]: 10,\n"
        + "    [com.foo.value19]: {\n"
        + "      foo: \"bar\"\n"
        + "    },\n"
        + "    [com.foo.value9]: 20,\n"
        + "    value1: -2,\n"
        + "    value10: 10,\n"
        + "    value11: 1,\n"
        + "    value12: 1.23,\n"
        + "    value13: 1.23,\n"
        + "    value14: \"When we got into office, the thing that surprised me most was to find that things were just as bad as we\\'d been saying they were.\\n\\n  -- John F. Kennedy\",\n"
        + "    value15: \"first partsecond partthird part\",\n"
        + "    value16: \"firstsecondthirdfourth\",\n"
        + "    value17: {\n"
        + "      foo: \"bar\"\n"
        + "    },\n"
        + "    value21: {\n"
        + "      foo: \"bar\"\n"
        + "    },\n"
        + "    value22: {\n"
        + "      foo: \"bar\"\n"
        + "    },\n"
        + "    value25: [\n"
        + "      1,\n"
        + "      2,\n"
        + "      3,\n"
        + "      4,\n"
        + "      5,\n"
        + "      6,\n"
        + "      7,\n"
        + "      8,\n"
        + "      9\n"
        + "    ],\n"
        + "    value26: [\n"
        + "      {\n"
        + "        key: \"entry1\",\n"
        + "        value: 1\n"
        + "      },\n"
        + "      {\n"
        + "        key: \"entry2\",\n"
        + "        value: 2\n"
        + "      },\n"
        + "      {\n"
        + "        key: \"entry3\",\n"
        + "        value: 3\n"
        + "      },\n"
        + "      {\n"
        + "        key: \"entry4\",\n"
        + "        value: 4\n"
        + "      }\n"
        + "    ],\n"
        + "    value4: 10,\n"
        + "    value5: 20,\n"
        + "    value6: 10,\n"
        + "    value7: 20,\n"
        + "    value8: 10\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(canonicalString, schema.canonicalString());
    ProtobufSchema normalizedSchema = schema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
  }

  @Test
  public void testNestedCustomOptions() {
    String schemaString = "syntax = \"proto3\";\n"
        + "package acme.events;\n"
        + "import \"google/protobuf/wrappers.proto\";\n"
        + "import \"google/protobuf/timestamp.proto\";\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "message ValidationSettings {\n"
        + "  bool is_optional = 1;\n"
        + "}\n"
        + "message EventMetadata {\n"
        + "  google.protobuf.StringValue owner = 1;\n"
        + "  google.protobuf.StringValue description = 2;\n"
        + "  RetentionPolicies retention_policy = 4;\n"
        + "}\n"
        + "\n"
        + "extend google.protobuf.FieldOptions {\n"
        + "  ValidationSettings validation_settings = 10000;\n"
        + "  EventMetadata event_metadata = 10001;\n"
        + "}\n"
        + "\n"
        + "enum RetentionPolicies {\n"
        + "  DEFAULT = 0; // Default value, allows enforcement to implement default strategy\n"
        + "  SEVEN_DAYS = 1;\n"
        + "  THIRTY_DAYS = 2;\n"
        + "  NINETY_DAYS = 3;\n"
        + "  ONE_YEAR = 4;\n"
        + "  TWO_YEARS = 5;\n"
        + "  SEVEN_YEARS = 6;\n"
        + "  ALL_TIME = 7;\n"
        + "}\n"
        + "\n"
        + "message EventHeader {\n"
        + "  google.protobuf.StringValue event_source = 1;\n"
        + "  google.protobuf.StringValue event_id = 2;\n"
        + "  google.protobuf.Timestamp event_timestamp = 3;\n"
        + "  google.protobuf.StringValue event_name = 4;\n"
        + "}\n"
        + "\n"
        + "message Event {\n"
        + "  EventHeader header = 1;\n"
        + "  oneof only_one_event_limiter {\n"
        + "    SearchEvent requested_search = 2 [\n"
        + "      (event_metadata).description.value = \"Fires when a user submits a search request.\",\n"
        + "      (event_metadata).owner.value = \"DISCO\"\n"
        + "    ];\n"
        + "  }\n"
        + "}\n"
        + "message SearchEvent {\n"
        + "  google.protobuf.Int32Value user_id = 1 [(validation_settings).is_optional = true];\n"
        + "}\n";
    String canonicalString = "syntax = \"proto3\";\n"
        + "package acme.events;\n"
        + "\n"
        + "import \"google/protobuf/wrappers.proto\";\n"
        + "import \"google/protobuf/timestamp.proto\";\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "message ValidationSettings {\n"
        + "  bool is_optional = 1;\n"
        + "}\n"
        + "message EventMetadata {\n"
        + "  google.protobuf.StringValue owner = 1;\n"
        + "  google.protobuf.StringValue description = 2;\n"
        + "  RetentionPolicies retention_policy = 4;\n"
        + "}\n"
        + "message EventHeader {\n"
        + "  google.protobuf.StringValue event_source = 1;\n"
        + "  google.protobuf.StringValue event_id = 2;\n"
        + "  google.protobuf.Timestamp event_timestamp = 3;\n"
        + "  google.protobuf.StringValue event_name = 4;\n"
        + "}\n"
        + "message Event {\n"
        + "  EventHeader header = 1;\n"
        + "\n"
        + "  oneof only_one_event_limiter {\n"
        + "    SearchEvent requested_search = 2 [\n"
        + "      (event_metadata).description.value = \"Fires when a user submits a search request.\",\n"
        + "      (event_metadata).owner.value = \"DISCO\"\n"
        + "    ];\n"
        + "  }\n"
        + "}\n"
        + "message SearchEvent {\n"
        + "  google.protobuf.Int32Value user_id = 1 [(validation_settings).is_optional = true];\n"
        + "}\n"
        + "enum RetentionPolicies {\n"
        + "  DEFAULT = 0;\n"
        + "  SEVEN_DAYS = 1;\n"
        + "  THIRTY_DAYS = 2;\n"
        + "  NINETY_DAYS = 3;\n"
        + "  ONE_YEAR = 4;\n"
        + "  TWO_YEARS = 5;\n"
        + "  SEVEN_YEARS = 6;\n"
        + "  ALL_TIME = 7;\n"
        + "}\n"
        + "\n"
        + "extend google.protobuf.FieldOptions {\n"
        + "  ValidationSettings validation_settings = 10000;\n"
        + "  EventMetadata event_metadata = 10001;\n"
        + "}\n";
    String normalized = "syntax = \"proto3\";\n"
        + "package acme.events;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "import \"google/protobuf/timestamp.proto\";\n"
        + "import \"google/protobuf/wrappers.proto\";\n"
        + "\n"
        + "message ValidationSettings {\n"
        + "  bool is_optional = 1;\n"
        + "}\n"
        + "message EventMetadata {\n"
        + "  .google.protobuf.StringValue owner = 1;\n"
        + "  .google.protobuf.StringValue description = 2;\n"
        + "  .acme.events.RetentionPolicies retention_policy = 4;\n"
        + "}\n"
        + "message EventHeader {\n"
        + "  .google.protobuf.StringValue event_source = 1;\n"
        + "  .google.protobuf.StringValue event_id = 2;\n"
        + "  .google.protobuf.Timestamp event_timestamp = 3;\n"
        + "  .google.protobuf.StringValue event_name = 4;\n"
        + "}\n"
        + "message Event {\n"
        + "  .acme.events.EventHeader header = 1;\n"
        + "\n"
        + "  oneof only_one_event_limiter {\n"
        + "    .acme.events.SearchEvent requested_search = 2 [(acme.events.event_metadata) = {\n"
        + "      description: {\n"
        + "        value: \"Fires when a user submits a search request.\"\n"
        + "      },\n"
        + "      owner: {\n"
        + "        value: \"DISCO\"\n"
        + "      }\n"
        + "    }];\n"
        + "  }\n"
        + "}\n"
        + "message SearchEvent {\n"
        + "  .google.protobuf.Int32Value user_id = 1 [(acme.events.validation_settings) = {\n"
        + "    is_optional: true\n"
        + "  }];\n"
        + "}\n"
        + "enum RetentionPolicies {\n"
        + "  DEFAULT = 0;\n"
        + "  SEVEN_DAYS = 1;\n"
        + "  THIRTY_DAYS = 2;\n"
        + "  NINETY_DAYS = 3;\n"
        + "  ONE_YEAR = 4;\n"
        + "  TWO_YEARS = 5;\n"
        + "  SEVEN_YEARS = 6;\n"
        + "  ALL_TIME = 7;\n"
        + "}\n"
        + "\n"
        + "extend .google.protobuf.FieldOptions {\n"
        + "  .acme.events.ValidationSettings validation_settings = 10000;\n"
        + "  .acme.events.EventMetadata event_metadata = 10001;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(canonicalString, schema.canonicalString());
    ProtobufSchema normalizedSchema = schema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
  }


  @Test
  public void testRanges() throws Exception {
    String schemaString = "\nmessage Money {\n"
        + "  reserved 5000 to 6000;\n"
        + "  reserved 10000 to 10001;\n"
        + "  reserved 20000 to 20000;\n"
        + "\n"
        + "  optional int64 cents = 1;\n"
        + "\n"
        + "  extensions 100 to 200;\n"
        + "  extensions 1000 to 1001;\n"
        + "  extensions 2000 to 2000;\n"
        + "\n"
        + "  enum MoneyEnum {\n"
        + "    reserved 100 to 200;\n"
        + "    reserved 1000 to 1001;\n"
        + "    reserved 2000 to 2000;\n"
        + "    NONE = 0;\n"
        + "    CENTS = 1;\n"
        + "    DOLLARS = 2;\n"
        + "  }\n"
        + "}\n";
    String canonicalString = "\nmessage Money {\n"
        + "  reserved 5000 to 6000;\n"
        + "  reserved 10000 to 10001;\n"
        + "  reserved 20000 to 20000;\n"
        + "\n"
        + "  optional int64 cents = 1;\n"
        + "\n"
        + "  extensions 100 to 200;\n"
        + "  extensions 1000 to 1001;\n"
        + "  extensions 2000 to 2000;\n"
        + "\n"
        + "  enum MoneyEnum {\n"
        + "    reserved 100 to 200;\n"
        + "    reserved 1000 to 1001;\n"
        + "    reserved 2000 to 2000;\n"
        + "    NONE = 0;\n"
        + "    CENTS = 1;\n"
        + "    DOLLARS = 2;\n"
        + "  }\n"
        + "}\n";
    String normalized = "\nmessage Money {\n"
        + "  reserved 5000 to 6000;\n"
        + "  reserved 10000 to 10001;\n"
        + "  reserved 20000;\n"
        + "\n"
        + "  optional int64 cents = 1;\n"
        + "\n"
        + "  extensions 100 to 200;\n"
        + "  extensions 1000 to 1001;\n"
        + "  extensions 2000;\n"
        + "\n"
        + "  enum MoneyEnum {\n"
        + "    reserved 100 to 200;\n"
        + "    reserved 1000 to 1001;\n"
        + "    reserved 2000;\n"
        + "    NONE = 0;\n"
        + "    CENTS = 1;\n"
        + "    DOLLARS = 2;\n"
        + "  }\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(canonicalString, schema.canonicalString());
    ProtobufSchema normalizedSchema = schema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
  }

  @Test
  public void testComplexEnum() throws Exception {
    String schemaString = "syntax = \"proto3\";\n"
        + "package acme.api.items;\n"
        + "\n"
        + "option java_package = \"com.acme.api.items\";\n"
        + "\n"
        + "extend google.protobuf.EnumValueOptions {\n"
        + "  optional VisibleTypeEnumValueOption visible_types = 24000;\n"
        + "}\n"
        + "\n"
        + "message VisibleTypeEnumValueOption {\n"
        + "  repeated ObjectType type = 1;\n"
        + "}\n"
        + "\n"
        + "message ObjectType {\n"
        + "}\n"
        + "\n"
        + "message VisibleTypeSet {\n"
        + "}\n"
        + "\n"
        + "enum Type {\n"
        + "  reserved 13,15;\n"
        + "  reserved \"MARKET_ITEM_SETTINGS\",\"PROMO\";\n"
        + "  ITEM = 0;\n"
        + "  PAGE = 1;\n"
        + "  ITEM_IMAGE = 2;\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "enum ItemsVisibleTypeSet {\n"
        + "  // This is for all clients before register 4.1.2.  The items service will set this to clients where it is not set already\n"
        + "  // When native clients need to support more objects, create a new enum value\n"
        + "  LEGACY = 1 [\n"
        + "    (acme.api.items.visible_types) = {\n"
        + "      type: {[type]: ITEM},\n"
        + "      type: {[type]: PAGE},\n"
        + "      type: {[type]: ITEM_IMAGE}\n"
        + "    }\n"
        + "  ];\n"
        + "}\n"
        + "\n"
        + "extend VisibleTypeSet {\n"
        + "  optional ItemsVisibleTypeSet items_visible_type_set = 10;\n"
        + "}\n"
        + "\n"
        + "extend ObjectType {\n"
        + "  optional Type type = 101;\n"
        + "}\n"
        + "\n"
        + "\n";
    String canonicalString = "syntax = \"proto3\";\n"
        + "package acme.api.items;\n"
        + "\n"
        + "option java_package = \"com.acme.api.items\";\n"
        + "\n"
        + "message VisibleTypeEnumValueOption {\n"
        + "  repeated ObjectType type = 1;\n"
        + "}\n"
        + "message ObjectType {}\n"
        + "message VisibleTypeSet {}\n"
        + "enum Type {\n"
        + "  reserved 13, 15;\n"
        + "  reserved \"MARKET_ITEM_SETTINGS\", \"PROMO\";\n"
        + "  ITEM = 0;\n"
        + "  PAGE = 1;\n"
        + "  ITEM_IMAGE = 2;\n"
        + "}\n"
        + "enum ItemsVisibleTypeSet {\n"
        + "  LEGACY = 1 [(acme.api.items.visible_types) = {\n"
        + "    type: [\n"
        + "      {\n"
        + "        [type]: ITEM\n"
        + "      },\n"
        + "      {\n"
        + "        [type]: PAGE\n"
        + "      },\n"
        + "      {\n"
        + "        [type]: ITEM_IMAGE\n"
        + "      }\n"
        + "    ]\n"
        + "  }];\n"
        + "}\n"
        + "\n"
        + "extend google.protobuf.EnumValueOptions {\n"
        + "  optional VisibleTypeEnumValueOption visible_types = 24000;\n"
        + "}\n"
        + "extend VisibleTypeSet {\n"
        + "  optional ItemsVisibleTypeSet items_visible_type_set = 10;\n"
        + "}\n"
        + "extend ObjectType {\n"
        + "  optional Type type = 101;\n"
        + "}\n";
    String normalized = "syntax = \"proto3\";\n"
        + "package acme.api.items;\n"
        + "\n"
        + "option java_package = \"com.acme.api.items\";\n"
        + "\n"
        + "message VisibleTypeEnumValueOption {\n"
        + "  repeated .acme.api.items.ObjectType type = 1;\n"
        + "}\n"
        + "message ObjectType {}\n"
        + "message VisibleTypeSet {}\n"
        + "enum Type {\n"
        + "  reserved 13;\n"
        + "  reserved 15;\n"
        + "  reserved \"MARKET_ITEM_SETTINGS\";\n"
        + "  reserved \"PROMO\";\n"
        + "  ITEM = 0;\n"
        + "  PAGE = 1;\n"
        + "  ITEM_IMAGE = 2;\n"
        + "}\n"
        + "enum ItemsVisibleTypeSet {\n"
        + "  LEGACY = 1 [(acme.api.items.visible_types) = {\n"
        + "    type: [\n"
        + "      {\n"
        + "        [acme.api.items.type]: ITEM\n"
        + "      },\n"
        + "      {\n"
        + "        [acme.api.items.type]: PAGE\n"
        + "      },\n"
        + "      {\n"
        + "        [acme.api.items.type]: ITEM_IMAGE\n"
        + "      }\n"
        + "    ]\n"
        + "  }];\n"
        + "}\n"
        + "\n"
        + "extend .google.protobuf.EnumValueOptions {\n"
        + "  optional .acme.api.items.VisibleTypeEnumValueOption visible_types = 24000;\n"
        + "}\n"
        + "extend .acme.api.items.VisibleTypeSet {\n"
        + "  optional .acme.api.items.ItemsVisibleTypeSet items_visible_type_set = 10;\n"
        + "}\n"
        + "extend .acme.api.items.ObjectType {\n"
        + "  optional .acme.api.items.Type type = 101;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(canonicalString, schema.canonicalString());
    ProtobufSchema normalizedSchema = schema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
    normalizedSchema = normalizedSchema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
  }

  @Test
  public void testRecordToJson() throws Exception {
    DynamicMessage.Builder builder = recordSchema.newMessageBuilder();
    Descriptor desc = builder.getDescriptorForType();
    FieldDescriptor fd = desc.findFieldByName("test_string");
    builder.setField(fd, "string");
    fd = desc.findFieldByName("test_bool");
    builder.setField(fd, true);
    fd = desc.findFieldByName("test_bytes");
    builder.setField(fd, ByteString.copyFromUtf8("hello"));
    fd = desc.findFieldByName("test_double");
    builder.setField(fd, 800.25);
    fd = desc.findFieldByName("test_float");
    builder.setField(fd, 23.4f);
    fd = desc.findFieldByName("test_fixed32");
    builder.setField(fd, 32);
    fd = desc.findFieldByName("test_fixed64");
    builder.setField(fd, 64L);
    fd = desc.findFieldByName("test_int32");
    builder.setField(fd, 32);
    fd = desc.findFieldByName("test_int64");
    builder.setField(fd, 64L);
    fd = desc.findFieldByName("test_sfixed32");
    builder.setField(fd, 32);
    fd = desc.findFieldByName("test_sfixed64");
    builder.setField(fd, 64L);
    fd = desc.findFieldByName("test_sint32");
    builder.setField(fd, 32);
    fd = desc.findFieldByName("test_sint64");
    builder.setField(fd, 64L);
    fd = desc.findFieldByName("test_uint32");
    builder.setField(fd, 32);
    fd = desc.findFieldByName("test_uint64");
    builder.setField(fd, 64L);
    DynamicMessage message = builder.build();

    JsonNode result = objectMapper.readTree(ProtobufSchemaUtils.toJson(message));
    assertTrue(result.isObject());
    assertTrue(result.get("test_str").isTextual());
    assertEquals("string", result.get("test_str").textValue());
    assertTrue(result.get("testBool").isBoolean());
    assertEquals(true, result.get("testBool").booleanValue());
    assertTrue(result.get("testBytes").isTextual());
    assertEquals("aGVsbG8=", result.get("testBytes").textValue());
    assertTrue(result.get("testDouble").isDouble());
    assertEquals(800.25, result.get("testDouble").doubleValue(), 0.01);
    assertTrue(result.get("testFloat").isDouble());
    assertEquals(23.4f, result.get("testFloat").doubleValue(), 0.1);
    assertTrue(result.get("testFixed32").isInt());
    assertEquals(32, result.get("testFixed32").intValue());
    assertTrue(result.get("testFixed64").isTextual());
    assertEquals("64", result.get("testFixed64").textValue());
    assertTrue(result.get("testInt32").isInt());
    assertEquals(32, result.get("testInt32").intValue());
    assertTrue(result.get("testInt64").isTextual());
    assertEquals("64", result.get("testInt64").textValue());
    assertTrue(result.get("testSfixed32").isInt());
    assertEquals(32, result.get("testSfixed32").intValue());
    assertTrue(result.get("testSfixed64").isTextual());
    assertEquals("64", result.get("testSfixed64").textValue());
    assertTrue(result.get("testSint32").isInt());
    assertEquals(32, result.get("testSint32").intValue());
    assertTrue(result.get("testSint64").isTextual());
    assertEquals("64", result.get("testSint64").textValue());
    assertTrue(result.get("testUint32").isInt());
    assertEquals(32, result.get("testUint32").intValue());
    assertTrue(result.get("testUint64").isTextual());
    assertEquals("64", result.get("testUint64").textValue());
  }

  @Test
  public void testArrayToJson() throws Exception {
    DynamicMessage.Builder builder = arraySchema.newMessageBuilder();
    Descriptor desc = builder.getDescriptorForType();
    FieldDescriptor fd = desc.findFieldByName("test_array");
    builder.setField(fd, Arrays.asList("one", "two", "three"));
    DynamicMessage message = builder.build();
    JsonNode result = objectMapper.readTree(ProtobufSchemaUtils.toJson(message));

    JsonNode fieldNode = result.get("testArray");
    assertTrue(fieldNode.isArray());
    assertEquals(3, fieldNode.size());
    assertEquals(JsonNodeFactory.instance.textNode("one"), fieldNode.get(0));
    assertEquals(JsonNodeFactory.instance.textNode("two"), fieldNode.get(1));
    assertEquals(JsonNodeFactory.instance.textNode("three"), fieldNode.get(2));
  }

  @Test
  public void testMapToJson() throws Exception {
    DynamicMessage.Builder mapBuilder = mapSchema.newMessageBuilder("TestMap.TestMapEntry");
    Descriptor mapDesc = mapBuilder.getDescriptorForType();
    FieldDescriptor keyField = mapDesc.findFieldByName("key");
    mapBuilder.setField(keyField, "first");
    FieldDescriptor valueField = mapDesc.findFieldByName("value");
    mapBuilder.setField(valueField, "one");
    DynamicMessage mapEntry = mapBuilder.build();

    DynamicMessage.Builder mapBuilder2 = mapSchema.newMessageBuilder("TestMap.TestMapEntry");
    Descriptor mapDesc2 = mapBuilder2.getDescriptorForType();
    FieldDescriptor keyField2 = mapDesc2.findFieldByName("key");
    mapBuilder2.setField(keyField2, "second");
    FieldDescriptor valueField2 = mapDesc2.findFieldByName("value");
    mapBuilder2.setField(valueField2, "two");
    DynamicMessage mapEntry2 = mapBuilder2.build();

    DynamicMessage.Builder builder = mapSchema.newMessageBuilder();
    Descriptor desc = builder.getDescriptorForType();
    FieldDescriptor fd = desc.findFieldByName("test_map");
    builder.setField(fd, Arrays.asList(mapEntry, mapEntry2));
    DynamicMessage message = builder.build();
    JsonNode result = objectMapper.readTree(ProtobufSchemaUtils.toJson(message));

    JsonNode fieldNode = result.get("testMap");
    assertEquals(2, fieldNode.size());
    assertNotNull(fieldNode.get("first"));
    assertEquals("one", fieldNode.get("first").asText());
    assertNotNull(fieldNode.get("second"));
    assertEquals("two", fieldNode.get("second").asText());
  }

  @Test
  public void testMeta() throws Exception {
    ResourceLoader resourceLoader = new ResourceLoader("/");

    String metaName = "confluent/meta.proto";
    ProtoFileElement meta = resourceLoader.readObj(metaName);
    SchemaReference metaRef = new SchemaReference(metaName, metaName, 1);

    String decimalName = "confluent/type/decimal.proto";
    ProtoFileElement decimal = resourceLoader.readObj(decimalName);
    SchemaReference decimalRef = new SchemaReference(decimalName, decimalName, 1);

    String descriptorName = "google/protobuf/descriptor.proto";
    ProtoFileElement descriptor = resourceLoader.readObj(descriptorName);
    SchemaReference descriptorRef = new SchemaReference(descriptorName, descriptorName, 1);

    ProtoFileElement original = resourceLoader.readObj(
        "io/confluent/kafka/schemaregistry/protobuf/diff/DecimalValue2.proto");
    List<SchemaReference> refs = new ArrayList<>();
    refs.add(metaRef);
    refs.add(decimalRef);
    refs.add(descriptorRef);
    Map<String, ProtoFileElement> deps = new HashMap<>();
    deps.put(metaName, meta);
    deps.put(decimalName, decimal);
    deps.put(descriptorName, descriptor);
    ProtobufSchema schema = new ProtobufSchema(original, refs, deps);
    Descriptor desc = schema.toDescriptor();
    ProtobufSchema schema2 = new ProtobufSchema(desc);

    assertTrue(schema.isCompatible(
        CompatibilityLevel.BACKWARD,
        Collections.singletonList(new SimpleParsedSchemaHolder(schema2))).isEmpty());
    assertEquals(schema.canonicalString(), schema2.canonicalString());
  }

  @Test
  public void testNativeDependencies() throws Exception {
    String schemaString = "syntax = \"proto3\";\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "message DecimalValue {\n"
        + "  confluent.type.Decimal value = 1 [(confluent.field_meta) = { params: [\n"
        + "    { value: \"8\", key: \"precision\" },\n"
        + "    { value: \"3\", key: \"scale\" }\n"
        + "  ]}];\n"
        + "}";

    ProtobufSchema schema = new ProtobufSchema(schemaString);
    Descriptor desc = schema.toDescriptor();
    ProtobufSchema schema2 = new ProtobufSchema(desc);

    assertTrue(schema.isCompatible(
            CompatibilityLevel.BACKWARD,
        Collections.singletonList(new SimpleParsedSchemaHolder(schema2))).isEmpty());
    assertEquals(schema.canonicalString(), schema2.canonicalString());
  }

  @Test
  public void testFileDescriptorProto() throws Exception {
    ResourceLoader resourceLoader = new ResourceLoader(
        "/io/confluent/kafka/schemaregistry/protobuf/diff/");

    ProtoFileElement original = resourceLoader.readObj("TestProto.proto");
    ProtobufSchema schema = new ProtobufSchema(original.toSchema());
    String fileProto = schema.formattedString(Format.SERIALIZED.symbol());
    ProtobufSchema schema2 = new ProtobufSchema(fileProto);
    assertTrue(schema.isCompatible(
        CompatibilityLevel.BACKWARD,
        Collections.singletonList(new SimpleParsedSchemaHolder(schema2))).isEmpty());
    fileProto = schema2.formattedString(Format.SERIALIZED.symbol());
    ProtobufSchema schema3 = new ProtobufSchema(fileProto);
    assertTrue(schema2.isCompatible(
        CompatibilityLevel.BACKWARD,
        Collections.singletonList(new SimpleParsedSchemaHolder(schema3))).isEmpty());
    assertEquals(schema2, schema3);

    original = resourceLoader.readObj("NestedTestProto.proto");
    schema = new ProtobufSchema(original.toSchema());
    fileProto = schema.formattedString(Format.SERIALIZED.symbol());
    schema2 = new ProtobufSchema(fileProto);
    assertTrue(schema.isCompatible(
        CompatibilityLevel.BACKWARD,
        Collections.singletonList(new SimpleParsedSchemaHolder(schema2))).isEmpty());
    fileProto = schema2.formattedString(Format.SERIALIZED.symbol());
    schema3 = new ProtobufSchema(fileProto);
    assertTrue(schema2.isCompatible(
        CompatibilityLevel.BACKWARD,
        Collections.singletonList(new SimpleParsedSchemaHolder(schema3))).isEmpty());
    assertEquals(schema2, schema3);
  }

  @Test
  public void testDefaultOmittedInProto3String() throws Exception {
    MessageDefinition.Builder message = MessageDefinition.newBuilder("msg1");
    message.addField(null, "string", "field1", 1, "defaultVal", null);
    DynamicSchema.Builder schema = DynamicSchema.newBuilder();
    schema.setSyntax(PROTO3);
    schema.addMessageDefinition(message.build());
    ProtobufSchema protobufSchema =
        new ProtobufSchema(schema.build().getMessageDescriptor("msg1"));
    assertEquals("syntax = \"proto3\";\n"
        + "\n"
        + "message msg1 {\n"
        + "  string field1 = 1;\n"
        + "}\n", protobufSchema.toString());
  }

  @Test
  public void testRoundTrip() throws Exception {
    ProtobufSchema schema = new ProtobufSchema(readFile("NestedNoMapTestProto.proto"),
        Collections.emptyList(), Collections.emptyMap(), null, null);
    String canonical = schema.canonicalString();
    assertEquals(canonical, new ProtobufSchema(schema.toDescriptor()).canonicalString());
  }

  @Test
  public void testSameMessageName() throws Exception {
    List<SchemaReference> refs = new ArrayList<>();
    refs.add(new SchemaReference("TestProto.proto", "test1", 1));
    refs.add(new SchemaReference("TestProto2.proto", "test1", 1));
    Map<String, String> resolved = new HashMap<>();
    resolved.put("TestProto.proto", readFile("TestProto.proto"));
    resolved.put("TestProto2.proto", readFile("TestProto2.proto"));
    ProtobufSchema schema = new ProtobufSchema(
        readFile("SameMessageName.proto"), refs, resolved, null, null);

    // DG-951 Ensure we can get the message in the schema,
    // even though it has the same unqualified name as messages
    // in one or more referenced schemas
    assertNotNull(schema.toDescriptor(
        schema.toMessageName(new MessageIndexes(Collections.singletonList(0)))));
  }

  @Test
  public void testNativeTypeImports() throws Exception {
    String schemaString = "syntax = \"proto3\";\n"
        + "\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "import \"google/type/calendar_period.proto\";\n"
        + "import \"google/type/color.proto\";\n"
        + "import \"google/type/date.proto\";\n"
        + "import \"google/type/datetime.proto\";\n"
        + "import \"google/type/dayofweek.proto\";\n"
        + "import \"google/type/decimal.proto\";\n"
        + "import \"google/type/expr.proto\";\n"
        + "import \"google/type/fraction.proto\";\n"
        + "import \"google/type/interval.proto\";\n"
        + "import \"google/type/latlng.proto\";\n"
        + "import \"google/type/money.proto\";\n"
        + "import \"google/type/month.proto\";\n"
        + "import \"google/type/phone_number.proto\";\n"
        + "import \"google/type/postal_address.proto\";\n"
        + "import \"google/type/quaternion.proto\";\n"
        + "import \"google/type/timeofday.proto\";\n"
        + "import \"google/protobuf/any.proto\";\n"
        + "import \"google/protobuf/api.proto\";\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "import \"google/protobuf/duration.proto\";\n"
        + "import \"google/protobuf/empty.proto\";\n"
        + "import \"google/protobuf/field_mask.proto\";\n"
        + "import \"google/protobuf/source_context.proto\";\n"
        + "import \"google/protobuf/struct.proto\";\n"
        + "import \"google/protobuf/timestamp.proto\";\n"
        + "import \"google/protobuf/type.proto\";\n"
        + "import \"google/protobuf/wrappers.proto\";\n"
        + "\n"
        + "message TestNativeTypes {\n"
        + "\n"
        + "  confluent.type.Decimal test_cflt_decimal = 1 [(confluent.field_meta) = {\n"
        + "    doc: \"test decimal\",\n"
        + "    params: [\n"
        + "      {\n"
        + "        value: \"8\",\n"
        + "        key: \"precision\"\n"
        + "      },\n"
        + "      {\n"
        + "        value: \"3\",\n"
        + "        key: \"scale\"\n"
        + "      }\n"
        + "    ]\n"
        + "  }];\n"
        + "  google.type.CalendarPeriod test_calendar_period = 2;\n"
        + "  google.type.Color test_color = 3;\n"
        + "  google.type.Date test_date = 4;\n"
        + "  google.type.DateTime test_datetime = 5;\n"
        + "  google.type.DayOfWeek test_dayofweek = 6;\n"
        + "  google.type.Decimal test_decimal = 7;\n"
        + "  google.type.Expr test_expr = 8;\n"
        + "  google.type.Fraction test_fraction = 9;\n"
        + "  google.type.Interval test_interval = 10;\n"
        + "  google.type.LatLng test_latlng = 11;\n"
        + "  google.type.Money test_money = 12;\n"
        + "  google.type.Month test_month = 13;\n"
        + "  google.type.PhoneNumber test_phone_number = 14;\n"
        + "  google.type.PostalAddress test_postal_address = 15;\n"
        + "  google.type.Quaternion test_quaternion = 16;\n"
        + "  google.type.TimeOfDay test_timeofday = 17;\n"
        + "  google.protobuf.Any test_any = 18;\n"
        + "  google.protobuf.Api test_api = 19;\n"
        + "  google.protobuf.DescriptorProto test_descriptor = 20;\n"
        + "  google.protobuf.Duration test_duration = 21;\n"
        + "  google.protobuf.Empty test_empty = 22;\n"
        + "  google.protobuf.FieldMask test_field_mask = 23;\n"
        + "  google.protobuf.SourceContext test_source_context = 24;\n"
        + "  google.protobuf.Struct test_struct = 25;\n"
        + "  google.protobuf.Timestamp test_timestamp = 26;\n"
        + "  google.protobuf.Type test_type = 27;\n"
        + "  google.protobuf.StringValue test_wrapper = 28;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertNotNull(schema.toDescriptor());
  }

  @Test
  public void testParams() {
    String schemaString = "syntax = \"proto3\";\n"
        + "package com.acme;\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "import \"google/protobuf/timestamp.proto\";\n"
        + "message MyMessage {\n"
        + " .google.protobuf.Timestamp MIT_DATE = 1;\n"
        + " int64 MIT_LOC_ID = 2;\n"
        + " int64 MIT_MI_ID = 3;\n"
        + " int64 MIT_R_ID = 4;\n"
        + " int32 MIT_COUNT = 5;\n"
        + " .confluent.type.Decimal MIT_PRICE = 6 [(confluent.field_meta) = {\n"
        + "  params: {\n"
        + "   key: \"scale\",\n"
        + "   value: \"2\"\n"
        + "  }\n"
        + " }];\n"
        + " int64 MIT_PC_ID = 7;\n"
        + " .google.protobuf.Timestamp MIT_CREATE_DATE = 8;\n"
        + " int64 MIT_CREATE_BY_USER_ID = 9;\n"
        + " .google.protobuf.Timestamp MIT_MODIFY_DATE = 10;\n"
        + " int64 MIT_MODIFY_BY_USER_ID = 11;\n"
        + " int64 MIT_CONCEPT_ID = 12;\n"
        + " .confluent.type.Decimal MIT_ACTUAL_PRICE = 13 [(confluent.field_meta) = {\n"
        + "  params: {\n"
        + "   key: \"scale\",\n"
        + "   value: \"2\"\n"
        + "  }\n"
        + " }];\n"
        + "}";
    ProtobufSchema schema = new ProtobufSchema(schemaString);
    // Ensure we can process params when creating a dynamic schema
    assertNotNull(schema.toDynamicSchema());
  }


  @Test
  public void testNormalization() {
    String schemaString = "syntax = \"proto3\";\n"
        + "package my.package;\n"
        + "\n"
        + "import \"google/protobuf/timestamp.proto\";\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "option java_package = \"my.package\";\n"
        + "option java_outer_classname = \"Outer\";\n"
        + "option java_multiple_files = true;\n"
        + "option java_string_check_utf8 = true;\n"
        + "option optimize_for = CODE_SIZE;\n"
        + "option go_package = \"go.package\";\n"
        + "option cc_generic_services = false;\n"
        + "option java_generic_services = true;\n"
        + "option py_generic_services = false;\n"
        + "option php_generic_services = true;\n"
        + "option cc_enable_arenas = true;\n"
        + "option objc_class_prefix = \"objc\";\n"
        + "option csharp_namespace = \"csharp\";\n"
        + "option swift_prefix = \"swift\";\n"
        + "option php_class_prefix = \"php\";\n"
        + "option php_namespace = \"php_ns\";\n"
        + "option php_metadata_namespace = \"php_md_ns\";\n"
        + "option ruby_package = \"ruby\";\n"
        + "\n"
        + "message Nested {\n"
        + "  confluent.type.Decimal test_cflt_decimal = 1 [(confluent.field_meta) = {\n"
        + "    params: [\n"
        + "      {\n"
        + "        value: \"8\",\n"
        + "        key: \"precision\"\n"
        + "      },\n"
        + "      {\n"
        + "        value: \"3\",\n"
        + "        key: \"scale\"\n"
        + "      }\n"
        + "    ],\n"
        + "    doc: \"test decimal\"\n"
        + "  }];\n"
        + "  google.protobuf.Timestamp test_timestamp = 2;\n"
        + "  map<string, InnerMessage> test_map = 3;\n"
        + "\n"
        + "  message InnerMessage {\n"
        + "    reserved \"foo\", \"bar\";\n"
        + "    reserved 14, 15, 9 to 11;\n"
        + "    reserved 1, 20, 29 to 31;\n"
        + "  \n"
        + "    option no_standard_descriptor_accessor = true;\n"
        + "    option deprecated = true;\n"
        + "  \n"
        + "    InnerMessage.DeepMessage deep2 = 4;\n"
        + "    Nested.InnerMessage.DeepMessage deep3 = 5;\n"
        + "    my.package.Nested.InnerMessage.DeepMessage deep4 = 6;\n"
        + "    .my.package.Nested.InnerMessage.DeepMessage deep5 = 7;\n"
        + "    string id = 1 [json_name = \"id\"];\n"
        + "    repeated int32 ids = 2 [packed = true];\n"
        + "    DeepMessage deep1 = 3;\n"
        + "  \n"
        + "    oneof some_val {\n"
        + "      string one_id = 12;\n"
        + "      int32 other_id = 11;\n"
        + "    }\n"
        + "    oneof some_val2 {\n"
        + "      string one_id2 = 13;\n"
        + "      int32 other_id2 = 10 [\n"
        + "        deprecated = true,\n"
        + "        ctype = CORD,\n"
        + "        jstype = JS_STRING,\n"
        + "        json_name = \"otherId2\"\n"
        + "      ];\n"
        + "    }\n"
        + "  \n"
        + "    message DeepMessage {\n"
        + "      string id = 1;\n"
        + "    }\n"
        + "    enum InnerEnum {\n"
        + "      reserved 100 to 110;\n"
        + "      option allow_alias = true;\n"
        + "      TWO = 2;\n"
        + "      ONE = 1;\n"
        + "      ZERO = 0 [deprecated = true];\n"
        + "      ALSO_ZERO = 0;\n"
        + "    }\n"
        + "  }\n"
        + "}\n"
        + "enum Status {\n"
        + "  INACTIVE = 1;\n"
        + "  ACTIVE = 0;\n"
        + "}\n"
        + "\n"
        + "service MyService {\n"
        + "  option deprecated = true;\n"
        + "\n"
        + "  rpc MyMethod2 (stream Nested) returns (Nested.InnerMessage);\n"
        + "  rpc MyMethod1 (Nested.InnerMessage) returns (stream Nested) {\n"
        + "    option idempotency_level = NO_SIDE_EFFECTS;\n"
        + "    option deprecated = true;\n"
        + "  };\n"
        + "}\n";

    String normalized = "syntax = \"proto3\";\n"
        + "package my.package;\n"
        + "\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "import \"google/protobuf/timestamp.proto\";\n"
        + "\n"
        + "option cc_enable_arenas = true;\n"
        + "option cc_generic_services = false;\n"
        + "option csharp_namespace = \"csharp\";\n"
        + "option go_package = \"go.package\";\n"
        + "option java_generic_services = true;\n"
        + "option java_multiple_files = true;\n"
        + "option java_outer_classname = \"Outer\";\n"
        + "option java_package = \"my.package\";\n"
        + "option java_string_check_utf8 = true;\n"
        + "option objc_class_prefix = \"objc\";\n"
        + "option optimize_for = CODE_SIZE;\n"
        + "option php_class_prefix = \"php\";\n"
        + "option php_generic_services = true;\n"
        + "option php_metadata_namespace = \"php_md_ns\";\n"
        + "option php_namespace = \"php_ns\";\n"
        + "option py_generic_services = false;\n"
        + "option ruby_package = \"ruby\";\n"
        + "option swift_prefix = \"swift\";\n"
        + "\n"
        + "message Nested {\n"
        + "  .confluent.type.Decimal test_cflt_decimal = 1 [(confluent.field_meta) = {\n"
        + "    doc: \"test decimal\",\n"
        + "    params: [\n"
        + "      {\n"
        + "        key: \"precision\",\n"
        + "        value: \"8\"\n"
        + "      },\n"
        + "      {\n"
        + "        key: \"scale\",\n"
        + "        value: \"3\"\n"
        + "      }\n"
        + "    ]\n"
        + "  }];\n"
        + "  .google.protobuf.Timestamp test_timestamp = 2;\n"
        + "  map<string, .my.package.Nested.InnerMessage> test_map = 3;\n"
        + "\n"
        + "  message InnerMessage {\n"
        + "    reserved 1;\n"
        + "    reserved 9 to 11;\n"
        + "    reserved 14;\n"
        + "    reserved 15;\n"
        + "    reserved 20;\n"
        + "    reserved 29 to 31;\n"
        + "    reserved \"bar\";\n"
        + "    reserved \"foo\";\n"
        + "  \n"
        + "    option deprecated = true;\n"
        + "    option no_standard_descriptor_accessor = true;\n"
        + "  \n"
        + "    string id = 1 [json_name = \"id\"];\n"
        + "    repeated int32 ids = 2 [packed = true];\n"
        + "    .my.package.Nested.InnerMessage.DeepMessage deep1 = 3;\n"
        + "    .my.package.Nested.InnerMessage.DeepMessage deep2 = 4;\n"
        + "    .my.package.Nested.InnerMessage.DeepMessage deep3 = 5;\n"
        + "    .my.package.Nested.InnerMessage.DeepMessage deep4 = 6;\n"
        + "    .my.package.Nested.InnerMessage.DeepMessage deep5 = 7;\n"
        + "  \n"
        + "    oneof some_val2 {\n"
        + "      int32 other_id2 = 10 [\n"
        + "        ctype = CORD,\n"
        + "        deprecated = true,\n"
        + "        json_name = \"otherId2\",\n"
        + "        jstype = JS_STRING\n"
        + "      ];\n"
        + "      string one_id2 = 13;\n"
        + "    }\n"
        + "    oneof some_val {\n"
        + "      int32 other_id = 11;\n"
        + "      string one_id = 12;\n"
        + "    }\n"
        + "  \n"
        + "    message DeepMessage {\n"
        + "      string id = 1;\n"
        + "    }\n"
        + "    enum InnerEnum {\n"
        + "      reserved 100 to 110;\n"
        + "      option allow_alias = true;\n"
        + "      ALSO_ZERO = 0;\n"
        + "      ZERO = 0 [deprecated = true];\n"
        + "      ONE = 1;\n"
        + "      TWO = 2;\n"
        + "    }\n"
        + "  }\n"
        + "}\n"
        + "enum Status {\n"
        + "  ACTIVE = 0;\n"
        + "  INACTIVE = 1;\n"
        + "}\n"
        + "\n"
        + "service MyService {\n"
        + "  option deprecated = true;\n"
        + "\n"
        + "  rpc MyMethod2 (stream .my.package.Nested) returns (.my.package.Nested.InnerMessage);\n"
        + "  rpc MyMethod1 (.my.package.Nested.InnerMessage) returns (stream .my.package.Nested) {\n"
        + "    option deprecated = true;\n"
        + "    option idempotency_level = NO_SIDE_EFFECTS;\n"
        + "  };\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(schemaString, schema.canonicalString());
    Descriptor descriptor = schema.toDescriptor();
    assertNotNull(descriptor);
    ProtobufSchema normalizedSchema = schema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
    descriptor = normalizedSchema.toDescriptor();
    assertNotNull(descriptor);
    normalizedSchema = new ProtobufSchema(descriptor).normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
  }

  @Test
  public void testNormalizationWithExtensions() {
    String schemaString = "package my.package;\n"
        + "\n"
        + "message ExtendedDummy {\n"
        + "    // just to namespace the extensions\n"
        + "    extend Extended {\n"
        + "        optional string extension_normal2 = 101;\n"
        + "        optional string extension_normal = 100;\n"
        + "    }\n"
        + "\n"
        + "    message Nested {\n"
        + "        // but these are to test insane but legit definitions\n"
        + "        extend package.Extended {\n"
        + "            optional string extension_identifier2 = 103;\n"
        + "            optional string extension_identifier = 102;\n"
        + "        }\n"
        + "        message Oh {\n"
        + "            extend my.package.Extended {\n"
        + "                optional string extension_vault_replace2 = 105;\n"
        + "                optional string extension_vault_replace = 104;\n"
        + "            }\n"
        + "            message My {\n"
        + "                extend .my.package.Extended {\n"
        + "                    optional string extension_fidelius_token2 = 107;\n"
        + "                    optional string extension_fidelius_token = 106;\n"
        + "                }\n"
        + "                message God {\n"
        + "                    extend Extended.NestedExtended {\n"
        + "                        optional string extension_nested_entity2 = 109;\n"
        + "                        optional string extension_nested_entity = 108;\n"
        + "                    }\n"
        + "                    extend my.package.Extended.NestedExtended {\n"
        + "                        optional string extension_nested_again2 = 111;\n"
        + "                        optional string extension_nested_again = 110;\n"
        + "                    }\n"
        + "                }\n"
        + "            }\n"
        + "        }\n"
        + "    }\n"
        + "}\n"
        + "\n"
        + "message Extended {\n"
        + "    optional string vaulted_data = 1;\n"
        + "    optional string field_normal = 2;\n"
        + "    extensions 100 to 199;\n"
        + "\n"
        + "    message NestedExtended {\n"
        + "        optional string vaulted_data = 1;\n"
        + "        optional string field_normal = 2;\n"
        + "        optional string field_vault_replace = 3;\n"
        + "        extensions 100 to 199;\n"
        + "        extend NestedExtended {\n"
        + "            optional string extension_nested_trouble2 = 113;\n"
        + "            optional string extension_nested_trouble = 112;\n"
        + "        }\n"
        + "    }\n"
        + "}\n";

    String canonical = "package my.package;\n"
        + "\n"
        + "message ExtendedDummy {\n"
        + "  extend Extended {\n"
        + "    optional string extension_normal2 = 101;\n"
        + "    optional string extension_normal = 100;\n"
        + "  }\n"
        + "\n"
        + "  message Nested {\n"
        + "    extend package.Extended {\n"
        + "      optional string extension_identifier2 = 103;\n"
        + "      optional string extension_identifier = 102;\n"
        + "    }\n"
        + "  \n"
        + "    message Oh {\n"
        + "      extend my.package.Extended {\n"
        + "        optional string extension_vault_replace2 = 105;\n"
        + "        optional string extension_vault_replace = 104;\n"
        + "      }\n"
        + "    \n"
        + "      message My {\n"
        + "        extend .my.package.Extended {\n"
        + "          optional string extension_fidelius_token2 = 107;\n"
        + "          optional string extension_fidelius_token = 106;\n"
        + "        }\n"
        + "      \n"
        + "        message God {\n"
        + "          extend Extended.NestedExtended {\n"
        + "            optional string extension_nested_entity2 = 109;\n"
        + "            optional string extension_nested_entity = 108;\n"
        + "          }\n"
        + "          extend my.package.Extended.NestedExtended {\n"
        + "            optional string extension_nested_again2 = 111;\n"
        + "            optional string extension_nested_again = 110;\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n"
        + "message Extended {\n"
        + "  optional string vaulted_data = 1;\n"
        + "  optional string field_normal = 2;\n"
        + "\n"
        + "  extensions 100 to 199;\n"
        + "\n"
        + "  message NestedExtended {\n"
        + "    optional string vaulted_data = 1;\n"
        + "    optional string field_normal = 2;\n"
        + "    optional string field_vault_replace = 3;\n"
        + "  \n"
        + "    extensions 100 to 199;\n"
        + "  \n"
        + "    extend NestedExtended {\n"
        + "      optional string extension_nested_trouble2 = 113;\n"
        + "      optional string extension_nested_trouble = 112;\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    String normalized = "package my.package;\n"
        + "\n"
        + "message ExtendedDummy {\n"
        + "  extend .my.package.Extended {\n"
        + "    optional string extension_normal = 100;\n"
        + "    optional string extension_normal2 = 101;\n"
        + "  }\n"
        + "\n"
        + "  message Nested {\n"
        + "    extend .my.package.Extended {\n"
        + "      optional string extension_identifier = 102;\n"
        + "      optional string extension_identifier2 = 103;\n"
        + "    }\n"
        + "  \n"
        + "    message Oh {\n"
        + "      extend .my.package.Extended {\n"
        + "        optional string extension_vault_replace = 104;\n"
        + "        optional string extension_vault_replace2 = 105;\n"
        + "      }\n"
        + "    \n"
        + "      message My {\n"
        + "        extend .my.package.Extended {\n"
        + "          optional string extension_fidelius_token = 106;\n"
        + "          optional string extension_fidelius_token2 = 107;\n"
        + "        }\n"
        + "      \n"
        + "        message God {\n"
        + "          extend .my.package.Extended.NestedExtended {\n"
        + "            optional string extension_nested_entity = 108;\n"
        + "            optional string extension_nested_entity2 = 109;\n"
        + "            optional string extension_nested_again = 110;\n"
        + "            optional string extension_nested_again2 = 111;\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n"
        + "message Extended {\n"
        + "  optional string vaulted_data = 1;\n"
        + "  optional string field_normal = 2;\n"
        + "\n"
        + "  extensions 100 to 199;\n"
        + "\n"
        + "  message NestedExtended {\n"
        + "    optional string vaulted_data = 1;\n"
        + "    optional string field_normal = 2;\n"
        + "    optional string field_vault_replace = 3;\n"
        + "  \n"
        + "    extensions 100 to 199;\n"
        + "  \n"
        + "    extend .my.package.Extended.NestedExtended {\n"
        + "      optional string extension_nested_trouble = 112;\n"
        + "      optional string extension_nested_trouble2 = 113;\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(canonical, schema.canonicalString());
    Descriptor descriptor = schema.toDescriptor();
    assertNotNull(descriptor);
    ProtobufSchema normalizedSchema = schema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
    descriptor = normalizedSchema.toDescriptor();
    assertNotNull(descriptor);
    normalizedSchema = new ProtobufSchema(descriptor).normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());

    String expected = "package my.package;\n"
        + "\n"
        + "message ExtendedDummy {\n"
        + "  message Nested {\n"
        + "    message Oh {\n"
        + "      message My {\n"
        + "        message God {}\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n"
        + "message Extended {\n"
        + "  optional string vaulted_data = 1;\n"
        + "  optional string field_normal = 2;\n"
        + "\n"
        + "  message NestedExtended {\n"
        + "    optional string vaulted_data = 1;\n"
        + "    optional string field_normal = 2;\n"
        + "    optional string field_vault_replace = 3;\n"
        + "  }\n"
        + "}\n";
    String noExtSchema = normalizedSchema.formattedString(Format.IGNORE_EXTENSIONS.symbol());
    assertEquals(expected, noExtSchema);
  }

  @Test
  public void testNormalizationWithExtensions2() {
    // Same as CustomOptions2.proto
    String schemaString = "syntax = \"proto2\";\n"
        + "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message FooBar {\n"
        + "  extensions 100 to 200;\n"
        + "\n"
        + "  optional int32 foo = 1;\n"
        + "  optional string bar = 2;\n"
        + "  repeated FooBar nested = 3;\n"
        + "\n"
        + "  message More {\n"
        + "    repeated int32 serial = 1;\n"
        + "\n"
        + "    extend FooBar {\n"
        + "      optional string more_string = 150;\n"
        + "    }\n"
        + "\n"
        + "    option (my_message_option) = {[FooBar.More2.more2_string]: \"foobar\", [rep]: []};\n"
        + "  }\n"
        + "\n"
        + "  extend google.protobuf.EnumOptions {\n"
        + "    optional string foobar_string = 71001;\n"
        + "  }\n"
        + "\n"
        + "  enum FooBarBazEnum {\n"
        + "    option (FooBar.foobar_string) = \"foobar\";\n"
        + "\n"
        + "    FOO = 1;\n"
        + "    BAR = 2;\n"
        + "    BAZ = 3;\n"
        + "  }\n"
        + "}\n"
        + "\n"
        + "extend google.protobuf.MessageOptions {\n"
        + "  optional FooBar my_message_option = 50001;\n"
        + "}\n"
        + "\n"
        + "extend FooBar {\n"
        + "  optional FooBar.FooBarBazEnum ext = 101;\n"
        + "  repeated FooBar.FooBarBazEnum rep = 102;\n"
        + "}";

    String canonical = "syntax = \"proto2\";\n"
        + "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message FooBar {\n"
        + "  optional int32 foo = 1;\n"
        + "  optional string bar = 2;\n"
        + "  repeated FooBar nested = 3;\n"
        + "\n"
        + "  extensions 100 to 200;\n"
        + "\n"
        + "  extend google.protobuf.EnumOptions {\n"
        + "    optional string foobar_string = 71001;\n"
        + "  }\n"
        + "\n"
        + ""
        + "  message More {\n"
        + "    option (my_message_option) = {\n"
        + "      [FooBar.More2.more2_string]: \"foobar\",\n"
        + "      [rep]: [\n"
        + "      ]\n"
        + "    };\n"
        + "  \n"
        + "    repeated int32 serial = 1;\n"
        + "  \n"
        + "    extend FooBar {\n"
        + "      optional string more_string = 150;\n"
        + "    }\n"
        + "  }\n"
        + "  enum FooBarBazEnum {\n"
        + "    option (FooBar.foobar_string) = \"foobar\";\n"
        + "    FOO = 1;\n"
        + "    BAR = 2;\n"
        + "    BAZ = 3;\n"
        + "  }\n"
        + "}\n"
        + "\n"
        + "extend google.protobuf.MessageOptions {\n"
        + "  optional FooBar my_message_option = 50001;\n"
        + "}\n"
        + "extend FooBar {\n"
        + "  optional FooBar.FooBarBazEnum ext = 101;\n"
        + "  repeated FooBar.FooBarBazEnum rep = 102;\n"
        + "}\n";

    String normalized = "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message FooBar {\n"
        + "  optional int32 foo = 1;\n"
        + "  optional string bar = 2;\n"
        + "  repeated .io.confluent.kafka.serializers.protobuf.test.FooBar nested = 3;\n"
        + "\n"
        + "  extensions 100 to 200;\n"
        + "\n"
        + "  extend .google.protobuf.EnumOptions {\n"
        + "    optional string foobar_string = 71001;\n"
        + "  }\n"
        + "\n"
        + "  message More {\n"
        + "    option (io.confluent.kafka.serializers.protobuf.test.my_message_option) = {\n"
        + "      [FooBar.More2.more2_string]: \"foobar\"\n"
        + "    };\n"
        + "  \n"
        + "    repeated int32 serial = 1;\n"
        + "  \n"
        + "    extend .io.confluent.kafka.serializers.protobuf.test.FooBar {\n"
        + "      optional string more_string = 150;\n"
        + "    }\n"
        + "  }\n"
        + "  enum FooBarBazEnum {\n"
        + "    option (io.confluent.kafka.serializers.protobuf.test.FooBar.foobar_string) = \"foobar\";\n"
        + "    FOO = 1;\n"
        + "    BAR = 2;\n"
        + "    BAZ = 3;\n"
        + "  }\n"
        + "}\n"
        + "\n"
        + "extend .google.protobuf.MessageOptions {\n"
        + "  optional .io.confluent.kafka.serializers.protobuf.test.FooBar my_message_option = 50001;\n"
        + "}\n"
        + "extend .io.confluent.kafka.serializers.protobuf.test.FooBar {\n"
        + "  optional .io.confluent.kafka.serializers.protobuf.test.FooBar.FooBarBazEnum ext = 101;\n"
        + "  repeated .io.confluent.kafka.serializers.protobuf.test.FooBar.FooBarBazEnum rep = 102;\n"
        + "}\n";

    String normalizedWithoutCustomOptions = "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message FooBar {\n"
        + "  optional int32 foo = 1;\n"
        + "  optional string bar = 2;\n"
        + "  repeated .io.confluent.kafka.serializers.protobuf.test.FooBar nested = 3;\n"
        + "\n"
        + "  extensions 100 to 200;\n"
        + "\n"
        + "  extend .google.protobuf.EnumOptions {\n"
        + "    optional string foobar_string = 71001;\n"
        + "  }\n"
        + "\n"
        + "  message More {\n"
        + "    repeated int32 serial = 1;\n"
        + "  \n"
        + "    extend .io.confluent.kafka.serializers.protobuf.test.FooBar {\n"
        + "      optional string more_string = 150;\n"
        + "    }\n"
        + "  }\n"
        + "  enum FooBarBazEnum {\n"
        + "    FOO = 1;\n"
        + "    BAR = 2;\n"
        + "    BAZ = 3;\n"
        + "  }\n"
        + "}\n"
        + "\n"
        + "extend .google.protobuf.MessageOptions {\n"
        + "  optional .io.confluent.kafka.serializers.protobuf.test.FooBar my_message_option = 50001;\n"
        + "}\n"
        + "extend .io.confluent.kafka.serializers.protobuf.test.FooBar {\n"
        + "  optional .io.confluent.kafka.serializers.protobuf.test.FooBar.FooBarBazEnum ext = 101;\n"
        + "  repeated .io.confluent.kafka.serializers.protobuf.test.FooBar.FooBarBazEnum rep = 102;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(canonical, schema.canonicalString());
    Descriptor descriptor = schema.toDescriptor();
    assertNotNull(descriptor);
    ProtobufSchema normalizedSchema = schema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
    descriptor = normalizedSchema.toDescriptor();
    assertNotNull(descriptor);
    normalizedSchema = new ProtobufSchema(descriptor).normalize();
    assertEquals(normalizedWithoutCustomOptions, normalizedSchema.canonicalString());

    String expected = "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "\n"
        + "message FooBar {\n"
        + "  optional int32 foo = 1;\n"
        + "  optional string bar = 2;\n"
        + "  repeated .io.confluent.kafka.serializers.protobuf.test.FooBar nested = 3;\n"
        + "\n"
        + "  message More {\n"
        + "    repeated int32 serial = 1;\n"
        + "  }\n"
        + "  enum FooBarBazEnum {\n"
        + "    FOO = 1;\n"
        + "    BAR = 2;\n"
        + "    BAZ = 3;\n"
        + "  }\n"
        + "}\n";
    String noExtSchema = normalizedSchema.formattedString(Format.IGNORE_EXTENSIONS.symbol());
    assertEquals(expected, noExtSchema);
  }

  @Test
  public void testNormalizationWithComplexCustomOptions() {
    String schemaString = "package acme.common;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_outer_classname = \"ExternalMetadata\";\n"
        + "option java_package = \"com.acme.protos.common\";\n"
        + "\n"
        + "message ExternalMetadata {\n"
        + "  /** The content type of the blob contained in metadata **/\n"
        + "  optional string content_type = 1;\n"
        + "  /** The arbitrary encoding of bytes **/\n"
        + "  optional bytes metadata = 2 [\n"
        + "    default = 0, (length).min = 1.0, (length).max = 1024.0];\n"
        + "}\n"
        + "\n"
        + "/** Field level validation options */\n"
        + "extend google.protobuf.FieldOptions {\n"
        + "  optional Range length = 22301;\n"
        + "}\n"
        + "\n"
        + "/** A range of numeric values */\n"
        + "message Range {\n"
        + "  /** The minimum allowable value */\n"
        + "  optional double min = 1 [default = -inf];\n;\n"
        + "\n"
        + "  /** The maximum allowable value */\n"
        + "  optional double max = 2 [default = nan];\n;\n"
        + "}\n";
    String normalized = "package acme.common;\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_outer_classname = \"ExternalMetadata\";\n"
        + "option java_package = \"com.acme.protos.common\";\n"
        + "\n"
        + "message ExternalMetadata {\n"
        + "  optional string content_type = 1;\n"
        + "  optional bytes metadata = 2 [\n"
        + "    (acme.common.length) = {\n"
        + "      max: 1024,\n"
        + "      min: 1\n"
        + "    },\n"
        + "    default = 0\n"
        + "  ];\n"
        + "}\n"
        + "message Range {\n"
        + "  optional double min = 1 [default = -inf];\n"
        + "  optional double max = 2 [default = nan];\n"
        + "}\n"
        + "\n"
        + "extend .google.protobuf.FieldOptions {\n"
        + "  optional .acme.common.Range length = 22301;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(schemaString);
    ProtobufSchema normalizedSchema = schema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
  }

  @Test
  public void testNormalizationWithPackagePrefix() {
    String schemaString = "syntax = \"proto3\";\n"
        + "package confluent.package;\n"
        + "\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "message Nested {\n"
        + "  type.Decimal test_cflt_decimal = 1;\n"
        + "}\n";

    String normalized = "syntax = \"proto3\";\n"
        + "package confluent.package;\n"
        + "\n"
        + "import \"confluent/type/decimal.proto\";\n"
        + "\n"
        + "message Nested {\n"
        + "  .confluent.type.Decimal test_cflt_decimal = 1;\n"
        + "}\n";

    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(schemaString, schema.canonicalString());
    Descriptor descriptor = schema.toDescriptor();
    assertNotNull(descriptor);
    ProtobufSchema normalizedSchema = schema.normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
    descriptor = normalizedSchema.toDescriptor();
    assertNotNull(descriptor);
    normalizedSchema = new ProtobufSchema(descriptor).normalize();
    assertEquals(normalized, normalizedSchema.canonicalString());
  }

  @Test
  public void testEnumAfterMessage() throws Exception {
    assertEquals(enumAfterMessageSchemaString, enumBeforeMessageSchema.canonicalString());
    assertEquals(enumAfterMessageSchemaString,
        new ProtobufSchema(enumBeforeMessageSchema.toDescriptor()).canonicalString());
  }

  @Test
  public void testParseSchema() {
    SchemaProvider protobufSchemaProvider = new ProtobufSchemaProvider();
    ParsedSchema parsedSchema = protobufSchemaProvider.parseSchemaOrElseThrow(
        new Schema(null, null, null, ProtobufSchema.TYPE, new ArrayList<>(), recordSchemaString), false, false);
    Optional<ParsedSchema> parsedSchemaOptional = protobufSchemaProvider.parseSchema(recordSchemaString,
            new ArrayList<>(), false, false);

    assertNotNull(parsedSchema);
    assertTrue(parsedSchemaOptional.isPresent());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseSchemaThrowException() {
    SchemaProvider protobufSchemaProvider = new ProtobufSchemaProvider();
    protobufSchemaProvider.parseSchemaOrElseThrow(
        new Schema(null, null, null, ProtobufSchema.TYPE, new ArrayList<>(), invalidSchemaString), false, false);
  }

  @Test
  public void testParseSchemaSuppressException() {
    SchemaProvider protobufSchemaProvider = new ProtobufSchemaProvider();
    Optional<ParsedSchema> parsedSchema = protobufSchemaProvider.parseSchema(invalidSchemaString,
            new ArrayList<>(), false, false);
    assertFalse(parsedSchema.isPresent());
  }

  @Test
  public void testEnumMethods() {
    EnumDescriptor enumDescriptor = enumSchema.getEnumDescriptor("TestEnum.Suit");
    ProtobufSchema enumSchema2 = new ProtobufSchema(enumDescriptor);
    EnumDescriptor enumDescriptor2 = enumSchema2.getEnumDescriptor("TestEnum.Suit");
    assertEquals(enumDescriptor.getFullName(), enumDescriptor2.getFullName());
  }

  @Test
  public void testNumberFormats() throws Exception {
    FormatContext ctx = new FormatContext(false, true);
    checkNumber(ctx, "123", "123");
    checkNumber(ctx, "0123", "83"); // octal
    checkNumber(ctx, "0x123", "291"); // hex
    checkNumber(ctx, "0123.0", "123");
    checkNumber(ctx, "123.", "123");
    checkNumber(ctx, "123.0", "123");
    checkNumber(ctx, "123.00", "123");
    checkNumber(ctx, "123e1", "1230");
    checkNumber(ctx, "123E1", "1230");
    checkNumber(ctx, "123E+1", "1230");
    checkNumber(ctx, "123E-1", "12.3");
    checkNumber(ctx, ".123E+3", "123");
    checkNumber(ctx, "1.23E+3", "1230");
    checkNumber(ctx, "12.3E+3", "12300");
    checkNumber(ctx, "123.E+3", "123000");
    checkNumber(ctx, "123.4E+3", "123400");
    checkNumber(ctx, "123.45E+3", "123450");
    checkNumber(ctx, "123.456E+3", "123456");
    checkNumber(ctx, "123.4567E+2", "12345.67");
  }

  @Test
  public void testBasicAddAndRemoveSchemaTags() {
    String schemaString = "syntax = \"proto3\";\n" +
      "package com.example.mynamespace;\n" +
      "\n" +
      "import \"confluent/meta.proto\";\n" +
      "import \"confluent/type/decimal.proto\";\n" +
      "\n" +
      "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n" +
      "option java_outer_classname = \"ComplexProto\";\n" +
      "option (confluent.file_meta).tags = \"FILE\";\n" +
      "\n" +
      "message SampleRecord {\n" +
      "  option (confluent.message_meta).tags = \"OTHER\";\n" +
      "\n" +
      "  int32 my_field1 = 1 [\n" +
      "    (confluent.field_meta).tags = \"OTHER\",\n" +
      "    (confluent.field_meta).tags = \"PRIVATE\"\n" +
      "  ];\n" +
      "  double my_field2 = 2 [(confluent.field_meta) = {\n" +
      "    tags: [ \"PRIVATE\" ]\n" +
      "  }];\n" +
      "  NestedRecord my_field3 = 3 [(confluent.field_meta) = {\n" +
      "    doc: \"field_meta\"\n" +
      "  }];\n" +
      "\n" +
      "  message NestedRecord {\n" +
      "    int64 nested_filed1 = 4;\n" +
      "  }\n" +
      "  enum Kind {\n" +
      "    option (confluent.enum_meta).tags = \"ENUM\";\n" +
      "    APPLE = 6 [(confluent.enum_value_meta) = {\n" +
      "      tags: [\n" +
      "        \"CONST\"\n" +
      "      ]\n" +
      "    }];\n" +
      "    BANANA = 7;\n" +
      "  }\n" +
      "}\n";

    String expectedString = "syntax = \"proto3\";\n" +
      "package com.example.mynamespace;\n" +
      "\n" +
      "import \"confluent/meta.proto\";\n" +
      "import \"confluent/type/decimal.proto\";\n" +
      "\n" +
      "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n" +
      "option java_outer_classname = \"ComplexProto\";\n" +
      "option (confluent.file_meta).tags = \"FILE\";\n" +
      "\n" +
      "message SampleRecord {\n" +
      "  option (confluent.message_meta) = {\n" +
      "    tags: [\n" +
      "      \"OTHER\",\n" +
      "      \"PII\"\n" +
      "    ]\n" +
      "  };\n" +
      "\n" +
      "  int32 my_field1 = 1 [(confluent.field_meta) = {\n" +
      "    tags: [\n" +
      "      \"OTHER\",\n" +
      "      \"PRIVATE\",\n" +
      "      \"PII\"\n" +
      "    ]\n" +
      "  }];\n" +
      "  double my_field2 = 2 [(confluent.field_meta) = {\n" +
      "    tags: [\n" +
      "      \"PRIVATE\",\n" +
      "      \"PII\"\n" +
      "    ]\n" +
      "  }];\n" +
      "  NestedRecord my_field3 = 3 [(confluent.field_meta) = {\n" +
      "    doc: \"field_meta\",\n" +
      "    tags: [\n" +
      "      \"PII\"\n" +
      "    ]\n" +
      "  }];\n" +
      "\n" +
      "  message NestedRecord {\n" +
      "    option (confluent.message_meta) = {\n" +
      "      tags: [\n" +
      "        \"PII\"\n" +
      "      ]\n" +
      "    };\n" +
      "  \n" +
      "    int64 nested_filed1 = 4 [(confluent.field_meta) = {\n" +
      "      tags: [\n" +
      "        \"PII\"\n" +
      "      ]\n" +
      "    }];\n" +
      "  }\n" +
      "  enum Kind {\n" +
      "    option (confluent.enum_meta).tags = \"ENUM\";\n" +
      "    APPLE = 6 [(confluent.enum_value_meta) = {\n" +
      "      tags: [\n" +
      "        \"CONST\"\n" +
      "      ]\n" +
      "    }];\n" +
      "    BANANA = 7;\n" +
      "  }\n" +
      "}\n";

    String removedTagSchema = "syntax = \"proto3\";\n" +
      "package com.example.mynamespace;\n" +
      "\n" +
      "import \"confluent/meta.proto\";\n" +
      "import \"confluent/type/decimal.proto\";\n" +
      "\n" +
      "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n" +
      "option java_outer_classname = \"ComplexProto\";\n" +
      "option (confluent.file_meta).tags = \"FILE\";\n" +
      "\n" +
      "message SampleRecord {\n" +
      "  option (confluent.message_meta) = {\n" +
      "    tags: [\n" +
      "      \"PII\"\n" +
      "    ]\n" +
      "  };\n" +
      "\n" +
      "  int32 my_field1 = 1 [(confluent.field_meta) = {\n" +
      "    tags: [\n" +
      "      \"PRIVATE\",\n" +
      "      \"PII\"\n" +
      "    ]\n" +
      "  }];\n" +
      "  double my_field2 = 2 [(confluent.field_meta) = {\n" +
      "    tags: [\n" +
      "      \"PII\"\n" +
      "    ]\n" +
      "  }];\n" +
      "  NestedRecord my_field3 = 3 [(confluent.field_meta) = {\n" +
      "    doc: \"field_meta\"\n" +
      "  }];\n" +
      "\n" +
      "  message NestedRecord {\n" +
      "    int64 nested_filed1 = 4 [(confluent.field_meta) = {\n" +
      "      tags: [\n" +
      "        \"PII\"\n" +
      "      ]\n" +
      "    }];\n" +
      "  }\n" +
      "  enum Kind {\n" +
      "    option (confluent.enum_meta).tags = \"ENUM\";\n" +
      "    APPLE = 6 [(confluent.enum_value_meta) = {\n" +
      "      tags: [\n" +
      "        \"CONST\"\n" +
      "      ]\n" +
      "    }];\n" +
      "    BANANA = 7;\n" +
      "  }\n" +
      "}\n";

    Map<SchemaEntity, Set<String>> tagsToAdd = new HashMap<>();
    tagsToAdd.put(new SchemaEntity(".SampleRecord.my_field1",
      SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PII"));
    tagsToAdd.put(new SchemaEntity(".SampleRecord.my_field2",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PII"));
    tagsToAdd.put(new SchemaEntity(".SampleRecord.my_field3",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PII"));
    tagsToAdd.put(new SchemaEntity(".SampleRecord.NestedRecord.nested_filed1",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PII"));
    tagsToAdd.put(new SchemaEntity(".SampleRecord",
        SchemaEntity.EntityType.SR_RECORD),
      Collections.singleton("PII"));
    tagsToAdd.put(new SchemaEntity(".SampleRecord.NestedRecord",
        SchemaEntity.EntityType.SR_RECORD),
      Collections.singleton("PII"));
    ParsedSchema schema = new ProtobufSchema(schemaString).copy(tagsToAdd, Collections.emptyMap());
    assertEquals(expectedString, schema.canonicalString());
    assertEquals(ImmutableSet.of("FILE", "OTHER", "PII", "PRIVATE", "ENUM", "CONST"), schema.inlineTags());

    Map<SchemaEntity, Set<String>> tagsToRemove = new HashMap<>();
    tagsToRemove.put(new SchemaEntity(".SampleRecord.my_field1",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("OTHER"));
    tagsToRemove.put(new SchemaEntity(".SampleRecord.my_field2",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PRIVATE"));
    tagsToRemove.put(new SchemaEntity(".SampleRecord.my_field3",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PII"));
    tagsToRemove.put(new SchemaEntity(".SampleRecord.NestedRecord.nested_filed1",
        SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("DOES_NOT_EXIST"));
    tagsToRemove.put(new SchemaEntity(".SampleRecord",
        SchemaEntity.EntityType.SR_RECORD),
      Collections.singleton("OTHER"));
    tagsToRemove.put(new SchemaEntity(".SampleRecord.NestedRecord",
        SchemaEntity.EntityType.SR_RECORD),
      Collections.singleton("PII"));
    schema = new ProtobufSchema(schema.canonicalString()).copy(Collections.emptyMap(), tagsToRemove);
    assertEquals(removedTagSchema, schema.canonicalString());
    assertEquals(ImmutableSet.of("FILE", "PII", "PRIVATE", "ENUM", "CONST"), schema.inlineTags());

    Map<String, Set<String>> pathTags =
        Collections.singletonMap("some.path", Collections.singleton("EXTERNAL"));
    Metadata metadata = new Metadata(pathTags, null, null);
    schema = schema.copy(metadata, null);
    assertEquals(ImmutableSet.of("FILE", "PII", "PRIVATE", "ENUM", "CONST", "EXTERNAL"), schema.tags());
  }

  @Test
  public void testAddDuplicateTags() {
    String schemaString = "syntax = \"proto3\";\n" +
        "package com.example.mynamespace;\n" +
        "\n" +
        "import \"confluent/meta.proto\";\n" +
        "import \"confluent/type/decimal.proto\";\n" +
        "\n" +
        "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n" +
        "option java_outer_classname = \"ComplexProto\";\n" +
        "\n" +
        "message SampleRecord {\n" +
        "  int32 my_field1 = 1 [(confluent.field_meta).tags = \"PRIVATE\"];\n" +
        "  double my_field2 = 2 [(confluent.field_meta) = {\n" +
        "    tags: [\n" +
        "      \"PRIVATE\"\n" +
        "    ]\n" +
        "  }];\n" +
        "  NestedRecord my_field3 = 3 [(confluent.field_meta) = {\n" +
        "    doc: \"field_meta\"\n" +
        "  }];\n" +
        "\n" +
        "  message NestedRecord {\n" +
        "    int64 nested_filed1 = 4;\n" +
        "  }\n" +
        "}\n";

    String expectedSchemaString = "syntax = \"proto3\";\n" +
      "package com.example.mynamespace;\n" +
      "\n" +
      "import \"confluent/meta.proto\";\n" +
      "import \"confluent/type/decimal.proto\";\n" +
      "\n" +
      "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n" +
      "option java_outer_classname = \"ComplexProto\";\n" +
      "\n" +
      "message SampleRecord {\n" +
      "  int32 my_field1 = 1 [(confluent.field_meta) = {\n" +
      "    tags: [\n" +
      "      \"PRIVATE\"\n" +
      "    ]\n" +
      "  }];\n" +
      "  double my_field2 = 2 [(confluent.field_meta) = {\n" +
      "    tags: [\n" +
      "      \"PRIVATE\"\n" +
      "    ]\n" +
      "  }];\n" +
      "  NestedRecord my_field3 = 3 [(confluent.field_meta) = {\n" +
      "    doc: \"field_meta\"\n" +
      "  }];\n" +
      "\n" +
      "  message NestedRecord {\n" +
      "    int64 nested_filed1 = 4;\n" +
      "  }\n" +
      "}\n";

    Map<SchemaEntity, Set<String>> tags = new HashMap<>();
    tags.put(new SchemaEntity(".SampleRecord.my_field1",
      SchemaEntity.EntityType.SR_FIELD),
      Collections.singleton("PRIVATE"));
    ParsedSchema schema = new ProtobufSchema(schemaString).copy(tags, Collections.emptyMap());
    assertEquals(expectedSchemaString, schema.canonicalString());
    assertEquals(ImmutableSet.of("PRIVATE"), schema.inlineTags());
  }

  @Test
  public void testComplexRemove() {
    String schemaString = "syntax = \"proto3\";\n" +
        "package com.example.mynamespace;\n" +
        "\n" +
        "import \"confluent/meta.proto\";\n" +
        "import \"confluent/type/decimal.proto\";\n" +
        "\n" +
        "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n" +
        "option java_outer_classname = \"ComplexProto\";\n" +
        "\n" +
        "message SampleRecord {\n" +
        "  int32 my_field1 = 1 [(confluent.field_meta).tags = \"PRIVATE\"];\n" +
        "  double my_field2 = 2 [(confluent.field_meta) = {\n" +
        "    doc: \"no tag\"\n" +
        "  }];\n" +
        "  NestedRecord my_field3 = 3 [(confluent.field_meta) = {\n" +
        "    doc: \"field_meta\",\n" +
        "    tags: [\n" +
        "      \"PII\",\n" +
        "      \"PRIVATE\"\n" +
        "    ]\n" +
        "  }];\n" +
        "  int32 my_field4 = 5 [\n" +
        "    otherOption = \"foo\",\n" +
        "    (confluent.field_meta).tags = \"PRIVATE\"\n" +
        "  ];\n" +
        "  oneof some_val {\n" +
        "    int32 other_id = 11 [(confluent.field_meta).tags = \"PRIVATE\"];\n" +
        "    string one_id = 12;\n" +
        "  }\n" +
        "\n" +
        "  message NestedRecord {\n" +
        "    int64 nested_filed1 = 4 [(confluent.field_meta).tags = [\"PII\"]];\n" +
        "  }\n" +
        "}\n";

    String afterRemovedTag = "syntax = \"proto3\";\n" +
      "package com.example.mynamespace;\n" +
      "\n" +
      "import \"confluent/meta.proto\";\n" +
      "import \"confluent/type/decimal.proto\";\n" +
      "\n" +
      "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n" +
      "option java_outer_classname = \"ComplexProto\";\n" +
      "\n" +
      "message SampleRecord {\n" +
      "  int32 my_field1 = 1;\n" +
      "  double my_field2 = 2 [(confluent.field_meta) = {\n" +
      "    doc: \"no tag\"\n" +
      "  }];\n" +
      "  NestedRecord my_field3 = 3 [(confluent.field_meta) = {\n" +
      "    doc: \"field_meta\",\n" +
      "    tags: [\n" +
      "      \"PII\"\n" +
      "    ]\n" +
      "  }];\n" +
      "  int32 my_field4 = 5 [otherOption = \"foo\"];\n" +
      "\n" +
      "  oneof some_val {\n" +
      "    int32 other_id = 11;\n" +
      "    string one_id = 12;\n" +
      "  }\n" +
      "\n" +
      "  message NestedRecord {\n" +
      "    int64 nested_filed1 = 4 [(confluent.field_meta) = {\n" +
      "      tags: [\n" +
      "        \"PII\"\n" +
      "      ]\n" +
      "    }];\n" +
      "  }\n" +
      "}\n";

    Map<SchemaEntity, Set<String>> tags = new HashMap<>();
    tags.put(new SchemaEntity(".SampleRecord.my_field1",
      SchemaEntity.EntityType.SR_FIELD), Collections.singleton("PRIVATE"));
    tags.put(new SchemaEntity(".SampleRecord.my_field2",
      SchemaEntity.EntityType.SR_FIELD), Collections.singleton("PRIVATE"));
    tags.put(new SchemaEntity(".SampleRecord.my_field3",
      SchemaEntity.EntityType.SR_FIELD), Collections.singleton("PRIVATE"));
    tags.put(new SchemaEntity(".SampleRecord.my_field4",
      SchemaEntity.EntityType.SR_FIELD), Collections.singleton("PRIVATE"));
    tags.put(new SchemaEntity(".SampleRecord.NestedRecord.nested_filed1",
      SchemaEntity.EntityType.SR_FIELD), Collections.singleton("PRIVATE"));
    tags.put(new SchemaEntity(".SampleRecord.some_val.other_id",
        SchemaEntity.EntityType.SR_FIELD), Collections.singleton("PRIVATE"));
    ParsedSchema schema = new ProtobufSchema(schemaString).copy(Collections.emptyMap(), tags);
    assertEquals(afterRemovedTag, schema.canonicalString());
    assertEquals(ImmutableSet.of("PII"), schema.inlineTags());
  }

  @Test
  public void testOptionalOneOfFieldName() {
    String schemaString = "syntax = \"proto3\";\n" +
        "package com.example.mynamespace;\n" +
        "\n" +
        "message SampleRecord {\n" +
        "  oneof test_oneof {\n" +
        "    string f1 = 1;\n" +
        "    int32 f2 = 2;\n" +
        "  }\n" +
        "}\n";

    String expectedSchemaString = "syntax = \"proto3\";\n" +
        "package com.example.mynamespace;\n" +
        "\n" +
        "message SampleRecord {\n" +
        "  oneof test_oneof {\n" +
        "    string f1 = 1 [(confluent.field_meta) = {\n" +
        "      tags: [\n" +
        "        \"TAG1\",\n" +
        "        \"TAG2\"\n" +
        "      ]\n" +
        "    }];\n" +
        "    int32 f2 = 2 [(confluent.field_meta) = {\n" +
        "      tags: [\n" +
        "        \"TAG3\",\n" +
        "        \"TAG4\"\n" +
        "      ]\n" +
        "    }];\n" +
        "  }\n" +
        "}\n";

    Map<SchemaEntity, Set<String>> tags = new LinkedHashMap<>();
    tags.put(new SchemaEntity(".SampleRecord.f1",
        SchemaEntity.EntityType.SR_FIELD), Collections.singleton("TAG1"));
    tags.put(new SchemaEntity(".SampleRecord.test_oneof.f1",
        SchemaEntity.EntityType.SR_FIELD), Collections.singleton("TAG2"));
    tags.put(new SchemaEntity("SampleRecord.f2",
        SchemaEntity.EntityType.SR_FIELD), Collections.singleton("TAG3"));
    tags.put(new SchemaEntity("SampleRecord.test_oneof.f2",
        SchemaEntity.EntityType.SR_FIELD), Collections.singleton("TAG4"));
    ParsedSchema result = new ProtobufSchema(schemaString).copy(tags, Collections.emptyMap());
    assertEquals(expectedSchemaString, result.canonicalString());
    assertEquals(ImmutableSet.of("TAG1", "TAG2", "TAG3", "TAG4"), result.inlineTags());
  }

  @Test
  public void testInvalidPath() {
    String schemaString = "syntax = \"proto3\";\n" +
        "package com.example.mynamespace;\n" +
        "\n" +
        "message SampleRecord {\n" +
        "  oneof test_oneof {\n" +
        "    string f1 = 1;\n" +
        "    int32 test_oneof = 2;\n" +
        "  }\n" +
        "}\n";
    ParsedSchema origin = new ProtobufSchema(schemaString);
    Set<String> toAdd = Collections.singleton("TAG1");

    Map<SchemaEntity, Set<String>> tags = new HashMap<>();
    tags.put(new SchemaEntity("SampleRecord", SchemaEntity.EntityType.SR_FIELD), toAdd);
    assertThrows("Missing field", IllegalArgumentException.class,
        () -> origin.copy(tags, Collections.emptyMap()));

    Map<SchemaEntity, Set<String>> tags2 = new HashMap<>();
    tags2.put(new SchemaEntity("SampleRecord.bad_oneof.f1", SchemaEntity.EntityType.SR_FIELD), toAdd);
    assertThrows("Bad oneOf fieldName", IllegalArgumentException.class,
        () -> origin.copy(tags2, Collections.emptyMap()));

    Map<SchemaEntity, Set<String>> tags3 = new HashMap<>();
    tags3.put(new SchemaEntity("SampleRecord.f3", SchemaEntity.EntityType.SR_FIELD), toAdd);
    assertThrows("Non-existing field", IllegalArgumentException.class,
        () -> origin.copy(tags3, Collections.emptyMap()));

    Map<SchemaEntity, Set<String>> tags4 = new HashMap<>();
    tags4.put(new SchemaEntity("badRecord", SchemaEntity.EntityType.SR_RECORD), toAdd);
    assertThrows("Non-existing message", IllegalArgumentException.class,
        () -> origin.copy(tags4, Collections.emptyMap()));

    Map<SchemaEntity, Set<String>> tags5 = new HashMap<>();
    tags5.put(new SchemaEntity("..SampleRecord", SchemaEntity.EntityType.SR_RECORD), toAdd);
    assertThrows("Invalid path", IllegalArgumentException.class,
        () -> origin.copy(tags5, Collections.emptyMap()));

    Map<SchemaEntity, Set<String>> tags6 = new HashMap<>();
    tags6.put(new SchemaEntity(".SampleRecord.f1", SchemaEntity.EntityType.SR_RECORD), toAdd);
    assertThrows("Missing oneOf fieldName", IllegalArgumentException.class,
        () -> origin.copy(tags6, Collections.emptyMap()));
  }

  @Test
  public void testProtoFileElementDeserializer() {
    String schemaString = "syntax = \"proto3\";\n" +
        "package com.example.mynamespace;\n" +
        "\n" +
        "import \"confluent/meta.proto\";\n" +
        "import \"confluent/type/decimal.proto\";\n" +
        "\n" +
        "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n" +
        "option java_outer_classname = \"ComplexProto\";\n" +
        "option java_multiple_files = true;\n" +
        "option optimize_for = CODE_SIZE;\n" +
        "option int_num = 1;\n" +
        "option double = 2.2;\n" +
        "option (confluent.file_meta).tags = [\"PII\", \"PRIVATE\"];\n" +
        "\n" +
        "message SampleRecord {\n" +
        "  int32 my_field1 = 1 [\n" +
        "    tag = \"PII\",\n" +
        "    (confluent.field_meta).tags = \"PRIVATE\"\n" +
        "  ];\n" +
        "  double my_field2 = 2 [(com.util.meta) = {\n" +
        "    needValidation: false,\n" +
        "    boolArray: [ true, false ],\n" +
        "    numArray: [ 1, 2 ],\n" +
        "    textArray: [ \"aaa\", \"bbb\" ]\n" +
        "    customOption: {\n" +
        "      kind: \"custom\",\n" +
        "      value: true\n" +
        "    }\n" +
        "    customOption2: {\n" +
        "      kind: \"custom2\",\n" +
        "      value: [\"text\"]\n" +
        "    }\n" +
        "    fileMap: [\n" +
        "      { key: \"t\", value: \"raw_orders\"},\n" +
        "      { otherKey: \"t\", otherValue: 2.0}\n" +
        "    ]\n" +
        "  }];\n" +
        "  repeated string my_field3 = 3;\n" +
        "  com.example.mynamespace.SampleRecord2 my_field4 = 4;\n" +
        "  SampleRecord2 my_field5 = 5 [\n" +
        "    tag = 1,\n" +
        "    tag3 = \"noop\"\n" +
        "  ];\n" +
        "  optional confluent.type.Decimal value = 6 [(confluent.field_meta) = {\n" +
        "    params: [\n" +
        "      { key: \"precision\", value: \"8\" },\n" +
        "      { key: \"scale\", value: \"3\" }\n" +
        "    ],\n" +
        "    tags: [\n" +
        "      \"PII\",\n" +
        "      \"PRIVATE\"\n" +
        "    ]\n" +
        "  }];\n" +
        "  oneof qux {\n" +
        "    option (my_oneof_option) = 7;\n" +
        "    string quux = 8;\n" +
        "  }\n" +
        "}\n" +
        "message SampleRecord2 {\n" +
        "  int32 my_field1 = 1;\n" +
        "  NestedRecord proto_field4 = 5;\n" +
        "  Kind fruit = 8;\n" +
        "\n" +
        "  extensions 100 to 200;\n" +
        "  extensions 300;\n" +
        "\n" +
        "  message NestedRecord {\n" +
        "    reserved 9, 11 to 13;\n" +
        "    reserved \"foo\", \"bar\";\n" +
        "\n" +
        "    int64 nested_filed1 = 4;\n" +
        "  }\n" +
        "  enum Kind {\n" +
        "    APPLE = 6;\n" +
        "    BANANA = 7;\n" +
        "  }\n" +
        "}\n" +
        "\n" +
        "extend SampleRecord2 {\n" +
        "  optional int32 options = 122;\n" +
        "}\n" +
        "\n" +
        "service SearchService {\n" +
        "  rpc Search (SampleRecord2) returns (SampleRecord);\n" +
        "}\n\n";

    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(ProtoFileElement.class, new ProtoFileElementDeserializer());
    mapper.registerModule(module);

    ProtobufSchema parsedSchema = new ProtobufSchema(schemaString);
    JsonNode jsonNode = mapper.valueToTree(parsedSchema.rawSchema());
    String serialized = jsonNode.toString();

    try {
      ProtoFileElement result = mapper.readValue(serialized, ProtoFileElement.class);
      ParsedSchema resultParsedSchema = new ProtobufSchema(result, parsedSchema.references(), parsedSchema.dependencies());
      assertEquals(parsedSchema, resultParsedSchema);
    } catch (IOException e) {
      fail("Error deserializing Json to ProtoFileElement.");
    }
  }

  private void checkNumber(FormatContext ctx, String in, String out) {
    assertEquals(out, ctx.formatNumber(ctx.parseNumber(in)));
  }

  private static JsonNode jsonTree(String jsonData) {
    try {
      return objectMapper.readTree(jsonData);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON", e);
    }
  }

  private static String readFile(String fileName) {
    ResourceLoader resourceLoader = new ResourceLoader(
        "/io/confluent/kafka/schemaregistry/protobuf/diff/");
    return resourceLoader.toString(fileName);
  }
}
