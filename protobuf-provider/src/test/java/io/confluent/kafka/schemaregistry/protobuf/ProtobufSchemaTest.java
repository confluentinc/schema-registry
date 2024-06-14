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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.protobuf.diff.ResourceLoader;

import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.PROTO3;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    ProtobufSchema schema = new ProtobufSchema(schemaString);
    assertEquals(canonicalString, schema.canonicalString());
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
        CompatibilityLevel.BACKWARD, Collections.singletonList(schema2)).isEmpty());
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
            CompatibilityLevel.BACKWARD, Collections.singletonList(schema2)).isEmpty());
    assertEquals(schema.canonicalString(), schema2.canonicalString());
  }

  @Test
  public void testFileDescriptorProto() throws Exception {
    ResourceLoader resourceLoader = new ResourceLoader(
        "/io/confluent/kafka/schemaregistry/protobuf/diff/");

    ProtoFileElement original = resourceLoader.readObj("TestProto.proto");
    ProtobufSchema schema = new ProtobufSchema(original.toSchema());
    String fileProto = schema.formattedString(ProtobufSchema.SERIALIZED_FORMAT);
    ProtobufSchema schema2 = new ProtobufSchema(fileProto);
    assertTrue(schema.isCompatible(
        CompatibilityLevel.BACKWARD, Collections.singletonList(schema2)).isEmpty());
    fileProto = schema2.formattedString(ProtobufSchema.SERIALIZED_FORMAT);
    ProtobufSchema schema3 = new ProtobufSchema(fileProto);
    assertTrue(schema2.isCompatible(
        CompatibilityLevel.BACKWARD, Collections.singletonList(schema3)).isEmpty());
    assertEquals(schema2, schema3);

    original = resourceLoader.readObj("NestedTestProto.proto");
    schema = new ProtobufSchema(original.toSchema());
    fileProto = schema.formattedString(ProtobufSchema.SERIALIZED_FORMAT);
    schema2 = new ProtobufSchema(fileProto);
    assertTrue(schema.isCompatible(
        CompatibilityLevel.BACKWARD, Collections.singletonList(schema2)).isEmpty());
    fileProto = schema2.formattedString(ProtobufSchema.SERIALIZED_FORMAT);
    schema3 = new ProtobufSchema(fileProto);
    assertTrue(schema2.isCompatible(
        CompatibilityLevel.BACKWARD, Collections.singletonList(schema3)).isEmpty());
    assertEquals(schema2, schema3);
  }

  @Test
  public void testDefaultOmittedInProto3String() throws Exception {
    MessageDefinition.Builder message = MessageDefinition.newBuilder("msg1");
    message.addField(null, "string", "field1", 1, "defaultVal", null, null);
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
    FieldDescriptor fd = schema.toDescriptor().findFieldByName("MIT_ACTUAL_PRICE");
    Meta meta = fd.getOptions().getExtension(MetaProto.fieldMeta);
    assertEquals("2", meta.getParamsOrThrow("scale"));

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
