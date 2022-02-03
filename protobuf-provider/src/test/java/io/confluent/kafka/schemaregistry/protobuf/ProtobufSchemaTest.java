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
