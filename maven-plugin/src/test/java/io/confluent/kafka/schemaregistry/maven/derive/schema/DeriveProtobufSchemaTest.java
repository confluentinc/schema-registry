/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.maven.derive.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.ReadFileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class DeriveProtobufSchemaTest {

  final DeriveProtobufSchema strictGenerator = new DeriveProtobufSchema(true);
  final DeriveProtobufSchema lenientGenerator = new DeriveProtobufSchema(false);
  private final KafkaProtobufSerializer<DynamicMessage> protobufSerializer;
  private final KafkaProtobufDeserializer<com.google.protobuf.Message> protobufDeserializer;
  private final String topic;

  public DeriveProtobufSchemaTest() {
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    protobufSerializer = new KafkaProtobufSerializer<DynamicMessage>(schemaRegistry, new HashMap(serializerConfig));
    protobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry);
    topic = "test";
  }

  void serializeAndDeserializeCheckMulti(List<String> messages, List<JSONObject> schemas) throws IOException {

    for (JSONObject schemaInfo : schemas) {

      ProtobufSchema schema = new ProtobufSchema(schemaInfo.get("schema").toString());
      JSONArray matchingMessages = schemaInfo.getJSONArray("messagesMatched");
      for (int i = 0; i < matchingMessages.length(); i++) {
        int index = matchingMessages.getInt(i);
        serializeAndDeserializeCheck(messages.get(index), schema);
      }

    }

  }


  private void serializeAndDeserializeCheck(String message, ProtobufSchema schema)
      throws IOException {

    schema.validate();
    String formattedString = new JSONObject(message).toString();
    Object test = ProtobufSchemaUtils.toObject(formattedString, schema);
    DynamicMessage dynamicMessage = (DynamicMessage) test;

    byte[] bytes = protobufSerializer.serialize(topic, dynamicMessage);
    assertEquals(test.toString(), protobufDeserializer.deserialize(topic, bytes).toString());

    /*
    Also note that if a scalar message field is set to its default, the value will not be serialized on the wire.
    https://developers.google.com/protocol-buffers/docs/proto3#default
    In below example if Booleans is set to false, it won't show up in the serialized output
    Long, absent fields and default fields cause issues.
   */

    /*
    String jsonString = JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace().print(dynamicMessage);

    if (!ignoreLong) {
      JsonElement jsonElement1 = JsonParser.parseString(message);
      JsonElement jsonElement2 = JsonParser.parseString(jsonString);
      assertEquals(jsonElement1, jsonElement2);
    }
     */

  }


  @Test
  public void testPrimitiveTypes() throws IOException {

    String message =
        "{\n"
            + "    \"String\": \"John Smith\",\n"
            + "    \"LongName\": 12020210210567,\n"
            + "    \"Integer\": 12,\n"
            + "    \"Booleans\": true,\n"
            + "    \"Float\": 1e16,\n"
            + "    \"Double\": 53443343443.453,\n"
            + "    \"Null\": null"
            + "  }";

    ProtobufSchema protobufSchema = strictGenerator.getSchema(message);
    Object result = ProtobufSchemaUtils.toObject(message, protobufSchema);

    assertTrue(result instanceof DynamicMessage);
    DynamicMessage resultRecord = (DynamicMessage) result;

    Descriptor desc = resultRecord.getDescriptorForType();

    FieldDescriptor fd = desc.findFieldByName("String");
    assertEquals("John Smith", resultRecord.getField(fd));

    fd = desc.findFieldByName("LongName");
    assertEquals(12020210210567L, resultRecord.getField(fd));

    fd = desc.findFieldByName("Integer");
    assertEquals(12, resultRecord.getField(fd));

    fd = desc.findFieldByName("Booleans");
    assertEquals(true, resultRecord.getField(fd));

    fd = desc.findFieldByName("Float");
    assertEquals(1e16, resultRecord.getField(fd));

    fd = desc.findFieldByName("Double");
    assertEquals(5.3443343443453E10, resultRecord.getField(fd));

    fd = desc.findFieldByName("Null");
    assert (resultRecord.getField(fd).toString().equals(""));

    String expectedSchema = "syntax = \"proto3\";\n" +
        "\n" +
        "import \"google/protobuf/any.proto\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  bool Booleans = 1;\n" +
        "  double Double = 2;\n" +
        "  double Float = 3;\n" +
        "  int32 Integer = 4;\n" +
        "  int64 LongName = 5;\n" +
        "  google.protobuf.Any Null = 6;\n" +
        "  string String = 7;\n" +
        "}\n";

    assertEquals(expectedSchema, protobufSchema.toString());
    serializeAndDeserializeCheck(message, protobufSchema);
  }

  @Test
  public void testPrimitiveTypesLimit()
      throws IOException {

    /*
      If number not found in range of int64/long, throws error
     */

    String message = "{\n"
        + "    \"BigDataTypes\": 1202021022234434333333333312020210222344343333333333120202102\n"
        + "  }";

    assertThrows(IllegalArgumentException.class, () -> strictGenerator.getSchema(message));

    /*
      If number not found in range of double, mapped as double with loss in precision
     */

    String message2 = "{\n" + "    \"Double\": 6243332.789012332245\n" + "  }";

    ProtobufSchema protobufSchema = strictGenerator.getSchema(message2);
    Object result = ProtobufSchemaUtils.toObject(message2, protobufSchema);
    assertTrue(result instanceof DynamicMessage);
    DynamicMessage resultRecord = (DynamicMessage) result;
    Descriptor desc = resultRecord.getDescriptorForType();
    FieldDescriptor fd = desc.findFieldByName("Double");
    assertEquals(6243332.789012332, resultRecord.getField(fd));

  }

  @Test
  public void testComplexTypesWithPrimitiveValues() throws IOException {

    String message =
        "{\n"
            + "\"ArrayEmpty\": [],\n"
            + "\"ArrayString\": [\"John Smith\", \"Tom Davies\"],\n"
            + "    \"ArrayInteger\": [12, 13, 14],\n" + "    \"ArrayBoolean\": [false, true, false],\n"
            + "    \"DoubleRecord\": {\"Double1\": 62.4122121, \"Double2\": 62.4122121},\n"
            + "    \"IntRecord\": {\"Int1\": 62, \"Int2\": 12},\n"
            + "    \"MixedRecord\": {\"Int1\": 62, \"Double1\": 1.2212, \"name\" : \"Just Testing\"}\n"
            + "  }";

    ProtobufSchema protobufSchema = strictGenerator.getSchema(message);
    serializeAndDeserializeCheck(message, protobufSchema);

    String expectedSchema = "syntax = \"proto3\";\n" +
        "\n" +
        "import \"google/protobuf/any.proto\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated bool ArrayBoolean = 1;\n" +
        "  repeated google.protobuf.Any ArrayEmpty = 2;\n" +
        "  repeated int32 ArrayInteger = 3;\n" +
        "  repeated string ArrayString = 4;\n" +
        "  DoubleRecordMessage DoubleRecord = 5;\n" +
        "  IntRecordMessage IntRecord = 6;\n" +
        "  MixedRecordMessage MixedRecord = 7;\n" +
        "\n" +
        "  message DoubleRecordMessage {\n" +
        "    double Double1 = 1;\n" +
        "    double Double2 = 2;\n" +
        "  }\n" +
        "  message IntRecordMessage {\n" +
        "    int32 Int1 = 1;\n" +
        "    int32 Int2 = 2;\n" +
        "  }\n" +
        "  message MixedRecordMessage {\n" +
        "    double Double1 = 1;\n" +
        "    int32 Int1 = 2;\n" +
        "    string name = 3;\n" +
        "  }\n" +
        "}\n";

    assertEquals(expectedSchema, protobufSchema.toString());

    /*
    Default value are not serialized by protoBuf
     */

    String message2 =
        "{\n"
            + "    \"IntRecord\": {\"Int1\": 62, \"Int2\": null}\n"
            + "  }";

    ProtobufSchema protobufSchema2 = strictGenerator.getSchema(message2);
    serializeAndDeserializeCheck(message2, protobufSchema2);
    String expectedSchema2 = "syntax = \"proto3\";\n" +
        "\n" +
        "import \"google/protobuf/any.proto\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  IntRecordMessage IntRecord = 1;\n" +
        "\n" +
        "  message IntRecordMessage {\n" +
        "    int32 Int1 = 1;\n" +
        "    google.protobuf.Any Int2 = 2;\n" +
        "  }\n" +
        "}\n";

    assertEquals(expectedSchema2, protobufSchema2.toString());

  }

  @Test
  public void testComplexTypesRecursive() throws IOException {

    /*
    Different combinations of map and array are tested
   */

    String message = "{\n"
        + "    \"ArrayOfRecords\": [{\"Int1\": 62323232, \"Int2\": 12}, {\"Int1\": 6232323, \"Int2\": 2.5}],\n"
        + "    \"RecordOfArrays\": {\"ArrayInt1\": [12, 13,14], \"ArrayBoolean1\": [true, false]},\n"
        + "    \"RecordOfRecords\": {\"Record1\": {\"name\": {\"Tom\":1}, \"place\": \"Bom\"}, \"Record2\": {\"thing\": \"Kom\", \"place\": \"Bom\"}},\n"
        + "    \"RecordOfArrays1\": {\"key\": {\"keys\":3}},\n"
        + "    \"RecordOfArrays2\": {\"key\": {\"keys\":3}},\n"
        + "    \"RecordOfArrays3\": {\"key\": [{\"ArrayInt1\": [true, false]},{\"ArrayInt1\": [false]}],"
        + "                          \"keys\": [{\"Hello\": [12, 13]}, {\"Hello\": [-2, 5]}]}\n"
        + "  }";

    ProtobufSchema protobufSchema = strictGenerator.getSchema(message);
    serializeAndDeserializeCheck(message, protobufSchema);

    String expectedSchema = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated ArrayOfRecordsMessage ArrayOfRecords = 1;\n" +
        "  RecordOfArraysMessage RecordOfArrays = 2;\n" +
        "  RecordOfArrays1Message RecordOfArrays1 = 3;\n" +
        "  RecordOfArrays2Message RecordOfArrays2 = 4;\n" +
        "  RecordOfArrays3Message RecordOfArrays3 = 5;\n" +
        "  RecordOfRecordsMessage RecordOfRecords = 6;\n" +
        "\n" +
        "  message ArrayOfRecordsMessage {\n" +
        "    int32 Int1 = 1;\n" +
        "    double Int2 = 2;\n" +
        "  }\n" +
        "  message RecordOfArraysMessage {\n" +
        "    repeated bool ArrayBoolean1 = 1;\n" +
        "    repeated int32 ArrayInt1 = 2;\n" +
        "  }\n" +
        "  message RecordOfArrays1Message {\n" +
        "    keyMessage key = 1;\n" +
        "  \n" +
        "    message keyMessage {\n" +
        "      int32 keys = 1;\n" +
        "    }\n" +
        "  }\n" +
        "  message RecordOfArrays2Message {\n" +
        "    keyMessage key = 1;\n" +
        "  \n" +
        "    message keyMessage {\n" +
        "      int32 keys = 1;\n" +
        "    }\n" +
        "  }\n" +
        "  message RecordOfArrays3Message {\n" +
        "    repeated keyMessage key = 1;\n" +
        "    repeated keysMessage keys = 2;\n" +
        "  \n" +
        "    message keyMessage {\n" +
        "      repeated bool ArrayInt1 = 1;\n" +
        "    }\n" +
        "    message keysMessage {\n" +
        "      repeated int32 Hello = 2;\n" +
        "    }\n" +
        "  }\n" +
        "  message RecordOfRecordsMessage {\n" +
        "    Record1Message Record1 = 1;\n" +
        "    Record2Message Record2 = 2;\n" +
        "  \n" +
        "    message Record1Message {\n" +
        "      nameMessage name = 1;\n" +
        "      string place = 2;\n" +
        "    \n" +
        "      message nameMessage {\n" +
        "        int32 Tom = 1;\n" +
        "      }\n" +
        "    }\n" +
        "    message Record2Message {\n" +
        "      string place = 1;\n" +
        "      string thing = 2;\n" +
        "    }\n" +
        "  }\n" +
        "}\n";

    assertEquals(protobufSchema.toString(), expectedSchema);
  }


  @Test
  public void testArrayOfArrays() {

    /*
      Array of arrays, ie 2d arrays aren't allowed in protobuf
      Raising error if found
     */

    String message = "{\n" + "    \"Array2d\": [[1,2], [2,3]],\n" + "  }";

    assertThrows(IllegalArgumentException.class, () -> strictGenerator.getSchema(message));

    String message2 = "{\n"
        + "    \"RecordOfArrays3\": { \"Array2D\": [ [{\"name\": \"J\"},{\"name\": \"K\"}], [{\"name\": \"T\"}] ]},\n"
        + "  }";

    assertThrows(IllegalArgumentException.class, () -> strictGenerator.getSchema(message2));

    String message3 = "{\n" + "    \"ArrayNull\": [null, null],\n" + "  }";
    assertThrows(IllegalArgumentException.class, () -> strictGenerator.getSchema(message3));

  }

  @Test
  public void testArrayDifferentTypes()
      throws IOException {

      /*
      Array has element 1 of type integer and second float
      Elements are of different type in array are allowed in protobuf
     */

    String message = "{\n" + "    \"arr\": [13.1, 24]\n" + "  }";
    ProtobufSchema protobufSchema = strictGenerator.getSchema(message);
    serializeAndDeserializeCheck(message, protobufSchema);

    String expectedSchema = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated double arr = 1;\n" +
        "}\n";

    assertEquals(expectedSchema, protobufSchema.toString());

    /*
      Record1 has parameters - Int1 and Int2
      Record2 has parameters - Int1 and Int3
      Both Records are of different type are allowed in protobuf
     */
    String message2 = "{\n"
        + "    \"ArrayOfRecords\": [{\"Int1\": 62, \"Int2\": 12}, {\"Int1\": 1, \"Int3\": 2}]\n"
        + "  }";

    ProtobufSchema protobufSchema2 = strictGenerator.getSchema(message2);
    serializeAndDeserializeCheck(message, protobufSchema);
    String expectedSchema2 = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated ArrayOfRecordsMessage ArrayOfRecords = 1;\n" +
        "\n" +
        "  message ArrayOfRecordsMessage {\n" +
        "    int32 Int1 = 1;\n" +
        "    int32 Int2 = 2;\n" +
        "    int32 Int3 = 3;\n" +
        "  }\n" +
        "}\n";
    assertEquals(expectedSchema2, protobufSchema2.toString());


    String message3 = "{\n"
        + "    \"ArrayOfRecords\": [{\"Int1\": 62, \"Int2\": 12}, {\"Int1\": 1, \"Int3\": [2,5]}, {\"Int1\": 1, \"Int3\": [2,132]}]\n"
        + "  }";

    ProtobufSchema protobufSchema3 = strictGenerator.getSchema(message3);
    serializeAndDeserializeCheck(message, protobufSchema);
    String expectedSchema3 = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated ArrayOfRecordsMessage ArrayOfRecords = 1;\n" +
        "\n" +
        "  message ArrayOfRecordsMessage {\n" +
        "    int32 Int1 = 1;\n" +
        "    int32 Int2 = 2;\n" +
        "    repeated int32 Int3 = 3;\n" +
        "  }\n" +
        "}\n";
    assertEquals(protobufSchema3.toString(), expectedSchema3);

    /*
    Int1 has different types int and bool, not allowed
    Raises Error
    */

    String message4 = "{\n"
        + "    \"ArrayOfRecords\": [{\"Int1\": 62, \"Int2\": 12}, {\"Int1\": false, \"Int3\": 2, \"Int4\": 2}]\n"
        + "  }";

    assertThrows(IllegalArgumentException.class, () -> strictGenerator.getSchema(message4));

    ProtobufSchema protobufSchema42 = lenientGenerator.getSchema(message4);
    String expectedSchema42 = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated ArrayOfRecordsMessage ArrayOfRecords = 1;\n" +
        "\n" +
        "  message ArrayOfRecordsMessage {\n" +
        "    int32 Int1 = 1;\n" +
        "    int32 Int2 = 2;\n" +
        "    int32 Int3 = 3;\n" +
        "    int32 Int4 = 4;\n" +
        "  }\n" +
        "}\n";
    assertEquals(protobufSchema42.toString(), expectedSchema42);

    /*
    Array has both records and int, not allowed strict check
    Raises Error
    */
    String message5 = "{\n"
        + "    \"ArrayOfRecords\": [{\"Int1\": 62, \"Int2\": 12}, {\"Int1\": 12}, 12]\n"
        + "  }";

    assertThrows(IllegalArgumentException.class, () -> strictGenerator.getSchema(message5));
    ProtobufSchema protobufSchema52 = lenientGenerator.getSchema(message5);
    String expectedSchema52 = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated ArrayOfRecordsMessage ArrayOfRecords = 1;\n" +
        "\n" +
        "  message ArrayOfRecordsMessage {\n" +
        "    int32 Int1 = 1;\n" +
        "    int32 Int2 = 2;\n" +
        "  }\n" +
        "}\n";
    assertEquals(expectedSchema52, protobufSchema52.toString());


    String message6 =
        "{\n"
            + "\"ArrayOfBoolean\": [0, 1, true, true, true],\n"
            + "\"ArrayOfInts\": [0, \"Java\", 10, 100, -12, 11221],\n"
            + "\"ArrayOfStrings\": [\"Java\", 10, 100, \"C++\", \"Scala\"]\n"
            + "  }";

    ProtobufSchema protobufSchema6 = lenientGenerator.getSchema(message6);
    String expectedSchema6 = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated bool ArrayOfBoolean = 1;\n" +
        "  repeated int32 ArrayOfInts = 2;\n" +
        "  repeated string ArrayOfStrings = 3;\n" +
        "}\n";
    assertEquals(expectedSchema6, protobufSchema6.toString());

    String message7 =
        "{\n"
            + "\"ArrayOfRecords1\": [{\"J\": true}, {\"J\": false}, {\"J\": 1}, {\"J\": false}],\n"
            + "\"ArrayOfRecords2\": [{\"Int1\": 10.1, \"Int2\":1.2}, {\"Int1\": -0.5, \"Int2\":1}, {\"Int1\": true, \"Int3\":1}],\n"
            + "\"ArrayOfRecords3\": [{\"Int1\": true, \"Int2\":false}, {\"Int1\": -0.5, \"Int2\":1}, {\"Int1\": 0.1, \"Int2\": -10}]\n"
            + "  }";

    ProtobufSchema protobufSchema7 = lenientGenerator.getSchema(message7);
    String expected7 = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated ArrayOfRecords1Message ArrayOfRecords1 = 1;\n" +
        "  repeated ArrayOfRecords2Message ArrayOfRecords2 = 2;\n" +
        "  repeated ArrayOfRecords3Message ArrayOfRecords3 = 3;\n" +
        "\n" +
        "  message ArrayOfRecords1Message {\n" +
        "    bool J = 1;\n" +
        "  }\n" +
        "  message ArrayOfRecords2Message {\n" +
        "    double Int1 = 2;\n" +
        "    double Int2 = 3;\n" +
        "    int32 Int3 = 4;\n" +
        "  }\n" +
        "  message ArrayOfRecords3Message {\n" +
        "    double Int1 = 3;\n" +
        "    int32 Int2 = 4;\n" +
        "  }\n" +
        "}\n";

    assertEquals(expected7, protobufSchema7.toString());


    /*
    Array of records with each element has one field and that field is of type record
    Checking if merging of records is happening recursively
     */

    String message8 = "{\n"
        + "    \"ArrayOfRecords\": [ {\"Int1\": {\"age\":12}}, {\"Int1\": {\"name\":\"S\"}}, {\"Int1\": {\"age\":12}}]\n"
        + "  }";

    ProtobufSchema protobufSchema8 = strictGenerator.getSchema(message8);
    serializeAndDeserializeCheck(message, protobufSchema);
    String expectedSchema8 = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated ArrayOfRecordsMessage ArrayOfRecords = 1;\n" +
        "\n" +
        "  message ArrayOfRecordsMessage {\n" +
        "    Int1Message Int1 = 1;\n" +
        "  \n" +
        "    message Int1Message {\n" +
        "      int32 age = 1;\n" +
        "      string name = 2;\n" +
        "    }\n" +
        "  }\n" +
        "}\n";
    assertEquals(protobufSchema8.toString(), expectedSchema8);

  }

  @Test
  public void testRecursive() throws JsonProcessingException {

    String message1 =
        "{\n"
            + "\"ArrayOfRecords1\": [{\"J\": [10,11,true]}, {\"J\": [10, \"first\", \"second\"]}, {\"J\": [\"first\", \"first\", 11]}]}\n"
            + "  }";

    ProtobufSchema protobufSchema1 = lenientGenerator.getSchema(message1);
    String expectedSchema1 = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated ArrayOfRecords1Message ArrayOfRecords1 = 1;\n" +
        "\n" +
        "  message ArrayOfRecords1Message {\n" +
        "    repeated string J = 1;\n" +
        "  }\n" +
        "}\n";
    assertEquals(expectedSchema1, protobufSchema1.toString());

    String message2 =
        "{\n"
            + "\"ArrayOfRecords1\": [{\"K\": [10,11,true]}, {\"J\": [10, \"first\", \"second\"]}, {\"J\": [\"first\", \"first\", 11]}]}\n"
            + "  }";

    ProtobufSchema protobufSchema2 = lenientGenerator.getSchema(message2);
    String expectedSchema2 = "syntax = \"proto3\";\n" +
        "\n" +
        "message mainMessage {\n" +
        "  repeated ArrayOfRecords1Message ArrayOfRecords1 = 1;\n" +
        "\n" +
        "  message ArrayOfRecords1Message {\n" +
        "    repeated string J = 1;\n" +
        "    repeated int32 K = 2;\n" +
        "  }\n" +
        "}\n";

    assertEquals(expectedSchema2, protobufSchema2.toString());
  }


  @Test
  public void testMultipleMessages() throws IOException {

    /*
      Optional Field Check, Long and Bool2 not present in all messages
     */
    ArrayList<String> arr = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      String message = String.format("{\n"
          + "    \"String\": \"%d\",\n"
          + "    \"Integer\": %d,\n"
          + "    \"Boolean\": %b,\n", i * 100, i, i % 2 == 0);

      if (i % 3 == 0) {
        String message2 = String.format("\"Long\": %d", i * 121202212);
        arr.add(message + message2 + "}");
      } else if (i % 3 == 2) {
        String message2 = String.format("\"Bool2\": %b", false);
        arr.add(message + message2 + "}");
      } else {
        arr.add(message + "}");
      }
    }

    /*
    Message 2 and 3 are in conflict, 3 can be merged with 1 and 3 can be merged with 2
    Best matching schema would be 3 merged with 1 having 3 occurrences
     */

    List<JSONObject> schemas = strictGenerator.getSchemaForMultipleMessages(arr);
    assert (schemas.size() == 1);

    String schemasExpected1 = "{\"schema\":\"syntax = \\\"proto3\\\";\\n\\nmessage Record {\\n  bool Bool2 = 1;\\n  bool Boolean = 2;\\n  int32 Integer = 3;\\n  int32 Long = 4;\\n  string String = 5;\\n}\\n\",\"messagesMatched\":[0,1,2,3,4,5,6],\"numMessagesMatched\":7}";
    assertEquals(schemasExpected1, schemas.get(0).toString());

    serializeAndDeserializeCheckMulti(arr, schemas);


    ArrayList<String> arr2 = new ArrayList<>();
    String message1 = "{\n"
        + "    \"String\": \"John Smith\",\n"
        + "  }";
    arr2.add(message1);

    String message2 = "{\n"
        + "    \"String\": \"John\",\n"
        + "    \"Float\": 1e16,\n"
        + "  }";
    arr2.add(message2);

    String message3 = "{\n"
        + "    \"String\": \"John\",\n"
        + "    \"Float\": \"JJ\",\n"
        + "  }";

    arr2.add(message3);
    arr2.add(message3);
    arr2.add(message3);

    List<JSONObject> schemas2 = strictGenerator.getSchemaForMultipleMessages(arr2);
    assert (schemas2.size() == 2);

    String schemas2Expected1 = "{\"schema\":\"syntax = \\\"proto3\\\";\\n\\nmessage Record {\\n  string Float = 1;\\n  string String = 2;\\n}\\n\",\"messagesMatched\":[0,2,3,4],\"numMessagesMatched\":4}";
    String schemas2Expected2 = "{\"schema\":\"syntax = \\\"proto3\\\";\\n\\nmessage Record {\\n  double Float = 1;\\n  string String = 2;\\n}\\n\",\"messagesMatched\":[0,1],\"numMessagesMatched\":2}";

    assertEquals(schemas2Expected1, schemas2.get(0).toString());
    assertEquals(schemas2Expected2, schemas2.get(1).toString());
    serializeAndDeserializeCheckMulti(arr2, schemas2);

    String message4 = "{\n"
        + "    \"String\": \"John\",\n"
        + "    \"Float\": \"JJ\",\n"
        + "    \"Num\": 100,\n"
        + "  }";

    String message5 = "{\n"
        + "    \"String\": \"John\",\n"
        + "    \"Float\": 33,\n"
        + "    \"Num\": true,\n"
        + "  }";

    arr2.add(message4);
    arr2.add(message5);

    List<JSONObject> schemas3 = strictGenerator.getSchemaForMultipleMessages(arr2);
    assert (schemas3.size() == 2);

    String schemas3Expected1 = "{\"schema\":\"syntax = \\\"proto3\\\";\\n\\nmessage Record {\\n  string Float = 1;\\n  int32 Num = 2;\\n  string String = 3;\\n}\\n\",\"messagesMatched\":[0,2,3,4,5],\"numMessagesMatched\":5}";
    String schemas3Expected2 = "{\"schema\":\"syntax = \\\"proto3\\\";\\n\\nmessage Record {\\n  double Float = 1;\\n  bool Num = 2;\\n  string String = 3;\\n}\\n\",\"messagesMatched\":[0,1,6],\"numMessagesMatched\":3}";

    assertEquals(schemas3Expected1, schemas3.get(0).toString());
    assertEquals(schemas3Expected2, schemas3.get(1).toString());
    serializeAndDeserializeCheckMulti(arr2, schemas3);

    List<JSONObject> schemas4 = lenientGenerator.getSchemaForMultipleMessages(arr2);
    assert (schemas4.size() == 1);

    String schemas4Expected1 = "{\"schema\":\"syntax = \\\"proto3\\\";\\n\\nmessage Record {\\n  string Float = 1;\\n  int32 Num = 2;\\n  string String = 3;\\n}\\n\"}";
    assertEquals(schemas4Expected1, schemas4.get(0).toString());

  }


  @Test
  public void testMultipleMessagesErrors() {

    /*
    Strict schema cannot be found for message1 because of field Arr having multiple data types
    Hence, message1 is ignored in multiple messages
    */

    ArrayList<String> arr = new ArrayList<>();
    String message1 = "{\n"
        + "    \"String\": \"John Smith\","
        + "    \"Arr\": [1.5, true]"
        + "  }";
    arr.add(message1);

    assertThrows(IllegalArgumentException.class, () -> strictGenerator.getSchemaForMultipleMessages(arr));

    /*
    Record with naming errors
    */
    String message3 =
        "[{\"\": {\"K\":12}},{\"M\": {\"J\":[null, null]}}, {\"K.L\":[1, 2]}]";

    List<String> messages = ReadFileUtils.readMessagesToString(message3);
    assertThrows(IllegalArgumentException.class, () ->
        strictGenerator.getSchemaForMultipleMessages(messages));

  }

  @Test
  public void testRecursiveMerging() throws IOException {

    /*
    Test to make sure merging of records is happening recursively for both strict and lenient
     */
    String m1 = "{\"J\" : {\"K\" : 12, \"A\":12}}";
    String m2 = "{\"J\" : {\"K\": 1232, \"C\":13}}";
    String m3 = "{\"J\" : {\"K\": 1.2, \"B\":true}}";

    ArrayList<String> messages = new ArrayList<>(Arrays.asList(m1, m2, m3));
    List<JSONObject> schemas = strictGenerator.getSchemaForMultipleMessages(messages);
    assert (schemas.size() == 1);
    serializeAndDeserializeCheckMulti(messages, schemas);

    String schemasExpected1 = "{\"schema\":\"syntax = \\\"proto3\\\";\\n\\nmessage Record {\\n  JMessage J = 1;\\n\\n  message JMessage {\\n    int32 A = 1;\\n    bool B = 2;\\n    int32 C = 3;\\n    double K = 4;\\n  }\\n}\\n\",\"messagesMatched\":[0,1,2],\"numMessagesMatched\":3}";
    assertEquals(schemasExpected1, schemas.get(0).toString());

    List<JSONObject> schemas1 = lenientGenerator.getSchemaForMultipleMessages(messages);
    assert (schemas1.size() == 1);

    String schemas1Expected1 = "{\"schema\":\"syntax = \\\"proto3\\\";\\n\\nmessage Record {\\n  JMessage J = 1;\\n\\n  message JMessage {\\n    int32 A = 1;\\n    bool B = 2;\\n    int32 C = 3;\\n    double K = 4;\\n  }\\n}\\n\"}";
    assertEquals(schemas1Expected1, schemas1.get(0).toString());

    /*
    Most occurring type is chosen for K inside Record
     */

    String m4 = "{\"J\" : {\"K\": true, \"B\":true}}";
    messages.add(m4);

    List<JSONObject> schemas2 = lenientGenerator.getSchemaForMultipleMessages(messages);
    assert (schemas2.size() == 1);

    String schemas2Expected1 = "{\"schema\":\"syntax = \\\"proto3\\\";\\n\\nmessage Record {\\n  JMessage J = 1;\\n\\n  message JMessage {\\n    int32 A = 1;\\n    bool B = 2;\\n    int32 C = 3;\\n    double K = 4;\\n  }\\n}\\n\"}";
    assertEquals(schemas2Expected1, schemas2.get(0).toString());

  }

}