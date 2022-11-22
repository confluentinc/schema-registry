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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DeriveProtoBufSchemaTest extends DeriveSchemaTest {

  private final KafkaProtobufSerializer<DynamicMessage> protobufSerializer;
  private final KafkaProtobufDeserializer<Message> protobufDeserializer;

  public DeriveProtoBufSchemaTest() {
    derive = new DeriveProtobufSchema();
    Properties serializerConfig = new Properties();
    serializerConfig.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    protobufSerializer = new KafkaProtobufSerializer<DynamicMessage>(schemaRegistry, new HashMap(serializerConfig));
    protobufDeserializer = new KafkaProtobufDeserializer<>(schemaRegistry);
  }

  protected void matchAndValidate(String message, JsonNode schemaString, String expectedSchema)
      throws IOException {
    ProtobufSchema schema = new ProtobufSchema(schemaString.asText());
    schema.validate();
    assertEquals(schema.toString(), expectedSchema);
    String formattedString = mapper.readTree(message).toString();
    Object protobufObject = ProtobufSchemaUtils.toObject(formattedString, schema);
    DynamicMessage dynamicMessage = (DynamicMessage) protobufObject;
    assertEquals(protobufObject, protobufDeserializer.deserialize("test", protobufSerializer.serialize("test", dynamicMessage)));
  }

  @Test
  public void testDerivePrimitive() throws JsonProcessingException {
    // Match all primitive types with expected schema
    generateSchemaAndCheckPrimitive("12", "{\"type\":\"int32\"}");
    generateSchemaAndCheckPrimitive("12232323322323", "{\"type\":\"int64\"}");
    generateSchemaAndCheckPrimitive("12.5", "{\"type\":\"double\"}");
    generateSchemaAndCheckPrimitive("12020210222344343333333333120202102223443", "{\"type\":\"double\"}");
    generateSchemaAndCheckPrimitive("true", "{\"type\":\"bool\"}");
    generateSchemaAndCheckPrimitive("\"Test\"", "{\"type\":\"string\"}");
    generateSchemaAndCheckPrimitive("", "{\"type\":\"google.protobuf.Any\"}");
    generateSchemaAndCheckPrimitive("null", "{\"type\":\"google.protobuf.Any\"}");
  }

  @Test
  public void testDerivePrimitiveForComplex() throws JsonProcessingException {
    // Checking all complex types, should be empty option
    generateSchemaAndCheckPrimitiveAbsent("[12]");
    generateSchemaAndCheckPrimitiveAbsent("{\"F1\":12}");
  }

  @Test
  public void testConvertToFormatArray() throws JsonProcessingException {
    DeriveProtobufSchema deriveProtobuf = (DeriveProtobufSchema) derive;
    String arraySchema = deriveProtobuf.convertToFormatArray(mapper.readTree(ARRAY_OF_NUMBERS), "array", 1);
    assertEquals(arraySchema, "repeated number array = 1;\n");
  }

  @Test
  public void testConvertToFormatRecord() throws JsonProcessingException {
    DeriveProtobufSchema deriveProtobuf = (DeriveProtobufSchema) derive;
    String recordSchema = deriveProtobuf.convertToFormatRecord(mapper.readTree(RECORD_WITH_ARRAY_OF_STRINGS), "Test");
    assertEquals(recordSchema, "message Test { \n" +
        "repeated string F1 = 1;\n" +
        "}\n");
  }

  @Test
  public void testConvertToFormat() throws JsonProcessingException {
    DeriveProtobufSchema deriveProtobuf = (DeriveProtobufSchema) derive;
    JsonNode recordSchema = deriveProtobuf.convertToFormat(mapper.readTree(RECORD_WITH_ARRAY_OF_STRINGS), "Test");
    assertEquals(recordSchema.asText(), "syntax = \"proto3\";\n" + "\n" +
        "message Test {\n" +
        "  repeated string F1 = 1;\n" +
        "}\n");
  }

  @Test
  public void testDeriveRecordPrimitive() throws Exception {
    // Get schema for record with fields having only primitive data types
    String longMessage = "\"Long\": 1202021212121009";
    String boolMessage = "\"Bool\": true";
    String primitiveTypesMessage = "{" + longMessage + "," + boolMessage + "}";
    String expectedSchema = "syntax = \"proto3\";\n" + "\n" +
        "message Schema {\n" +
        "  bool Bool = 1;\n" +
        "  int64 Long = 2;\n" +
        "}\n";
    generateSchemaAndCheckExpected(primitiveTypesMessage, expectedSchema);
  }

  @Test
  public void testDeriveMergeArrays() throws Exception {
    // Check merging of int and double type
    String intMessage = "{\"type\":\"array\",\"items\":{\"type\":\"int32\"}}";
    String doubleMessage = "{\"type\":\"array\",\"items\":{\"type\":\"double\"}}";
    ObjectNode mergedArray = derive.mergeArrays(Arrays.asList(mapper.readTree(intMessage), mapper.readTree(doubleMessage)), true, true);
    assertEquals(mergedArray.toString(), "{\"type\":\"array\",\"items\":{\"type\":\"double\"}}");

    // nested arrays should raise error
    assertThrows(IllegalArgumentException.class, () -> derive.mergeArrays(Collections.singletonList(mapper.readTree(ARRAY_OF_ARRAY_OF_NUMBERS)), false, true));
  }

  @Test
  public void testDeriveRecordComplexTypesWithPrimitiveValues() throws IOException {
    // Get schema for record with arrays and records having only primitive data types
    String emptyArrayMessage = "\"emptyArray\":[]";
    String recordOfMultipleMessage = "\"MixedRecord\": {\"Double1\": 1.221, \"name\" : \"T\"}";
    String complexTypesWithPrimitiveValues = "{" + recordOfMultipleMessage + "," + emptyArrayMessage + "}";
    String expectedSchema = "syntax = \"proto3\";\n" + "\n" +
        "import \"google/protobuf/any.proto\";\n" + "\n" +
        "message Schema {\n" +
        "  MixedRecordMessage MixedRecord = 1;\n" +
        "  repeated google.protobuf.Any emptyArray = 2;\n" + "\n" +
        "  message MixedRecordMessage {\n" +
        "    double Double1 = 1;\n" +
        "    string name = 2;\n" +
        "  }\n" +
        "}\n";
    generateSchemaAndCheckExpected(complexTypesWithPrimitiveValues, expectedSchema);
  }

  @Test
  public void testDeriveRecordFailure() {
    // multiple data types is not allowed in protobuf, should throw error
    String arrayOfMultipleTypes = "{\"Record\": [1, true]}";
    assertThrows(IllegalArgumentException.class, () -> generateSchemaAndCheckExpected(arrayOfMultipleTypes, ""));

    // recursive merging of field N with different types should also throw error
    String recordOfRecords = "{\"RecordOfRecords\": [{\"R1\": {\"N\": 1}}, {\"R1\": {\"N\": \"B\"}}]}";
    assertThrows(IllegalArgumentException.class, () -> generateSchemaAndCheckExpected(recordOfRecords, ""));

    // array of nulls should raise error
    String arrayOfNulls = "{\"RecordOfRecords\": [null]}";
    assertThrows(IllegalArgumentException.class, () -> generateSchemaAndCheckExpected(arrayOfNulls, ""));
  }

  @Test
  public void testCheckName() {
    // empty name should throw error
    assertThrows(IllegalArgumentException.class, () -> derive.checkName(""));
    // name starting with digit should also throw error
    assertThrows(IllegalArgumentException.class, () -> derive.checkName("1"));
    // name starting with digit should also throw error
    assertThrows(IllegalArgumentException.class, () -> derive.checkName("^"));
  }

  @Test
  public void testRecursiveMergingOfNumberTypes() throws IOException {
    // recursive merging of field N with number types should happen
    String recordOfRecords = "{\"RecordOfNum\": [{\"R1\": {\"N\": 1.5}}, {\"R1\": {\"N\": 1}}]}";
    generateSchemaAndCheckExpected(recordOfRecords, "syntax = \"proto3\";\n" + "\n" +
        "message Schema {\n" +
        "  repeated RecordOfNumMessage RecordOfNum = 1;\n" + "\n" +
        "  message RecordOfNumMessage {\n" +
        "    R1Message R1 = 1;\n" + "  \n" +
        "    message R1Message {\n" +
        "      double N = 1;\n" +
        "    }\n" + "  }\n"
        + "}\n");
  }

  @Test
  public void testRecursiveMergingOfNumberTypesInsideArray() throws IOException {
    // Field FF1 should be interpreted as type long
    JsonNode messageWithArrayOfRecordInt = mapper.readTree("{\"F1\": [{\"FF1\":1}]}");
    JsonNode messageWithArrayOfRecordLong = mapper.readTree("{\"F1\": [{\"FF1\":112211221122121}]}");
    JsonNode schema = derive.getSchemaForMultipleMessages(Arrays.asList(messageWithArrayOfRecordInt, messageWithArrayOfRecordLong)).get("schemas");
    String expectedSchema1 = "syntax = \"proto3\";\n" + "\n" +
        "message Schema {\n" +
        "  repeated F1Message F1 = 1;\n" + "\n" +
        "  message F1Message {\n" +
        "    int64 FF1 = 1;\n" +
        "  }\n" +
        "}\n";
    assertEquals(schema.get(0).get("schema").asText(), expectedSchema1);
    assertEquals(schema.get(0).get("messagesMatched").toString(), "[0,1]");
  }

  @Test
  public void testDeriveMultipleMessages() throws JsonProcessingException {
    // Message1,4 and Message2 cannot be merged due to conflicting types of F1
    // Message3 can merge with both, hence we have 2 different schema generated
    JsonNode message1 = mapper.readTree("{\"F1\": 1.5, \"F2\": true}");
    JsonNode message2 = mapper.readTree("{\"F1\": 1, \"F2\": 1}");
    JsonNode message3 = mapper.readTree("{\"F3\": [1, 1.5, 3]}");
    JsonNode message4 = mapper.readTree("{\"F1\": 1.5 , \"F2\": true}");
    JsonNode schema = derive.getSchemaForMultipleMessages(Arrays.asList(message4, message2, message1, message3, message2)).get("schemas");

    assertEquals(schema.size(), 2);
    String expectedSchema1 = "syntax = \"proto3\";\n\n" +
        "message Schema {\n" +
        "  double F1 = 1;\n" +
        "  bool F2 = 2;\n" +
        "  repeated double F3 = 3;\n" +
        "}\n";
    assertEquals(schema.get(0).get("schema").asText(), expectedSchema1);
    assertEquals(schema.get(0).get("messagesMatched").toString(), "[0,2,3]");

    String expectedSchema2 = "syntax = \"proto3\";\n\n" +
        "message Schema {\n" +
        "  double F1 = 1;\n" +
        "  int32 F2 = 2;\n" +
        "  repeated double F3 = 3;\n" +
        "}\n";
    assertEquals(schema.get(1).get("schema").asText(), expectedSchema2);
    assertEquals(schema.get(1).get("messagesMatched").toString(), "[1,3,4]");
  }
}