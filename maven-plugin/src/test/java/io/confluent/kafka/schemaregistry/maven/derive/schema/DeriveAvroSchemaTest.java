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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DeriveAvroSchemaTest extends DeriveSchemaTest {

  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;

  public DeriveAvroSchemaTest() {
    derive = new DeriveAvroSchema();
    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry);
  }

  protected void matchAndValidate(String message, JsonNode schemaString, String expectedSchema)
      throws IOException {
    AvroSchema schema = new AvroSchema(schemaString.toString());
    schema.validate(false);
    assertEquals(schema.toString(), expectedSchema);
    Object test = AvroSchemaUtils.toObject(message, schema);
    byte[] bytes = this.avroSerializer.serialize("test", test);
    assertEquals(test, this.avroDeserializer.deserialize("test", bytes, schema.rawSchema()));
    final ByteArrayOutputStream bs = new ByteArrayOutputStream();
    final String utf8 = StandardCharsets.UTF_8.name();
    try (PrintStream ps = new PrintStream(bs, true, utf8)) {
      AvroSchemaUtils.toJson(test, ps);
    }
    // Checking if original message and message generated after serializing and deserializing is same
    assertEquals(mapper.readTree(message), mapper.readTree(bs.toString(utf8)));
  }

  private void testUnion(List<String> messages, String expectedSchema) throws IOException {
    List<JsonNode> messageObject = new ArrayList<>();
    for (String message : messages) {
      messageObject.add(derive.getSchemaForRecord((ObjectNode) mapper.readTree(message)));
    }
    ObjectNode merged = derive.mergeRecords(messageObject);
    JsonNode schema = derive.convertToFormat(merged, "Record");
    for (String message : messages) {
      matchAndValidate(message, schema, expectedSchema);
    }
  }

  @Test
  public void testDerivePrimitive() throws JsonProcessingException {
    // Match all primitive types with expected schema
    generateSchemaAndCheckPrimitive("12", "{\"type\":\"int\"}");
    generateSchemaAndCheckPrimitive("12232323322323", "{\"type\":\"long\"}");
    generateSchemaAndCheckPrimitive("12.5", "{\"type\":\"double\"}");
    generateSchemaAndCheckPrimitive("12020210222344343333333333120202102223443", "{\"type\":\"double\"}");
    generateSchemaAndCheckPrimitive("true", "{\"type\":\"boolean\"}");
    generateSchemaAndCheckPrimitive("\"Test\"", "{\"type\":\"string\"}");
    generateSchemaAndCheckPrimitive("", "{\"type\":\"null\"}");
    generateSchemaAndCheckPrimitive("null", "{\"type\":\"null\"}");
  }

  @Test
  public void testDerivePrimitiveForComplex() throws JsonProcessingException {
    // Checking all complex types, should be empty option
    generateSchemaAndCheckPrimitiveAbsent("[12]");
    generateSchemaAndCheckPrimitiveAbsent("{\"F1\":12}");
  }

  @Test
  public void testConvertToFormatArray() throws JsonProcessingException {
    DeriveAvroSchema deriveAvro = (DeriveAvroSchema) derive;
    ObjectNode arraySchema = deriveAvro.convertToFormatArray(mapper.readTree(ARRAY_OF_NUMBERS), "array");
    assertEquals(arraySchema.toString(), "{\"type\":\"array\",\"items\":\"number\"}");
  }

  @Test
  public void testConvertToFormatRecord() throws JsonProcessingException {
    DeriveAvroSchema deriveAvro = (DeriveAvroSchema) derive;
    ObjectNode recordSchema = deriveAvro.convertToFormatForRecord(mapper.readTree(RECORD_WITH_ARRAY_OF_STRINGS), "Test");
    assertEquals(recordSchema.toString(), "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"F1\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}");
  }

  @Test
  public void testConvertToFormat() throws JsonProcessingException {
    JsonNode recordSchema = derive.convertToFormat(mapper.readTree(RECORD_WITH_ARRAY_OF_STRINGS), "Test");
    assertEquals(recordSchema.toString(), "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"F1\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}");
  }

  @Test
  public void testDeriveRecordPrimitive() throws Exception {
    // Get schema for record with fields having only primitive data types
    String longMessage = "\"Long\": 1202021212121009";
    String boolMessage = "\"Bool\": true";
    String primitiveTypesMessage = "{" + longMessage + "," + boolMessage + "}";
    String expectedSchema = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"Bool\",\"type\":\"boolean\"},{\"name\":\"Long\",\"type\":\"long\"}]}";
    generateSchemaAndCheckExpected(primitiveTypesMessage, expectedSchema);
  }

  @Test
  public void testDeriveRecordComplexTypesWithPrimitiveValues() throws IOException {
    // Get schema for record with arrays and records having only primitive data types
    String emptyArrayMessage = "\"emptyArray\":[]";
    String recordOfMultipleMessage = "\"MixedRecord\": {\"Double1\": 1.221, \"name\" : \"T\"}";
    String complexTypesWithPrimitiveValues = "{" + recordOfMultipleMessage + "," + emptyArrayMessage + "}";
    String expectedSchema = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"MixedRecord\",\"type\":{\"type\":\"record\",\"name\":\"MixedRecord\",\"fields\":[{\"name\":\"Double1\",\"type\":\"double\"},{\"name\":\"name\",\"type\":\"string\"}]}},{\"name\":\"emptyArray\",\"type\":{\"type\":\"array\",\"items\":\"null\"}}]}";
    generateSchemaAndCheckExpected(complexTypesWithPrimitiveValues, expectedSchema);
  }

  @Test
  public void testDeriveMergeArrays() throws Exception {
    // Check merging of int and double type
    String intMessage = "{\"type\":\"array\",\"items\":{\"type\":\"int\"}}";
    String doubleMessage = "{\"type\":\"array\",\"items\":{\"type\":\"double\"}}";
    ObjectNode mergedArray = derive.mergeArrays(Arrays.asList(mapper.readTree(intMessage), mapper.readTree(doubleMessage)), true, false);
    assertEquals(mergedArray.toString(), "{\"type\":\"array\",\"items\":{\"type\":\"double\"}}");
  }

  @Test
  public void testDeriveMergeArraysForRecord() throws Exception {
    // Check merging of int and double type inside field of a record
    JsonNode recordWithInteger = mapper.readTree("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"int\"}}}");
    JsonNode recordWithDouble = mapper.readTree("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"double\"}}}");
    ObjectNode mergedArray = derive.mergeArrays(Arrays.asList(recordWithDouble, recordWithInteger), false, false);
    assertEquals(mergedArray.toString(), "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"double\"}}}}");
  }

  @Test
  public void testDeriveMergeArraysForRecordInsideArray() throws Exception {
    // Check merging of int and long type inside field of a record inside an array
    JsonNode recordWithInteger = mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"int\"}}}}");
    JsonNode recordWithDouble = mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"long\"}}}}");
    ObjectNode mergedArray = derive.mergeArrays(Arrays.asList(recordWithDouble, recordWithInteger), false, false);
    assertEquals(mergedArray.toString(), "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"long\"}}}}}");
  }

  @Test
  public void testDeriveMergeArraysFailure() throws Exception {
    // Two different records cannot be merged due to extra field 'F2'
    JsonNode recordWithInteger = mapper.readTree("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"int\"}, \"F2\":{\"type\":\"int\"}}}");
    JsonNode recordWithDouble = mapper.readTree("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"double\"}}}");
    assertThrows(IllegalArgumentException.class, () -> derive.mergeArrays(Arrays.asList(recordWithDouble, recordWithInteger), false, false));

    // Two different records cannot be merged due to extra field 'F2'
    JsonNode arrayOfIntegers = mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"double\"}}");
    JsonNode arrayOfStrings = mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"string\"}}");
    assertThrows(IllegalArgumentException.class, () -> derive.mergeArrays(Arrays.asList(arrayOfIntegers, arrayOfStrings), false, false));
  }

  @Test
  public void testDeriveMergeUnionsPrimitive() throws IOException {
    // Null, int type and array should be merged together
    String nullMessage = "{\"UnionPrimitive\": null}";
    String intUnionMessage = "{\"UnionPrimitive\": {\"int\":12}}";
    String arrayUnionMessage = "{\"UnionPrimitive\": {\"array\": [true,false]}}";
    testUnion(Arrays.asList(nullMessage, intUnionMessage, arrayUnionMessage), "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"UnionPrimitive\",\"type\":[{\"type\":\"array\",\"items\":\"boolean\"},\"int\",\"null\"]}]}");
  }

  @Test
  public void testDeriveMergeUnionsPrimitiveSingleBranch() throws IOException {
    // Single branch is taken as type union
    String stringMessage = "{\"UnionPrimitive\": {\"string\": \"12\"}}";
    testUnion(Collections.singletonList(stringMessage), "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"UnionPrimitive\",\"type\":[\"string\"]}]}");
  }

  @Test
  public void testDeriveArrayOfUnions() throws IOException {
    // Int, double, null and array should be merged together inside array
    String arrayOfUnions = "{\"arrayOfUnions\":[{\"int\":12}, {\"double\":1.2}, null, {\"array\":[12,13]}]}";
    String expectedSchema = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"arrayOfUnions\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"array\",\"items\":\"int\"},\"double\",\"int\",\"null\"]}}]}";
    generateSchemaAndCheckExpected(arrayOfUnions, expectedSchema);
  }

  @Test
  public void testDeriveArrayOfUnionsRecursive() throws IOException {
    // Int, double, null and array should be merged together inside array
    String arrayOfUnions = "{\"arr\":[{\"F1\":[null, {\"boolean\":true}]}, {\"F1\":[{\"int\":12}]}, {\"F1\":[null, null]}]}";
    String expectedSchema = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"arr\",\"fields\":[{\"name\":\"F1\",\"type\":{\"type\":\"array\",\"items\":[\"boolean\",\"int\",\"null\"]}}]}}}]}";
    generateSchemaAndCheckExpected(arrayOfUnions, expectedSchema);
  }
  @Test
  public void testDeriveMergeUnionsRecursive() throws IOException {
    // Test recursive merging of field new and old, and merging of 2 different records R1 and R2
    String message1 = "{\"value\": {\"length\": {\"R1\": {\"new\": {\"int\": 5}, \"old\": {\"long\": 523233232333}}}}}";
    String message2 = "{\"value\": {\"length\": {\"R1\": {\"new\": {\"long\": 12121276767225}, \"old\": null}}}}";
    String message3 = "{\"value\": {\"length\": {\"R2\": {\"first\": \"J\", \"second\": \"S\"}}}}";
    testUnion(Arrays.asList(message1, message2, message3), "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"value\",\"type\":{\"type\":\"record\",\"name\":\"value\",\"fields\":[{\"name\":\"length\",\"type\":[{\"type\":\"record\",\"name\":\"R2\",\"fields\":[{\"name\":\"first\",\"type\":\"string\"},{\"name\":\"second\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"R1\",\"fields\":[{\"name\":\"new\",\"type\":[\"int\",\"long\"]},{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}]}]}}]}");
  }

  @Test
  public void testDeriveMergeUnions() throws IOException {
    // Test recursive merging of field new and old, and merging of 2 different records R1 and R2
    DeriveAvroSchema deriveAvro = (DeriveAvroSchema) derive;
    JsonNode recordWithString = mapper.readTree(String.format(RECORD_WITH_STRING, "string"));
    JsonNode recordWithInteger = mapper.readTree("{\"type\":\"object\",\"properties\":{\"int\":{\"type\":\"int\"}}}");
    deriveAvro.mergeUnions(Arrays.asList(recordWithString, recordWithInteger), new ArrayList<>());
    String expectedSchema = "{\"type\":\"union\",\"properties\":[{\"type\":\"int\"},{\"type\":\"string\"}]}";
    assertEquals(recordWithString.toString(), expectedSchema);
    assertEquals(recordWithInteger.toString(), expectedSchema);
  }

  @Test
  public void testDeriveUnionsFailure() {
    // intMessage is not tagged with int and hence not of type union, should raise error
    String nullMessage = "{\"UnionPrimitive\": null}";
    String intMessage = "{\"UnionPrimitive\": 12}";
    assertThrows(IllegalArgumentException.class, () -> testUnion(Arrays.asList(nullMessage, intMessage), ""));

    // Union tag doesn't match type, F1 != int
    String intF1Message = "{\"UnionPrimitive\": {\"F1\":12}}";
    assertThrows(IllegalArgumentException.class, () -> testUnion(Arrays.asList(nullMessage, intF1Message), ""));

    // Record has 2 fields and can't be of type union or tag name for object is missing
    String recordMessage = "{\"UnionPrimitive\": {\"F1\":45, \"F2\":12}}";
    assertThrows(IllegalArgumentException.class, () -> testUnion(Arrays.asList(nullMessage, recordMessage), ""));
  }

  @Test
  public void testDeriveMultipleMessages() throws JsonProcessingException {
    // Message1 and Message2 cannot be merged due to conflicting types of F2
    // Message3 cannot be merged with both because of extra field F3, hence we have 3 different schema generated
    JsonNode message1 = mapper.readTree("{\"F1\": 1.5, \"F2\": true}");
    JsonNode message2 = mapper.readTree("{\"F1\": 1, \"F2\": 1}");
    JsonNode message3 = mapper.readTree("{\"F3\": [1, 1.5, 3]}");
    JsonNode schema = derive.getSchemaForMultipleMessages(Arrays.asList(message1, message1, message2, message2, message3, message1, message1)).get("schemas");

    assertEquals(schema.size(), 3);
    String expectedSchema1 = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"F1\",\"type\":\"double\"},{\"name\":\"F2\",\"type\":\"boolean\"}]}";
    assertEquals(schema.get(0).get("schema").toString(), expectedSchema1);
    assertEquals(schema.get(0).get("messagesMatched").toString(), "[0,1,5,6]");

    String expectedSchema2 = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"F1\",\"type\":\"double\"},{\"name\":\"F2\",\"type\":\"int\"}]}";
    assertEquals(schema.get(1).get("schema").toString(), expectedSchema2);
    assertEquals(schema.get(1).get("messagesMatched").toString(), "[2,3]");

    String expectedSchema3 = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"F3\",\"type\":{\"type\":\"array\",\"items\":\"double\"}}]}";
    assertEquals(schema.get(2).get("schema").toString(), expectedSchema3);
    assertEquals(schema.get(2).get("messagesMatched").toString(), "[4]");
  }

  @Test
  public void testDeriveMultipleMessagesWithUnion() throws JsonProcessingException {
    // Field F1 should have all branches: int, long and null
    // F2 should have type array and null
    // Message3 has extra field F3 and cannot be merged with others
    JsonNode message1 = mapper.readTree("{\"F1\": {\"int\":12}, \"F2\": null}");
    JsonNode message2 = mapper.readTree("{\"F1\": {\"long\":12}, \"F2\": {\"array\":[12]}}");
    JsonNode message3 = mapper.readTree("{\"F3\": {\"string\":\"1\"}, \"F1\": null}");
    JsonNode schema = derive.getSchemaForMultipleMessages(Arrays.asList(message1, message2, message3, message3)).get("schemas");

    assertEquals(schema.size(), 2);
    String expectedSchema1 = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"F1\",\"type\":[\"int\",\"long\",\"null\"]},{\"name\":\"F2\",\"type\":[{\"type\":\"array\",\"items\":\"int\"},\"null\"]}]}";
    assertEquals(schema.get(0).get("schema").toString(), expectedSchema1);
    assertEquals(schema.get(0).get("messagesMatched").toString(), "[0,1]");

    String expectedSchema2 = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"F1\",\"type\":[\"int\",\"long\",\"null\"]},{\"name\":\"F3\",\"type\":[\"string\"]}]}";
    assertEquals(schema.get(1).get("schema").toString(), expectedSchema2);
    assertEquals(schema.get(1).get("messagesMatched").toString(), "[2,3]");
  }

  @Test
  public void testDeriveMultipleMessagesWithUnionAndOtherTypes() throws JsonProcessingException {
    // F1 should have 3 different types - union, int and array of int
    JsonNode message1 = mapper.readTree("{\"F1\": {\"int\":12}}");
    JsonNode message2 = mapper.readTree("{\"F1\": {\"long\":12}}");
    JsonNode message3 = mapper.readTree("{\"F1\": 12}");
    JsonNode message4 = mapper.readTree("{\"F1\": [12, 23]}");
    JsonNode schema = derive.getSchemaForMultipleMessages(Arrays.asList(message1, message2, message3, message4)).get("schemas");

    assertEquals(schema.size(), 3);
    String expectedSchema1 = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"F1\",\"type\":[\"int\",\"long\"]}]}";
    assertEquals(schema.get(0).get("schema").toString(), expectedSchema1);
    assertEquals(schema.get(0).get("messagesMatched").toString(), "[0,1]");

    String expectedSchema2 = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"F1\",\"type\":\"int\"}]}";
    assertEquals(schema.get(1).get("schema").toString(), expectedSchema2);
    assertEquals(schema.get(1).get("messagesMatched").toString(), "[2]");

    String expectedSchema3 = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"F1\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}";
    assertEquals(schema.get(2).get("schema").toString(), expectedSchema3);
    assertEquals(schema.get(2).get("messagesMatched").toString(), "[3]");
  }
}