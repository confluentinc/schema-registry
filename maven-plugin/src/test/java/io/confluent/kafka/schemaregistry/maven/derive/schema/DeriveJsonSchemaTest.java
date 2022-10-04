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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class DeriveJsonSchemaTest {

  private final ObjectMapper mapper = new ObjectMapper();
  DeriveJsonSchema derive = new DeriveJsonSchema();

  public void generateSchemaAndCheckPrimitive(String message, String expectedSchema)
      throws JsonProcessingException {
    Optional<ObjectNode> primitiveSchema = derive.getPrimitiveSchema(mapper.readTree(message));
    assert primitiveSchema.isPresent();
    assertEquals(primitiveSchema.get(), mapper.readTree(expectedSchema));
  }

  public void generateSchemaAndCheckPrimitiveAbsent(String message)
      throws JsonProcessingException {
    Optional<ObjectNode> primitiveSchema = derive.getPrimitiveSchema(mapper.readTree(message));
    assert !primitiveSchema.isPresent();
  }

  public void generateSchemaAndCheckExpected(String message, String expectedSchema)
      throws JsonProcessingException {
    ObjectNode messageObject = (ObjectNode) mapper.readTree(message);
    JsonSchema jsonSchema = new JsonSchema(derive.getSchemaForRecord(messageObject).toString());
    assertEquals(expectedSchema, jsonSchema.toString());
    jsonSchema.validate(messageObject);
    jsonSchema.validate();
  }

  @Test
  public void testDerivePrimitive() throws JsonProcessingException {
    // Match all primitive types with expected schema
    generateSchemaAndCheckPrimitive("12", "{\"type\":\"number\"}");
    generateSchemaAndCheckPrimitive("12.5", "{\"type\":\"number\"}");
    generateSchemaAndCheckPrimitive("12020210222344343333333333120202102223443", "{\"type\":\"number\"}");
    generateSchemaAndCheckPrimitive("true", "{\"type\":\"boolean\"}");
    generateSchemaAndCheckPrimitive("\"Test\"", "{\"type\":\"string\"}");
    generateSchemaAndCheckPrimitive("", "{\"type\":\"null\"}");
    generateSchemaAndCheckPrimitive("null", "{\"type\":\"null\"}");
  }

  @Test
  public void testDerivePrimitiveForComplex() throws JsonProcessingException {
    // Checking all complex types, should be empty option
    generateSchemaAndCheckPrimitiveAbsent("[12]");
    generateSchemaAndCheckPrimitiveAbsent("[1.5, true]");
    generateSchemaAndCheckPrimitiveAbsent("{\"F1\":12}");
    generateSchemaAndCheckPrimitiveAbsent("{\"F2\":\"12\"}");
  }

  @Test
  public void testDeriveRecordPrimitive() throws Exception {
    // Get schema for record with fields having only primitive data types
    String stringMessage = "\"String\": \"Test\"";
    String longMessage = "\"LongName\": 12020210210";
    String nullMessage = "\"Null\": null";
    String primitiveTypesMessage = "{" + stringMessage + "," + longMessage + "," + nullMessage + "}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"LongName\":{\"type\":\"number\"},\"Null\":{\"type\":\"null\"},\"String\":{\"type\":\"string\"}}}";
    generateSchemaAndCheckExpected(primitiveTypesMessage, expectedSchema);
  }

  @Test
  public void testDeriveRecordComplexTypesWithPrimitiveValues() throws IOException {
    // Get schema for record with arrays and records having only primitive data types
    String arrayOfNullsMessage = "\"arrayOfNulls\":[null]";
    String recordOfMultipleMessage = "\"MixedRecord\": {\"Int1\": 62, \"Double1\": 1.2212, \"name\" : \"Testing\"}";
    String complexTypesWithPrimitiveValues = "{" + arrayOfNullsMessage + "," + recordOfMultipleMessage + "}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"MixedRecord\":{\"type\":\"object\",\"properties\":{\"Double1\":{\"type\":\"number\"},\"Int1\":{\"type\":\"number\"},\"name\":{\"type\":\"string\"}}},\"arrayOfNulls\":{\"type\":\"array\",\"items\":{\"type\":\"null\"}}}}";
    generateSchemaAndCheckExpected(complexTypesWithPrimitiveValues, expectedSchema);
  }

  @Test
  public void testDeriveRecordComplexTypesRecursive() throws IOException {
    // Get schema for record with arrays and records having complex types
    String recordOfArrays = "\"RecordOfArrays\": {\"ArrayInt1\": [12], \"ArrayEmpty\": []}";
    String recordOfRecords = "\"RecordOfRecords\": {\"Record1\": {\"name\": \"Tom\"}, \"Record2\": {\"place\": \"Bom\"}}";
    String complexTypesWithPrimitiveValues = "{" + recordOfArrays + "," + recordOfRecords + "}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"RecordOfArrays\":{\"type\":\"object\",\"properties\":{\"ArrayEmpty\":{\"type\":\"array\",\"items\":{}},\"ArrayInt1\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}},\"RecordOfRecords\":{\"type\":\"object\",\"properties\":{\"Record1\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}},\"Record2\":{\"type\":\"object\",\"properties\":{\"place\":{\"type\":\"string\"}}}}}}}";
    generateSchemaAndCheckExpected(complexTypesWithPrimitiveValues, expectedSchema);
  }

  @Test
  public void testDeriveRecordWithArrayOfDifferentTypes() throws IOException {
    // Array has elements of type number, float and string represented as oneOf in schema
    String arrayOfDifferentTypes = "{\"ArrayOfDifferentTypes\": [2, 13.1, true, \"J\", \"K\"]}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"ArrayOfDifferentTypes\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"},{\"type\":\"string\"}]}}}}";
    generateSchemaAndCheckExpected(arrayOfDifferentTypes, expectedSchema);

    // Array of Records with arrays and different records, checking recursive merging of records
    String arrayOfRecordsAndArrays = "{\"ArrayOfRecordsAndArrays\": [ {\"J\":[1,11]}, {\"J\":{\"J\":12}},  {\"J\":{\"J\": true}}]}";
    String expectedSchema2 = "{\"type\":\"object\",\"properties\":{\"ArrayOfRecordsAndArrays\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"J\":{\"oneOf\":[{\"type\":\"array\",\"items\":{\"type\":\"number\"}},{\"type\":\"object\",\"properties\":{\"J\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"}]}}}]}}}}}}";
    generateSchemaAndCheckExpected(arrayOfRecordsAndArrays, expectedSchema2);
  }

  @Test
  public void testDeriveMultipleMessages() throws JsonProcessingException {
    // Merging Records with different field names
    String arrayOfStrings = "{\"ArrayString\": \"1\"}";
    String arrayOfIntegers = "{\"ArrayInteger\": 12}";
    String expectedSchema = "{\"schema\":{\"type\":\"object\",\"properties\":{\"ArrayInteger\":{\"type\":\"number\"},\"ArrayString\":{\"type\":\"string\"}}}}";
    ObjectNode schema = derive.getSchemaForMultipleMessages(Arrays.asList(arrayOfStrings, arrayOfIntegers, arrayOfStrings, arrayOfIntegers));
    assertEquals(expectedSchema, schema.toString());
  }
}