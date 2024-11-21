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
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DeriveJsonSchemaTest extends DeriveSchemaTest {

  public DeriveJsonSchemaTest() {
    derive = new DeriveJsonSchema();
  }

  public void generateSchemaAndCheckExpected(List<String> messages, String expectedSchema)
      throws IOException {
    List<JsonNode> messagesJson = new ArrayList<>();
    for (String message : messages) {
      messagesJson.add(mapper.readTree(message));
    }
    JsonNode schemaString = derive.getSchemaForArray(messagesJson, "ArrayObject");
    JsonSchema schema = new JsonSchema(schemaString.toString());
    schema.validate();
    assertEquals(schema.toString(), expectedSchema);
  }

  public void matchAndValidate(String message, JsonNode schemaString, String expectedSchema)
      throws IOException {
    JsonSchema schema = new JsonSchema(schemaString.toString());
    schema.validate();
    schema.validate(mapper.readValue(message, ObjectNode.class));
    assertEquals(schema.toString(), expectedSchema);
  }

  public void generateSchemasAndMatchExpectedMergeArrays(String schemaString1,
                                                         String schemaString2,
                                                         String ExpectedSchema)
      throws JsonProcessingException {
    JsonNode schema1 = mapper.readTree(schemaString1);
    JsonNode schema2 = mapper.readTree(schemaString2);
    ObjectNode schema = derive.mergeArrays(Arrays.asList(schema1, schema2), true, false);
    assertEquals(ExpectedSchema, schema.toString());
  }

  public void generateSchemasAndMatchExpectedMergeRecords(String schemaString1,
                                                          String schemaString2,
                                                          String ExpectedSchema)
      throws JsonProcessingException {
    JsonNode schema1 = mapper.readTree(schemaString1);
    JsonNode schema2 = mapper.readTree(schemaString2);
    ObjectNode schema = derive.mergeRecords(Arrays.asList(schema1, schema2));
    assertEquals(ExpectedSchema, schema.toString());
  }

  @Test
  public void testDerivePrimitive() throws JsonProcessingException {
    // Match all primitive types with expected schema
    generateSchemaAndCheckPrimitive("12", TYPE_NUMBER);
    generateSchemaAndCheckPrimitive("12.5", TYPE_NUMBER);
    generateSchemaAndCheckPrimitive("12020210222344343333333333120202102223443", TYPE_NUMBER);
    generateSchemaAndCheckPrimitive("true", TYPE_BOOLEAN);
    generateSchemaAndCheckPrimitive("\"Test\"", TYPE_STRING);
    generateSchemaAndCheckPrimitive("", TYPE_NULL);
    generateSchemaAndCheckPrimitive("null", TYPE_NULL);
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
    String nullMessage = "\"Null\": null";
    String primitiveTypesMessage = "{" + stringMessage + "," + nullMessage + "}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"Null\":{\"type\":\"null\"},\"String\":{\"type\":\"string\"}}}";
    generateSchemaAndCheckExpected(primitiveTypesMessage, expectedSchema);
  }

  @Test
  public void testDeriveRecordComplexTypesWithPrimitiveValues() throws IOException {
    // Get schema for record with combined fields F1 and F2
    String arrayOfNullsMessage = "\"RecordWithNull\": {\"Field\": null}";
    String recordOfMultipleMessage = "\"RecordWithNumber\": {\"Field\": \"62\"}";
    String complexTypesWithPrimitiveValues = "{" + arrayOfNullsMessage + "," + recordOfMultipleMessage + "}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"RecordWithNull\":{\"type\":\"object\",\"properties\":{\"Field\":{\"type\":\"null\"}}},\"RecordWithNumber\":{\"type\":\"object\",\"properties\":{\"Field\":{\"type\":\"string\"}}}}}";
    generateSchemaAndCheckExpected(complexTypesWithPrimitiveValues, expectedSchema);
  }

  @Test
  public void testDeriveRecordWithMergingOfFieldAndTypes() throws IOException {
    // Array of Records with arrays and different records, checking recursive merging of records
    String arrayOfRecordsAndArrays = "{\"ArrayOfRecordsAndArrays\": [ {\"J\":[1,11]}, {\"J\":{\"J\":12}},  {\"J\":{\"J\": true}}]}";
    String expectedSchema2 = "{\"type\":\"object\",\"properties\":{\"ArrayOfRecordsAndArrays\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"J\":{\"oneOf\":[{\"type\":\"array\",\"items\":{\"type\":\"number\"}},{\"type\":\"object\",\"properties\":{\"J\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"}]}}}]}}}}}}";
    generateSchemaAndCheckExpected(arrayOfRecordsAndArrays, expectedSchema2);
  }

  @Test
  public void testDeriveArrayPrimitive() throws IOException {
    // Empty array schema
    generateSchemaAndCheckExpected(new ArrayList<>(), EMPTY_ARRAY);
    // null array schema
    String nullItem = "null";
    String nullItemsSchema = "{\"type\":\"array\",\"items\":{\"type\":\"null\"}}";
    generateSchemaAndCheckExpected(Arrays.asList(nullItem, nullItem), nullItemsSchema);
  }

  @Test
  public void testDeriveArrayTypeArray() throws IOException {
    // Merging Arrays of different types
    String arrayOfStrings = "[\"1\"]";
    String arrayOfIntegers = "[3.5, true]";
    String expectedSchema = String.format("{\"type\":\"array\",\"items\":%s}", ARRAY_OF_BOOLEAN_NUMBERS_AND_STRINGS);
    generateSchemaAndCheckExpected(Arrays.asList(arrayOfStrings, arrayOfIntegers, arrayOfStrings, arrayOfIntegers), expectedSchema);
  }

  @Test
  public void testDeriveArrayTypeArrayComplex() throws IOException {
    // Testing recursive nesting of arrays
    String array3d = "[ [[1,2]], [[1,22]] ]";
    String expectedSchema3d = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}}}";
    generateSchemaAndCheckExpected(Arrays.asList(array3d, array3d), expectedSchema3d);
  }

  @Test
  public void testDeriveMultipleMessages() throws JsonProcessingException {
    // Merging Records with different field names
    JsonNode stringMessage = mapper.readTree("{\"string\": \"1\"}");
    JsonNode integerMessage = mapper.readTree("{\"number\": 12}");
    String expectedSchema = "{\"schemas\":[{\"schema\":{\"type\":\"object\",\"properties\":{\"number\":{\"type\":\"number\"},\"string\":{\"type\":\"string\"}}},\"messagesMatched\":[0,1,2,3]}]}";
    ObjectNode schema = derive.getSchemaForMultipleMessages(Arrays.asList(stringMessage, integerMessage, stringMessage, integerMessage));
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldMergeEmptyItemsMergeArrays() throws JsonProcessingException {
    // Merge Empty items and number type
    generateSchemasAndMatchExpectedMergeArrays(ARRAY_OF_NUMBERS, EMPTY_ARRAY, ARRAY_OF_NUMBERS);
  }

  @Test
  public void shouldCombineOneOfTypesMergeArrays() throws JsonProcessingException {
    // Merge oneOf and primitive Type
    String arrayOfBoolean = "{\"type\":\"array\",\"items\":{\"type\":\"boolean\"}}";
    generateSchemasAndMatchExpectedMergeArrays(ARRAY_OF_NUMBERS_AND_STRINGS, arrayOfBoolean, ARRAY_OF_BOOLEAN_NUMBERS_AND_STRINGS);
  }

  @Test
  public void shouldCombineArrayTypesMergeArrays() throws JsonProcessingException {
    // Merge arrays recursively
    String arrayOfArrayOfNumbersAndStrings = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}}";
    String arrayOfArrayOfStrings = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}";
    generateSchemasAndMatchExpectedMergeArrays(arrayOfArrayOfStrings, ARRAY_OF_ARRAY_OF_NUMBERS, arrayOfArrayOfNumbersAndStrings);
  }

  @Test
  public void shouldCombineFieldsMergeRecord() throws JsonProcessingException {
    // Field F1 and F2 are combined into 1
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"string\"},\"F2\":{\"type\":\"string\"}}}";
    generateSchemasAndMatchExpectedMergeRecords(String.format(RECORD_WITH_STRING, "F1"), String.format(RECORD_WITH_STRING, "F2"), expectedSchema);
  }

  @Test
  public void shouldCombineFieldTypesMergeRecord() throws JsonProcessingException {
    // Field F1's types are combined
    String recordWithNumber = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"number\"}}}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}}";
    generateSchemasAndMatchExpectedMergeRecords(String.format(RECORD_WITH_STRING, "F1"), recordWithNumber, expectedSchema);
  }

  @Test
  public void shouldCombineFieldsOfDifferentTypesMergeRecord() throws JsonProcessingException {
    // Merging Field F1 with types: string and array
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"oneOf\":[{\"type\":\"array\",\"items\":{\"type\":\"string\"}},{\"type\":\"string\"}]}}}";
    generateSchemasAndMatchExpectedMergeRecords(String.format(RECORD_WITH_STRING, "F1"), RECORD_WITH_ARRAY_OF_STRINGS, expectedSchema);
  }
}