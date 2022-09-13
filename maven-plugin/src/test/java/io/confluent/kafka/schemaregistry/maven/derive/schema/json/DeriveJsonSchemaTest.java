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

package io.confluent.kafka.schemaregistry.maven.derive.schema.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public class DeriveJsonSchemaTest {

  public void generateSchemaAndCheckPrimitive(String message, String expectedSchema)
      throws JsonProcessingException {
    Optional<ObjectNode> primitiveSchema = DeriveJsonSchema.getPrimitiveSchema(mapper.readTree(message));
    assert primitiveSchema.isPresent();
    assertEquals(primitiveSchema.get(), mapper.readTree(expectedSchema));
  }

  public void generateSchemaAndCheckPrimitiveNegative(String message)
      throws JsonProcessingException {
    Optional<ObjectNode> primitiveSchema = DeriveJsonSchema.getPrimitiveSchema(mapper.readTree(message));
    assert !primitiveSchema.isPresent();
  }

  public void generateSchemaAndCheckExpected(List<String> messages, String expectedSchema)
      throws JsonProcessingException {

    List<JsonNode> messagesJson = new ArrayList<>();
    for (String message : messages) {
      messagesJson.add(mapper.readTree(message));
    }
    JsonSchema jsonSchema = new JsonSchema(
        DeriveJsonSchema.getSchemaForArray(messagesJson, "ArrayObject"));
    assertEquals(expectedSchema, jsonSchema.toString());
    jsonSchema.validate();
  }

  public void generateSchemaAndCheckExpected(String message, String expectedSchema)
      throws JsonProcessingException {
    ObjectNode messageObject = (ObjectNode) mapper.readTree(message);
    JsonSchema jsonSchema = new JsonSchema(
        DeriveJsonSchema.getSchemaForRecord(messageObject).toString());
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
  }

  @Test
  public void testDerivePrimitiveNegative() throws JsonProcessingException {
    // Checking all complex types, should be empty option
    generateSchemaAndCheckPrimitiveNegative("[12]");
    generateSchemaAndCheckPrimitiveNegative("[1.5, true]");
    generateSchemaAndCheckPrimitiveNegative("{\"F1\":12}");
    generateSchemaAndCheckPrimitiveNegative("{\"F2\":\"12\"}");
  }

  @Test
  public void testPrimitiveTypes() throws Exception {
    // Get schema for record with fields having only primitive data types
    String primitiveTypes =
        "{\n"
            + "    \"String\": \"John Smith\",\n"
            + "    \"LongName\": 12020210210,\n"
            + "    \"BigDataType\": 120202102223443433333333331202021022234434333333333312020210,\n"
            + "    \"Integer\": 12,\n"
            + "    \"Boolean\": false,\n"
            + "    \"Float\": 1e16,\n"
            + "    \"Double\": 624333333333333333333333333333333333333333323232.789012332222222245,\n"
            + "    \"Null\": null\n"
            + "  }";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"BigDataType\":{\"type\":\"number\"},\"Boolean\":{\"type\":\"boolean\"},\"Double\":{\"type\":\"number\"},\"Float\":{\"type\":\"number\"},\"Integer\":{\"type\":\"number\"},\"LongName\":{\"type\":\"number\"},\"Null\":{\"type\":\"null\"},\"String\":{\"type\":\"string\"}}}";
    generateSchemaAndCheckExpected(primitiveTypes, expectedSchema);
  }

  @Test
  public void testComplexTypesWithPrimitiveValues() throws IOException {
    // Get schema for record with arrays and records having only primitive data types
    String complexTypesWithPrimitiveValues =
        "{\n"
            + "    \"ArrayEmpty\": [],\n"
            + "    \"ArrayNull\": [null],\n"
            + "    \"ArrayString\": [\"John Smith\", \"Tom Davies\"],\n"
            + "    \"ArrayInteger\": [12, 13, 14],\n"
            + "    \"ArrayBoolean\": [false, true, false],\n"
            + "    \"DoubleRecord\": {\"Double1\": 62.4122121, \"Double2\": 62.4122121},\n"
            + "    \"IntRecord\": {\"Int1\": 62, \"Int2\": 12},\n"
            + "    \"MixedRecord\": {\"Int1\": 62, \"Double1\": 1.2212, \"name\" : \"Just Testing\"}\n"
            + "  }";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"ArrayBoolean\":{\"type\":\"array\",\"items\":{\"type\":\"boolean\"}},\"ArrayEmpty\":{\"type\":\"array\",\"items\":{}},\"ArrayInteger\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}},\"ArrayNull\":{\"type\":\"array\",\"items\":{\"type\":\"null\"}},\"ArrayString\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"DoubleRecord\":{\"type\":\"object\",\"properties\":{\"Double1\":{\"type\":\"number\"},\"Double2\":{\"type\":\"number\"}}},\"IntRecord\":{\"type\":\"object\",\"properties\":{\"Int1\":{\"type\":\"number\"},\"Int2\":{\"type\":\"number\"}}},\"MixedRecord\":{\"type\":\"object\",\"properties\":{\"Double1\":{\"type\":\"number\"},\"Int1\":{\"type\":\"number\"},\"name\":{\"type\":\"string\"}}}}}";
    generateSchemaAndCheckExpected(complexTypesWithPrimitiveValues, expectedSchema);
  }

  @Test
  public void testComplexTypesRecursive() throws IOException {
    // Get schema for record with arrays and records having complex types
    String message =
        "{\n"
            + "    \"ArrayOfRecords\": [{\"Int1\": 6.1, \"Int2\": 12}, {\"Int2\": 2, \"Int1\": 62.5}],\n"
            + "    \"RecordOfArrays\": {\"ArrayInt1\": [12, 13,14], \"ArrayBoolean1\": [true, false]},\n"
            + "    \"RecordOfRecords\": {\"Record1\": {\"name\": \"Tom\", \"place\": \"Bom\"}, \"Record2\": { \"place\": \"Bom\", \"thing\": \"Kom\"}},\n"
            + "    \"Array2d\": [[1,2], [2,3]],\n"
            + "    \"Array3d\": [[[1,2]], [[2,3,3,4]]],\n"
            + "    \"Array2dEmpty\": [[], []],\n"
            + "    \"Array2dDiff\": [[1], [true]],\n"
            + "    \"Array2dNull\": [[null], [null]]\n"
            + "  }";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"Array2d\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}},\"Array2dDiff\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"}]}}},\"Array2dEmpty\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{}}},\"Array2dNull\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"null\"}}},\"Array3d\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}},\"ArrayOfRecords\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Int1\":{\"type\":\"number\"},\"Int2\":{\"type\":\"number\"}}}},\"RecordOfArrays\":{\"type\":\"object\",\"properties\":{\"ArrayBoolean1\":{\"type\":\"array\",\"items\":{\"type\":\"boolean\"}},\"ArrayInt1\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}},\"RecordOfRecords\":{\"type\":\"object\",\"properties\":{\"Record1\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"place\":{\"type\":\"string\"}}},\"Record2\":{\"type\":\"object\",\"properties\":{\"place\":{\"type\":\"string\"},\"thing\":{\"type\":\"string\"}}}}}}}";
    generateSchemaAndCheckExpected(message, expectedSchema);
  }

  @Test
  public void testArrayDifferentTypes() throws IOException {

    // Array has elements of type number, float and string represented as oneOf in schema
    String message =
        "{\n"
            + "    \"ArrayOfDifferentTypes\": [2, 13.1, true, \"J\", \"K\"],\n"
            + "    \"ArrayOfDifferentTypes2\": [{\"J\": true},{\"J\":1}],\n"
            + "    \"ArrayOfDifferentTypes3\": [[1,2,34,true], [false, \"K\"]]\n"
            + "  }";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"ArrayOfDifferentTypes\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"},{\"type\":\"string\"}]}},\"ArrayOfDifferentTypes2\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"J\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"}]}}}},\"ArrayOfDifferentTypes3\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"},{\"type\":\"string\"}]}}}}}";
    generateSchemaAndCheckExpected(message, expectedSchema);

    // Array of Records with arrays and different records, checking recursive merging of records
    String message2 = "{\"ArrayOfRecords\": [[ {\"J\":[1,11]}, {\"J\":{\"J\":12}},  {\"J\":{\"J\": true}}]]}";
    String expectedSchema2 = "{\"type\":\"object\",\"properties\":{\"ArrayOfRecords\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"J\":{\"oneOf\":[{\"type\":\"array\",\"items\":{\"type\":\"number\"}},{\"type\":\"object\",\"properties\":{\"J\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"}]}}}]}}}}}}}";
    generateSchemaAndCheckExpected(message2, expectedSchema2);
  }

  @Test
  public void testDeriveArrayPrimitive() throws JsonProcessingException {
    // Merging number types
    String number12 = "12";
    String number45 = "45";
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"number\"}}";
    generateSchemaAndCheckExpected(Arrays.asList(number12, number45, number12), expectedSchema);
  }

  @Test
  public void testDeriveArrayDifferentPrimitive() throws JsonProcessingException {
    // Merging number types and boolean types
    String number12 = "12";
    String number45 = "true";
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"}]}}";
    generateSchemaAndCheckExpected(Arrays.asList(number12, number45, number12), expectedSchema);
  }

  @Test
  public void testDeriveArrayTypeArray() throws JsonProcessingException {
    // Merging Arrays of different types
    String arrayOfStrings = "[1, 2]";
    String arrayOfIntegers = "[3.5, true]";
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"}]}}}";
    generateSchemaAndCheckExpected(Arrays.asList(arrayOfStrings, arrayOfIntegers, arrayOfStrings, arrayOfIntegers), expectedSchema);
  }

  @Test
  public void testDeriveArrayRecords() throws JsonProcessingException {
    // Merging Records with different field names
    String arrayOfStrings = "{\"ArrayString\": [\"John Smith\", \"Tom Davies\"]}";
    String arrayOfIntegers = "{\"ArrayInteger\": [1, 2]}";
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"ArrayInteger\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}},\"ArrayString\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}}}";
    generateSchemaAndCheckExpected(Arrays.asList(arrayOfStrings, arrayOfIntegers, arrayOfStrings, arrayOfIntegers), expectedSchema);
  }

}