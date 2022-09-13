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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.json.MergeJsonUtils.*;
import static org.junit.Assert.assertEquals;

public class MergeJsonUtilsTest {

  private final ObjectMapper mapper = new ObjectMapper();

  String emptyArray = "{\"type\":\"array\",\"items\":{}}";
  String arrayOfNumbers = "{\"type\":\"array\",\"items\":{\"type\":\"number\"}}";
  String arrayOfStrings = "{\"type\":\"array\",\"items\":{\"type\":\"string\"}}";
  String arrayOfNumbersAndStrings = "{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}";
  String arrayOfArrayOfNumbers = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}";
  String arrayOfArrayOfStrings = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}";
  String arrayOfArrayOfNumbersAndStrings = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}}";
  String arrayOfObjectWithNumber = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"number\"}}}}";
  String recordWithString = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"string\"}}}";
  String recordWithArrayOfStrings = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}}";

  public void generateSchemaAndCheckUnique(String schemaString1)
      throws JsonProcessingException {
    ObjectNode schema = (ObjectNode) mapper.readTree(schemaString1);
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema, schema, schema));
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).equals(schema));
  }

  public void generateSchemasAndMatchExpectedMergeArrays(String schemaString1,
                                                         String schemaString2,
                                                         String ExpectedSchema)
      throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree(schemaString1);
    ObjectNode schema2 = (ObjectNode) mapper.readTree(schemaString2);
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeArrays(schemas);
    assertEquals(ExpectedSchema, schema.toString());
  }

  public void generateSchemasAndMatchExpectedMergeRecords(String schemaString1,
                                                          String schemaString2,
                                                          String ExpectedSchema)
      throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree(schemaString1);
    ObjectNode schema2 = (ObjectNode) mapper.readTree(schemaString2);
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeRecords(schemas);
    assertEquals(ExpectedSchema, schema.toString());
  }

  @Test
  public void shouldGetOneUnique() throws JsonProcessingException {
    generateSchemaAndCheckUnique(arrayOfNumbers);
    generateSchemaAndCheckUnique(arrayOfArrayOfNumbers);
    generateSchemaAndCheckUnique(recordWithString);
    generateSchemaAndCheckUnique(recordWithArrayOfStrings);
  }

  @Test
  public void shouldGetDifferentUniqueElementsJson() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree(arrayOfNumbers);
    ObjectNode schema2 = (ObjectNode) mapper.readTree(arrayOfArrayOfNumbers);
    ObjectNode schema3 = (ObjectNode) mapper.readTree(recordWithString);
    ObjectNode schema4 = (ObjectNode) mapper.readTree(recordWithArrayOfStrings);
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2, schema2, schema3, schema4));
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 4);
    for (ObjectNode schema : schemas) {
      assert (uniqueSchemas.contains(schema));
    }
  }

  @Test
  public void shouldNotMakeAnyChangesMergeArrays() throws JsonProcessingException {
    generateSchemasAndMatchExpectedMergeArrays(arrayOfNumbers, arrayOfNumbers, arrayOfNumbers);
    generateSchemasAndMatchExpectedMergeArrays(arrayOfArrayOfNumbers, arrayOfArrayOfNumbers, arrayOfArrayOfNumbers);
  }

  @Test
  public void shouldMergeEmptyItemsMergeArrays() throws JsonProcessingException {
    generateSchemasAndMatchExpectedMergeArrays(emptyArray, arrayOfNumbers, arrayOfNumbers);
    generateSchemasAndMatchExpectedMergeArrays(arrayOfNumbers, emptyArray, arrayOfNumbers);
  }

  @Test
  public void shouldCombinePrimitiveTypesMergeArrays() throws JsonProcessingException {
    generateSchemasAndMatchExpectedMergeArrays(arrayOfStrings, arrayOfNumbers, arrayOfNumbersAndStrings);
    generateSchemasAndMatchExpectedMergeArrays(arrayOfNumbers, arrayOfStrings, arrayOfNumbersAndStrings);
  }

  @Test
  public void shouldCombineArrayTypesMergeArrays() throws JsonProcessingException {
    generateSchemasAndMatchExpectedMergeArrays(arrayOfArrayOfNumbers, arrayOfArrayOfStrings, arrayOfArrayOfNumbersAndStrings);
    generateSchemasAndMatchExpectedMergeArrays(arrayOfArrayOfStrings, arrayOfArrayOfNumbers, arrayOfArrayOfNumbersAndStrings);
  }

  @Test
  public void shouldCombineRecordFieldTypeMergeArrays() throws JsonProcessingException {
    String arrayOfObjectWithString = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"string\"}}}}";
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"F1\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}}}";
    generateSchemasAndMatchExpectedMergeArrays(arrayOfObjectWithNumber, arrayOfObjectWithString, expectedSchema);
    generateSchemasAndMatchExpectedMergeArrays(arrayOfObjectWithString, arrayOfObjectWithNumber, expectedSchema);

  }

  @Test
  public void shouldCombineRecordFieldsMergeArrays() throws JsonProcessingException {
    String arrayOfObjectWithNumberF2 = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"F2\":{\"type\":\"string\"}}}}";
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"number\"},\"F2\":{\"type\":\"string\"}}}}";
    generateSchemasAndMatchExpectedMergeArrays(arrayOfObjectWithNumber, arrayOfObjectWithNumberF2, expectedSchema);
    generateSchemasAndMatchExpectedMergeArrays(arrayOfObjectWithNumberF2, arrayOfObjectWithNumber, expectedSchema);
  }

  @Test
  public void shouldNotMakeAnyChangesMergeRecord() throws JsonProcessingException {
    generateSchemasAndMatchExpectedMergeRecords(recordWithString, recordWithString, recordWithString);
  }

  @Test
  public void shouldCombineFieldsMergeRecord() throws JsonProcessingException {
    String recordWithStringF2 = "{\"type\":\"object\",\"properties\":{\"F2\":{\"type\":\"string\"}}}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"string\"},\"F2\":{\"type\":\"string\"}}}";
    generateSchemasAndMatchExpectedMergeRecords(recordWithString, recordWithStringF2, expectedSchema);
    generateSchemasAndMatchExpectedMergeRecords(recordWithStringF2, recordWithString, expectedSchema);
  }

  @Test
  public void shouldCombineFieldTypesMergeRecord() throws JsonProcessingException {
    String recordWithNumber = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"number\"}}}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}}";
    generateSchemasAndMatchExpectedMergeRecords(recordWithString, recordWithNumber, expectedSchema);
    generateSchemasAndMatchExpectedMergeRecords(recordWithNumber, recordWithString, expectedSchema);
  }

  @Test
  public void shouldCombineArrayMergeRecord() throws JsonProcessingException {
    String recordWithArrayOfNumbers = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}}}";
    generateSchemasAndMatchExpectedMergeRecords(recordWithArrayOfStrings, recordWithArrayOfNumbers, expectedSchema);
    generateSchemasAndMatchExpectedMergeRecords(recordWithArrayOfNumbers, recordWithArrayOfStrings, expectedSchema);
  }

  @Test
  public void shouldCombineFieldsOfDifferentTypesMergeRecord() throws JsonProcessingException {
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"oneOf\":[{\"type\":\"array\",\"items\":{\"type\":\"string\"}},{\"type\":\"string\"}]}}}";
    generateSchemasAndMatchExpectedMergeRecords(recordWithArrayOfStrings, recordWithString, expectedSchema);
    generateSchemasAndMatchExpectedMergeRecords(recordWithString, recordWithArrayOfStrings, expectedSchema);
  }

  @Test
  public void shouldCombineFieldsOfDifferentTypesOneOfMergeRecord() throws JsonProcessingException {
    String recordWithArrayOfStringsOrString = "{\"type\":\"object\",\"properties\":{\"F1\":{\"oneOf\":[{\"type\":\"array\",\"items\":{\"type\":\"string\"}},{\"type\":\"string\"}]}}}";
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"oneOf\":[{\"type\":\"array\",\"items\":{\"type\":\"string\"}},{\"type\":\"string\"}]}}}";
    generateSchemasAndMatchExpectedMergeRecords(recordWithArrayOfStrings, recordWithArrayOfStringsOrString, expectedSchema);
    generateSchemasAndMatchExpectedMergeRecords(recordWithArrayOfStringsOrString, recordWithArrayOfStrings, expectedSchema);
  }
}