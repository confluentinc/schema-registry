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

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

/**
 * Testing whether valid schemas are generated and validating schema against the message using:
 *
 * <pre>
 *  {@code
 *    jsonSchema.validate(mapper.readTree(message));
 *  }
 * </pre>
 */

public class DeriveJsonSchemaTest {


  final DeriveJsonSchema schemaGenerator = new DeriveJsonSchema();


  @Test
  public void testPrimitiveTypes() throws Exception {

    /*
    Primitive data types test
    */

    String message =
        "{\n"
            + "    \"String\": \"John Smith\",\n"
            + "    \"LongName\": 12020210210,\n"
            + "    \"BigDataType\": 12020210222344343333333333120202102223443433333333331202021022234434333333333312020210222344343333333333120202102223443433333333331202021022234434333333333312020210222344343333333333,\n"
            + "    \"Integer\": 12,\n"
            + "    \"Boolean\": false,\n"
            + "    \"Float\": 1e16,\n"
            + "    \"Double\": 624333333333333333333333333333333333333333323232.789012332222222245,\n"
            + "    \"Null\": null\n"
            + "  }";

    ObjectNode messageObject = (ObjectNode) mapper.readTree(message);
    JsonSchema jsonSchema = new JsonSchema(
        schemaGenerator.getSchemaForRecord(messageObject, "Record").toString());

    jsonSchema.validate(mapper.readTree(message));
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"BigDataType\":{\"type\":\"number\"},\"Boolean\":{\"type\":\"boolean\"},\"Double\":{\"type\":\"number\"},\"Float\":{\"type\":\"number\"},\"Integer\":{\"type\":\"number\"},\"LongName\":{\"type\":\"number\"},\"Null\":{\"type\":\"null\"},\"String\":{\"type\":\"string\"}}}";
    assertEquals(expectedSchema, jsonSchema.toString());

  }

  @Test
  public void testComplexTypesWithPrimitiveValues() throws IOException {

    String message =
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

    ObjectNode messageObject = (ObjectNode) mapper.readTree(message);
    JsonSchema jsonSchema = new JsonSchema(
        schemaGenerator.getSchemaForRecord(messageObject, "Record").toString());
    jsonSchema.validate(mapper.readTree(message));

    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"ArrayBoolean\":{\"type\":\"array\",\"items\":{\"type\":\"boolean\"}},\"ArrayEmpty\":{\"type\":\"array\",\"items\":{}},\"ArrayInteger\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}},\"ArrayNull\":{\"type\":\"array\",\"items\":{\"type\":\"null\"}},\"ArrayString\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"DoubleRecord\":{\"type\":\"object\",\"properties\":{\"Double1\":{\"type\":\"number\"},\"Double2\":{\"type\":\"number\"}}},\"IntRecord\":{\"type\":\"object\",\"properties\":{\"Int1\":{\"type\":\"number\"},\"Int2\":{\"type\":\"number\"}}},\"MixedRecord\":{\"type\":\"object\",\"properties\":{\"Double1\":{\"type\":\"number\"},\"Int1\":{\"type\":\"number\"},\"name\":{\"type\":\"string\"}}}}}";
    assertEquals(jsonSchema.toString(), expectedSchema);
  }

  @Test
  public void testComplexTypesRecursive() throws IOException {

    /*
    Different combinations of map and array are tested
   */

    String message =
        "{\n"
            + "    \"ArrayOfRecords\": [{\"Int1\": 62323232.78901245, \"Int2\": 12}, {\"Int2\": 2, \"Int1\": 6232323.789012453}],\n"
            + "    \"RecordOfArrays\": {\"ArrayInt1\": [12, 13,14], \"ArrayBoolean1\": [true, false]},\n"
            + "    \"RecordOfRecords\": {\"Record1\": {\"name\": \"Tom\", \"place\": \"Bom\"}, \"Record2\": { \"place\": \"Bom\", \"thing\": \"Kom\"}},\n"
            + "    \"Array2d\": [[1,2], [2,3]],\n"
            + "    \"Array3d\": [[[1,2]], [[2,3,3,4]]],\n"
            + "    \"Array2dEmpty\": [[], []],\n"
            + "    \"Array2dDiff\": [[1], [true]],\n"
            + "    \"Array2dNull\": [[null], [null]],\n"
            + "    \"RecordOfArrays2\": { \"Array2D\": [ [1,2], [2,3,3,4] ], \"Array3D\": [ [[1,2]], [[2,3,3,4]]] },\n"
            + "    \"RecordOfArrays3\": { \"Array2D\": [ [{\"name\": \"J\"},{\"name\": \"K\"}], [{\"name\": \"T\"}] ]},\n"
            + "    \"RecordOfArrays4\": { \"Array2D\": [ [{\"name\": 1},{\"name\": 2}], [{\"name\": 4}] ]},\n"
            + "    \"RecordOfArrays5\": {\"key\": {\"keys\":3}},\n"
            + "    \"RecordOfArrays6\": {\"key\": {\"keys\":\"4\"}}\n"
            + "  }";

    ObjectNode messageObject = (ObjectNode) mapper.readTree(message);
    JsonSchema jsonSchema = new JsonSchema(
        schemaGenerator.getSchemaForRecord(messageObject, "Record").toString());
    jsonSchema.validate(mapper.readTree(message));

    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"Array2d\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}},\"Array2dDiff\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"boolean\"}]}}},\"Array2dEmpty\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{}}},\"Array2dNull\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"null\"}}},\"Array3d\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}},\"ArrayOfRecords\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Int1\":{\"type\":\"number\"},\"Int2\":{\"type\":\"number\"}}}},\"RecordOfArrays\":{\"type\":\"object\",\"properties\":{\"ArrayBoolean1\":{\"type\":\"array\",\"items\":{\"type\":\"boolean\"}},\"ArrayInt1\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}},\"RecordOfArrays2\":{\"type\":\"object\",\"properties\":{\"Array2D\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}},\"Array3D\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}}}},\"RecordOfArrays3\":{\"type\":\"object\",\"properties\":{\"Array2D\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}}}}}},\"RecordOfArrays4\":{\"type\":\"object\",\"properties\":{\"Array2D\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"number\"}}}}}}},\"RecordOfArrays5\":{\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"object\",\"properties\":{\"keys\":{\"type\":\"number\"}}}}},\"RecordOfArrays6\":{\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"object\",\"properties\":{\"keys\":{\"type\":\"string\"}}}}},\"RecordOfRecords\":{\"type\":\"object\",\"properties\":{\"Record1\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"place\":{\"type\":\"string\"}}},\"Record2\":{\"type\":\"object\",\"properties\":{\"place\":{\"type\":\"string\"},\"thing\":{\"type\":\"string\"}}}}}}}";
    assertEquals(expectedSchema, jsonSchema.toString());
  }

  @Test
  public void testArrayDifferentTypes() throws IOException {

      /*
      Array has elements of type number, float and string
      Elements are of different type in array, this is allowed in JSON
      They are represented using oneOf in schema
     */

    String message =
        "{\n"
            + "    \"ArrayOfDifferentTypes\": [2, 13.1, true, \"J\", \"K\"],\n"
            + "    \"ArrayOfDifferentTypes2\": [{\"J\": true},{\"J\":1}],\n"
            + "    \"ArrayOfDifferentTypes3\": [[1,2,34,true], [false, \"K\"]]\n"
            + "  }";

    ObjectNode messageObject = (ObjectNode) mapper.readTree(message);
    JsonSchema jsonSchema = new JsonSchema(
        schemaGenerator.getSchemaForRecord(messageObject, "Record").toString());
    jsonSchema.validate(mapper.readTree(message));
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"ArrayOfDifferentTypes\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":\"number\"},{\"type\":\"boolean\"}]}},\"ArrayOfDifferentTypes2\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"J\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"boolean\"}]}}}},\"ArrayOfDifferentTypes3\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"boolean\"},{\"type\":\"string\"}]}}}}}";
    assertEquals(expectedSchema, jsonSchema.toString());

    /*
      Record1 has parameters - Int1 and Int2
      Record2 has parameters - Int1 and Int3
      Both Records are of different type, this is allowed in JSON
     */

    String message2 =
        "{\n"
            + "    \"ArrayOfRecords\": [{\"Int1\": 62, \"Int2\": 12}, {\"Int1\": 1, \"Int2\": 2}],\n"
            + "    \"ArrayOfRecords1\": [{\"Int1\": 62, \"Int2\": 12}, {\"Int1\": true, \"Int2\": 2}],\n"
            + "    \"ArrayOfRecords2\": [{\"Int1\": 62, \"Int2\": 12}, {\"Int1\": 1, \"Int3\": 2}],\n"
            + "    \"ArrayOfRecords3\": [{\"Int1\": 62, \"Int2\": [true, 1]}, {\"Int1\": [1, 11, 12], \"Int3\": 2}],\n"
            + "    \"ArrayOfRecords4\": [{\"Int1\": 62, \"Int2\": [true, 1]}, {\"Int1\": [1, 11, 12], \"Int2\": [1, \"GG\"]}],\n"
            + "    \"ArrayOfRecords5\": [{\"Int1\": 62, \"Int2\": {\"Int2\": [true, 1]}}, {\"Int1\": [1, 11, 12], \"Int2\": {\"Int2\":[1, \"GG\"]} }],\n"
            + "    \"ArrayOfRecords6\": [ [{\"J\":1}], [{\"J\":true}, {\"K\":true}, {\"J\": \"Time\"}] ],\n"
            + "    \"ArrayOfRecords7\": [ [{\"J\":1}, [1] ], [{\"J\":true}, {\"K\":true}, [true]] ],\n"
            + "    \"ArrayOfRecords8\": [ "
            + "         [ {\"J\":1}, [1, true,[12, true]] ], [{\"J\":true}, {\"K\":true}, [2, true,[null, 1] ]  ]"
            + "    ],\n"
            + "    \"ArrayOfRecords9\": [ "
            + "         [ {\"J\":[1, \"TT\"]}], [{\"J\":[true, 4]}, {\"K\":true}]],"
            + "    \"ArrayOfRecords10\": [ "
            + "         [ {\"J\":[1,11]}, {\"J\":{\"J\":12}},  {\"J\":{\"J\": true}}]"
            + "    ]\n"
            + "  }";

    ObjectNode messageObject2 = (ObjectNode) mapper.readTree(message2);
    ObjectNode ObjectNode = schemaGenerator.getSchemaForRecord(messageObject2, "Record");
    JsonSchema jsonSchema2 = new JsonSchema(ObjectNode.toString());
    jsonSchema2.validate(mapper.readTree(message2));

  }


  @Test
  public void testMultipleMessages() throws IOException {

    /*
    Logic is same as array of records
    Adding basic tests as a sanity check
     */

    ArrayList<String> arr = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      String message = String.format("{\n"
          + "    \"String\": \"%d\",\n"
          + "    \"Integer\": %d,\n"
          + "    \"Boolean\": %b\n"
          + "  }", i * 100, i, i % 2 == 0);
      arr.add(message);
    }

    ObjectNode schema = (ObjectNode) schemaGenerator.getSchemaForMultipleMessages(arr).get("schema");
    JsonSchema jsonSchema = new JsonSchema(schema.toString());
    for (String message : arr) {
      jsonSchema.validate(mapper.readTree(message));
    }
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"Boolean\":{\"type\":\"boolean\"},\"Integer\":{\"type\":\"number\"},\"String\":{\"type\":\"string\"}}}";
    assertEquals(expectedSchema, jsonSchema.toString());


    /*
    Order Checking Tests for Multiple Messages
     */

    String message31 = "    {\n" +
        "      \"name\" : \"J\",\n" +
        "      \"Age\" : 13,\n" +
        "        \"Date\":151109,\n" +
        "      \"arr\" : [12, 45, 56]\n" +
        "    }";

    String message32 = "    {\n" +
        "      \"arr\" : [true, false],\n" +
        "        \"Date\":151109,\n" +
        "      \"Age\" : 13,\n" +
        "      \"name\" : \"J\"\n" +
        "    }";

    String message33 = "    {\n" +
        "      \"Age\" : 13,\n" +
        "      \"name\" : \"J\",\n" +
        "      \"arr\" : [12, 45, 56],\n" +
        "        \"Date\":151109\n" +
        "    }";

    ArrayList<String> arr3 = new ArrayList<>(Arrays.asList(message31, message32, message33));
    ObjectNode ObjectNode = (ObjectNode) schemaGenerator.getSchemaForMultipleMessages(arr3).get("schema");
    JsonSchema jsonSchema3 = new JsonSchema(ObjectNode.toString());
    for (String message : arr3) {
      jsonSchema3.validate(mapper.readTree(message));
    }

    String expectedSchema3 = "{\"type\":\"object\",\"properties\":{\"Age\":{\"type\":\"number\"},\"Date\":{\"type\":\"number\"},\"arr\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"}]}},\"name\":{\"type\":\"string\"}}}";
    assertEquals(expectedSchema3, jsonSchema3.toString());

    String message41 = "   { \"J\":{\n" +
        "      \"name\" : \"J\",\n" +
        "      \"Age\" : 13,\n" +
        "        \"Date\":151109,\n" +
        "      \"arr\" : [12, 45, 56]\n" +
        "    }}";

    String message42 = " {  \"J\" :{\n" +
        "        \"Date\":151109,\n" +
        "      \"arr\" : [true, false],\n" +
        "      \"Age\" : 13,\n" +
        "      \"name\" : \"J\"\n" +
        "    }}";

    String message43 = " {\"J\" :   {\n" +
        "      \"name\" : \"J\",\n" +
        "      \"arr\" : [true, false],\n" +
        "        \"Date\":151109,\n" +
        "      \"Age\" : 13\n" +
        "    }}";

    ArrayList<String> arr4 = new ArrayList<>(Arrays.asList(message41, message42, message43));
    ObjectNode ObjectNode2 = (ObjectNode) schemaGenerator.getSchemaForMultipleMessages(arr4).get("schema");
    JsonSchema jsonSchema4 = new JsonSchema(ObjectNode2.toString());
    for (String message : arr4) {
      jsonSchema4.validate(mapper.readTree(message));
    }
    String expectedSchema4 = "{\"type\":\"object\",\"properties\":{\"Age\":{\"type\":\"number\"},\"Date\":{\"type\":\"number\"},\"arr\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"}]}},\"name\":{\"type\":\"string\"}}}";
    assertEquals(expectedSchema4, jsonSchema3.toString());

  }

}



