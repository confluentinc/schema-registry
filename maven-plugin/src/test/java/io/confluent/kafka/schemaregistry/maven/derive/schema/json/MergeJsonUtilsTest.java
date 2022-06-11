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

  @Test
  public void shouldGetOneUniqueElementBasic() throws JsonProcessingException {
    ObjectNode schema = (ObjectNode) mapper.readTree("{\"name\":\"K\",\"type\":\"double\"}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema, schema));
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).equals(schema));
  }

  @Test
  public void shouldGetOneUniqueElement() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"name\":\"K\",\"type\":\"double\"}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"K\",\"type\":\"double\"}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2));
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).equals(schema1));
  }

  @Test
  public void shouldGetOneUniqueElementRecordTypeAvro() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema2));
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).equals(schema1));
  }

  @Test
  public void shouldGetOneUniqueElementRecordTypeJson() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"}}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"}}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema2));
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).equals(schema1));
  }

  @Test
  public void shouldGetOneUniqueElementArrayTypeAvro() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2));
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).equals(schema1));
  }

  @Test
  public void shouldGetOneUniqueElementArrayTypeJson() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema2));
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).equals(schema1));
  }

  @Test
  public void shouldGetDifferentUniqueElementsAvro() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"name\":\"K\",\"type\":\"boolean\"}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"K\",\"type\":\"number\"}");
    ObjectNode schema3 = (ObjectNode) mapper.readTree("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ObjectNode schema4 = (ObjectNode) mapper.readTree("{\"name\":\"R2\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ObjectNode schema5 = (ObjectNode) mapper.readTree("{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2, schema2, schema3, schema4, schema5));
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 5);
    for (ObjectNode schema : schemas) {
      assert (uniqueSchemas.contains(schema));
    }
  }

  @Test
  public void shouldGetDifferentUniqueElementsJson() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"name\":\"K\",\"type\":\"double\"}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"K\",\"type\":\"int32\"}");
    ObjectNode schema3 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"}}}");
    ObjectNode schema4 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2, schema2, schema3, schema4));
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 4);
    for (ObjectNode schema : schemas) {
      assert (uniqueSchemas.contains(schema));
    }
  }

  @Test
  public void shouldNotMakeAnyChangesMergeArrays() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeArrays(schemas);
    assertEquals(schema1.toString(), schema.toString());
  }

  @Test
  public void shouldCombinePrimitiveTypesMergeArrays() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"string\"}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeArrays(schemas);
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldCombineArrayTypesMergeArrays() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeArrays(schemas);
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldCombineRecordFieldTypeMergeArrays() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"}}}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"string\"}}}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeArrays(schemas);
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Integer\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldCombineRecordFieldsMergeArrays() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"}}}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Temp\":{\"type\":\"string\"}}}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeArrays(schemas);
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"},\"Temp\":{\"type\":\"string\"}}}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldNotMakeAnyChangesMergeRecord() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"Temp\":{\"type\":\"string\"}}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"Temp\":{\"type\":\"string\"}}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeRecords(schemas);
    assertEquals(schema1.toString(), schema.toString());
  }

  @Test
  public void shouldCombineFieldsMergeRecord() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"string\"}}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"F2\":{\"type\":\"string\"}}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeRecords(schemas);
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"string\"},\"F2\":{\"type\":\"string\"}}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldCombineFieldTypesMergeRecord() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"string\"}}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"number\"}}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeRecords(schemas);
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":\"number\"}]}}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldCombineArrayMergeRecord() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schema = mergeRecords(schemas);
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":\"number\"}]}}}}";
    assertEquals(expectedSchema, schema.toString());
  }

}
