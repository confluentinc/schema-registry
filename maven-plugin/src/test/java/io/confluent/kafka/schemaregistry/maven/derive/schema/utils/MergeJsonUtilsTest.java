package io.confluent.kafka.schemaregistry.maven.derive.schema.utils;

import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeJsonUtils.*;
import static org.junit.Assert.assertEquals;

public class MergeJsonUtilsTest {

  @Test
  public void shouldGetOneUniqueElementBasic() {
    JSONObject schema1 = new JSONObject("{\"name\":\"K\",\"type\":\"double\"}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema1));
    ArrayList<JSONObject> uniqueSchemas = getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).similar(schema1));
  }

  @Test
  public void shouldGetOneUniqueElement() {
    JSONObject schema1 = new JSONObject("{\"name\":\"K\",\"type\":\"double\"}");
    JSONObject schema2 = new JSONObject("{\"name\":\"K\",\"type\":\"double\"}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2));
    ArrayList<JSONObject> uniqueSchemas = getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).similar(schema1));
  }

  @Test
  public void shouldGetOneUniqueElementRecordTypeAvro() {
    JSONObject schema1 = new JSONObject("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    JSONObject schema2 = new JSONObject("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema2));
    ArrayList<JSONObject> uniqueSchemas = getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).similar(schema1));
  }

  @Test
  public void shouldGetOneUniqueElementRecordTypeJson() {
    JSONObject schema1 = new JSONObject("{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"}}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"}}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema2));
    ArrayList<JSONObject> uniqueSchemas = getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).similar(schema1));
  }


  @Test
  public void shouldGetOneUniqueElementArrayTypeAvro() {
    JSONObject schema1 = new JSONObject("{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}");
    JSONObject schema2 = new JSONObject("{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2));
    ArrayList<JSONObject> uniqueSchemas = getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).similar(schema1));
  }

  @Test
  public void shouldGetOneUniqueElementArrayTypeJson() {
    JSONObject schema1 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema2));
    ArrayList<JSONObject> uniqueSchemas = getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 1);
    assert (uniqueSchemas.get(0).similar(schema1));
  }


  @Test
  public void shouldGetDifferentUniqueElementsAvro() {
    JSONObject schema1 = new JSONObject("{\"name\":\"K\",\"type\":\"boolean\"}");
    JSONObject schema2 = new JSONObject("{\"name\":\"K\",\"type\":\"number\"}");
    JSONObject schema3 = new JSONObject("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    JSONObject schema4 = new JSONObject("{\"name\":\"R2\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    JSONObject schema5 = new JSONObject("{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2, schema2, schema3, schema4, schema5));
    ArrayList<JSONObject> uniqueSchemas = getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 5);
    for (JSONObject schema : schemas) {
      assert (uniqueSchemas.contains(schema));
    }
  }


  @Test
  public void shouldGetDifferentUniqueElementsJson() {
    JSONObject schema1 = new JSONObject("{\"name\":\"K\",\"type\":\"double\"}");
    JSONObject schema2 = new JSONObject("{\"name\":\"K\",\"type\":\"int32\"}");
    JSONObject schema3 = new JSONObject("{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"}}}");
    JSONObject schema4 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2, schema2, schema3, schema4));
    ArrayList<JSONObject> uniqueSchemas = getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 4);
    for (JSONObject schema : schemas) {
      assert (uniqueSchemas.contains(schema));
    }
  }

  @Test
  public void shouldNotMakeAnyChangesMergeArrays() {
    JSONObject schema1 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    JSONObject schema = mergeArrays(schemas);
    assertEquals(schema1.toString(), schema.toString());
  }

  @Test
  public void shouldCombinePrimitiveTypesMergeArrays() {
    JSONObject schema1 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"number\"}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"string\"}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    JSONObject schema = mergeArrays(schemas);
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldCombineArrayTypesMergeArrays() {
    JSONObject schema1 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    JSONObject schema = mergeArrays(schemas);
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldCombineRecordFieldTypeMergeArrays() {
    JSONObject schema1 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"}}}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"string\"}}}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    JSONObject schema = mergeArrays(schemas);
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Integer\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldCombineRecordFieldsMergeArrays() {
    JSONObject schema1 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"}}}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Temp\":{\"type\":\"string\"}}}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    JSONObject schema = mergeArrays(schemas);
    String expectedSchema = "{\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"},\"Temp\":{\"type\":\"string\"}}}}";
    assertEquals(expectedSchema, schema.toString());
  }


  @Test
  public void shouldNotMakeAnyChangesMergeRecord() {
    JSONObject schema1 = new JSONObject("{\"type\":\"object\",\"properties\":{\"Temp\":{\"type\":\"string\"}}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"object\",\"properties\":{\"Temp\":{\"type\":\"string\"}}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    JSONObject schema = mergeRecords(schemas);
    assertEquals(schema1.toString(), schema.toString());
  }

  @Test
  public void shouldCombineFieldsMergeRecord() {
    JSONObject schema1 = new JSONObject("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"string\"}}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"object\",\"properties\":{\"F2\":{\"type\":\"string\"}}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    JSONObject schema = mergeRecords(schemas);
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"string\"},\"F2\":{\"type\":\"string\"}}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldCombineFieldTypesMergeRecord() {
    JSONObject schema1 = new JSONObject("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"string\"}}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"number\"}}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    JSONObject schema = mergeRecords(schemas);
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":\"number\"}]}}}";
    assertEquals(expectedSchema, schema.toString());
  }

  @Test
  public void shouldCombineArrayMergeRecord() {
    JSONObject schema1 = new JSONObject("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}}");
    JSONObject schema2 = new JSONObject("{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    JSONObject schema = mergeRecords(schemas);
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":\"number\"}]}}}}";
    assertEquals(expectedSchema, schema.toString());
  }

}
