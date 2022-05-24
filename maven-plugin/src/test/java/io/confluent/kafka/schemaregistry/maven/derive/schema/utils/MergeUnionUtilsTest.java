package io.confluent.kafka.schemaregistry.maven.derive.schema.utils;

import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeUnionUtils.*;
import static org.junit.Assert.assertEquals;

public class MergeUnionUtilsTest {

  @Test
  public void shouldNotReturnPrimitiveTypeCheckUnionInRecord() {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    ArrayList<String> branch = new ArrayList<>();
    checkUnionInRecord(new JSONObject(schemaString1), branch, false);
    assertEquals(branch.size(), 0);
  }

  @Test
  public void shouldNotReturnPrimitiveTypeMismatchCheckUnionInRecord() {

    List<String> numberTypes = Arrays.asList("int", "double", "float", "long");
    List<String> otherTypes = Arrays.asList("boolean", "string");

    for (String type : numberTypes) {
      for (String name : otherTypes) {
        String schema = String.format("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"%s\",\"type\":\"%s\"}]}", name, type);
        ArrayList<String> branch = new ArrayList<>();
        checkUnionInRecord(new JSONObject(schema), branch, false);
        assertEquals(branch.size(), 0);
      }
    }

    for (String type : otherTypes) {
      for (String name : numberTypes) {
        String schema = String.format("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"%s\",\"type\":\"%s\"}]}", name, type);
        ArrayList<String> branch = new ArrayList<>();
        checkUnionInRecord(new JSONObject(schema), branch, false);
        assertEquals(branch.size(), 0);
      }
    }

  }

  @Test
  public void shouldReturnPrimitiveTypeCheckUnionInRecord() {

    for (String type : Arrays.asList("int", "double", "float", "long", "boolean", "string")) {
      String schema = String.format("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"%s\",\"type\":\"%s\"}]}", type, type);
      ArrayList<String> branch = new ArrayList<>();
      checkUnionInRecord(new JSONObject(schema), branch, false);
      assertEquals(branch.size(), 1);
      assertEquals(branch.get(0), type);
    }

    // name of the branch is chosen as type finally,
    for (String type : Arrays.asList("int", "double", "float", "long")) {
      for (String name : Arrays.asList("int", "double", "float", "long")) {
        String schema = String.format("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"%s\",\"type\":\"%s\"}]}", name, type);
        ArrayList<String> branch = new ArrayList<>();
        checkUnionInRecord(new JSONObject(schema), branch, false);
        assertEquals(branch.size(), 1);
        assertEquals(branch.get(0), name);
      }
    }

  }

  @Test
  public void shouldReturnRecordCheckUnionInRecord() {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"X\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}]}";
    ArrayList<String> branch = new ArrayList<>();
    checkUnionInRecord(new JSONObject(schemaString1), branch, true);
    assertEquals(branch.size(), 1);
    assertEquals(branch.get(0), "{\"name\":\"X\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");

    branch.remove(0);
    checkUnionInRecord(new JSONObject(schemaString1), branch, false);
    assertEquals(branch.size(), 0);
  }

  @Test
  public void shouldReturnUnionTypeCheckUnionInRecord() {
    String schemaString1 = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"length\",\"type\":[\"double\",\"long\"]}]}";
    ArrayList<String> branch = new ArrayList<>();
    checkUnionInRecord(new JSONObject(schemaString1), branch, true);
    assertEquals(branch.size(), 1);
    assertEquals(branch.get(0), "{\"name\":\"length\",\"type\":[\"double\",\"long\"]}");

    branch.remove(0);
    checkUnionInRecord(new JSONObject(schemaString1), branch, false);
    assertEquals(branch.size(), 0);
  }


  @Test
  public void shouldReturnArrayCheckUnionInRecord() {
    String schemaString1 = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"array\",\"type\":\"array\",\"items\":{\"name\":\"array\",\"type\":\"array\",\"items\":\"int\"}}]}";
    ArrayList<String> branch = checkForUnion(new JSONObject(schemaString1), true);
    assertEquals(branch.size(), 1);
    assertEquals(branch.get(0), "{\"name\":\"array\",\"type\":\"array\",\"items\":{\"name\":\"array\",\"type\":\"array\",\"items\":\"int\"}}");

    ArrayList<String> branchNoComplexTypes = checkForUnion(new JSONObject(schemaString1), false);
    assertEquals(branchNoComplexTypes.size(), 0);
  }


  @Test
  public void shouldNotCopyTypes() {
    JSONObject schema1 = new JSONObject("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}");
    JSONObject schema2 = new JSONObject("{\"name\":\"l\",\"type\":[\"boolean\",\"string\"]}");
    copyTypes(schema1, schema2);
    assertEquals("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}", schema1.toString());
    assertEquals("{\"name\":\"l\",\"type\":[\"boolean\",\"string\"]}", schema2.toString());
  }

  @Test
  public void shouldCopyTypesPrimitive() {
    JSONObject schema1 = new JSONObject("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}");
    JSONObject schema2 = new JSONObject("{\"name\":\"l\",\"type\":[\"long\"]}");
    copyTypes(schema1, schema2);
    assertEquals("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}", schema1.toString());
    assertEquals("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}", schema2.toString());
  }

  @Test
  public void shouldCopyTypesArray() {
    String schemaString1 = "{\"name\":\"arr\",\"type\":[\"int\",{\"name\":\"array\",\"type\":\"array\",\"items\":\"string\"}]}";
    JSONObject schema1 = new JSONObject(schemaString1);
    JSONObject schema2 = new JSONObject("{\"name\":\"arr\",\"type\":\"record\",\"fields\":[{\"name\":\"array\",\"type\":\"array\",\"items\":\"string\"}]}");
    copyTypes(schema1, schema2);
    assertEquals(schemaString1, schema1.toString());
    assertEquals(schemaString1, schema2.toString());
  }

  @Test
  public void shouldCopyTypesRecord() {
    String schemaString1 = "{\"name\":\"arr\",\"type\":[\"int\",{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}]}";
    JSONObject schema1 = new JSONObject(schemaString1);
    JSONObject schema2 = new JSONObject("{\"name\":\"arr\",\"type\":\"record\",\"fields\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}]}");
    copyTypes(schema1, schema2);
    assertEquals(schemaString1, schema1.toString());
    assertEquals(schemaString1, schema2.toString());
  }

  @Test
  public void shouldMatchTypesPrimitive() {
    JSONObject schema1 = new JSONObject("{\"name\":\"l\",\"type\":[\"long\"]}");
    JSONObject schema2 = new JSONObject("{\"name\":\"l\",\"type\":\"null\"}");
    matchTypes(schema1, schema2);
    assertEquals("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}", schema1.toString());
    assertEquals("{\"name\":\"l\",\"type\":\"null\"}", schema2.toString());
  }

  @Test
  public void shouldMatchTypesArray() {
    String schemaString1 = "{\"name\":\"arr\",\"type\":[{\"name\":\"array\",\"type\":\"array\",\"items\":\"string\"}]}";
    JSONObject schema1 = new JSONObject(schemaString1);
    JSONObject schema2 = new JSONObject("{\"name\":\"arr\",\"type\":[\"int\"]}");
    matchTypes(schema1, schema2);
    String expectedSchema = "{\"name\":\"arr\",\"type\":[\"int\",{\"name\":\"array\",\"type\":\"array\",\"items\":\"string\"}]}";
    assertEquals(expectedSchema, schema1.toString());
    assertEquals("{\"name\":\"arr\",\"type\":[\"int\"]}", schema2.toString());
  }

  @Test
  public void shouldMatchTypesRecord() {
    String schemaString1 = "{\"name\":\"arr\",\"type\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}]}";
    JSONObject schema1 = new JSONObject(schemaString1);
    JSONObject schema2 = new JSONObject("{\"name\":\"arr\",\"type\":[\"int\"]}");
    matchTypes(schema1, schema2);
    String expectedSchema = "{\"name\":\"arr\",\"type\":[\"int\",{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}]}";
    assertEquals(expectedSchema, schema1.toString());
    assertEquals("{\"name\":\"arr\",\"type\":[\"int\"]}", schema2.toString());
  }

  @Test
  public void shouldMatchRecordTypeWithRecord() {
    String schemaString1 = "{\"name\":\"revision\",\"type\":\"null\"}";
    String schemaString2 = "{\"name\":\"revision\",\"type\":{\"name\":\"revision\",\"type\":\"record\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}]}}";

    JSONObject schema1 = new JSONObject(schemaString1);
    JSONObject schema2 = new JSONObject(schemaString2);

    matchRecordTypeWithRecord(schema1, schema2, false);
    String expectedSchema = "{\"name\":\"revision\",\"type\":[\"int\",\"null\"]}";
    assertEquals(expectedSchema, schema1.toString());
    assertEquals(schemaString2, schema2.toString());
  }

  @Test
  public void shouldMatchJsonArrayWithRecord() {

    // Field with name R is matched recursively to update field 'old' with branches : long and null
    String schemaString1 = "{\"name\":\"revision\",\"type\":[\"null\",{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}]}";
    String schemaString2 = "{\"name\":\"revision\",\"type\":{\"name\":\"revision\",\"fields\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\"]}]}],\"type\":\"record\"}}";

    JSONObject schema1 = new JSONObject(schemaString1);
    JSONObject schema2 = new JSONObject(schemaString2);

    matchJsonArrayWithRecord(schema1, schema2, true);
    assertEquals(schemaString1, schema1.toString());
    String expectedSchema = "{\"name\":\"revision\",\"type\":{\"name\":\"revision\",\"fields\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}],\"type\":\"record\"}}";
    assertEquals(expectedSchema, schema2.toString());
  }

  @Test
  public void shouldMatchItems() {

    // Field with name R is matched recursively to update field 'old' with branches : long and null
    String schemaString1 = "{\"name\":\"revision\",\"items\":[\"null\",{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}]}";
    String schemaString2 = "{\"name\":\"revision\",\"items\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\"]}]}],\"type\":\"record\"}";

    JSONObject schema1 = new JSONObject(schemaString1);
    JSONObject schema2 = new JSONObject(schemaString2);

    matchItems(schema1, schema2, true);
    assertEquals(schemaString1, schema1.toString());
    String expectedSchema = "{\"name\":\"revision\",\"type\":\"record\",\"items\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}]}";
    assertEquals(expectedSchema, schema2.toString());
  }

  @Test
  public void shouldCompressPrimitiveType() {
    String schemaString1 = "{\"name\":\"l\",\"type\":\"record\",\"fields\":[{\"name\":\"boolean\",\"type\":\"boolean\"}]}";
    JSONObject schema1 = new JSONObject(schemaString1);
    compressRecordToUnion(schema1);
    assertEquals("{\"name\":\"l\",\"type\":[\"boolean\"]}", schema1.toString());
  }

  @Test
  public void shouldCompressTypeArray() {
    String schemaString1 = "{\"type\":\"array\",\"items\":{\"name\":\"l\",\"type\":\"null\"}}";
    JSONObject schema1 = new JSONObject(schemaString1);
    compressRecordToUnion(schema1);
    assertEquals("{\"type\":\"array\",\"items\":\"null\"}", schema1.toString());
  }



}