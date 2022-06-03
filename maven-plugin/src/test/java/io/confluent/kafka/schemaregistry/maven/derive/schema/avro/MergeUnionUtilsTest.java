package io.confluent.kafka.schemaregistry.maven.derive.schema.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.avro.MergeUnionUtils.*;
import static org.junit.Assert.assertEquals;

public class MergeUnionUtilsTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void shouldNotReturnPrimitiveTypeCheckUnionInRecord() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    ArrayList<String> branch = new ArrayList<>();
    checkUnionInRecord(mapper.readValue(schemaString1, ObjectNode.class), branch, false);
    assertEquals(branch.size(), 0);
  }

  @Test
  public void shouldNotReturnPrimitiveTypeMismatchCheckUnionInRecord() throws JsonProcessingException {

    List<String> numberTypes = Arrays.asList("int", "double", "float", "long");
    List<String> otherTypes = Arrays.asList("boolean", "string");

    for (String type : numberTypes) {
      for (String name : otherTypes) {
        String schema = String.format("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"%s\",\"type\":\"%s\"}]}", name, type);
        ArrayList<String> branch = new ArrayList<>();
        checkUnionInRecord(mapper.readValue(schema, ObjectNode.class), branch, false);
        assertEquals(branch.size(), 0);
      }
    }

    for (String type : otherTypes) {
      for (String name : numberTypes) {
        String schema = String.format("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"%s\",\"type\":\"%s\"}]}", name, type);
        ArrayList<String> branch = new ArrayList<>();
        checkUnionInRecord(mapper.readValue(schema, ObjectNode.class), branch, false);
        assertEquals(branch.size(), 0);
      }
    }

  }

  @Test
  public void shouldReturnPrimitiveTypeCheckUnionInRecord() throws JsonProcessingException {

    for (String type : Arrays.asList("int", "double", "float", "long", "boolean", "string")) {
      String schema = String.format("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"%s\",\"type\":\"%s\"}]}", type, type);
      ArrayList<String> branch = new ArrayList<>();
      checkUnionInRecord(mapper.readValue(schema, ObjectNode.class), branch, false);
      assertEquals(branch.size(), 1);
      assertEquals(branch.get(0), type);
    }

    // name of the branch is chosen as type finally,
    for (String type : Arrays.asList("int", "double", "float", "long")) {
      for (String name : Arrays.asList("int", "double", "float", "long")) {
        String schema = String.format("{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"%s\",\"type\":\"%s\"}]}", name, type);
        ArrayList<String> branch = new ArrayList<>();
        checkUnionInRecord(mapper.readValue(schema, ObjectNode.class), branch, false);
        assertEquals(branch.size(), 1);
        assertEquals(branch.get(0), name);
      }
    }

  }

  @Test
  public void shouldReturnRecordCheckUnionInRecord() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"X\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}]}";
    ArrayList<String> branch = new ArrayList<>();
    checkUnionInRecord(mapper.readValue(schemaString1, ObjectNode.class), branch, true);
    assertEquals(branch.size(), 1);
    assertEquals(branch.get(0), "{\"name\":\"X\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");

    branch.remove(0);
    checkUnionInRecord(mapper.readValue(schemaString1, ObjectNode.class), branch, false);
    assertEquals(branch.size(), 0);
  }

  @Test
  public void shouldReturnUnionTypeCheckUnionInRecord() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"length\",\"type\":[\"double\",\"long\"]}]}";
    ArrayList<String> branch = new ArrayList<>();
    checkUnionInRecord(mapper.readValue(schemaString1, ObjectNode.class), branch, true);
    assertEquals(branch.size(), 1);
    assertEquals(branch.get(0), "{\"name\":\"length\",\"type\":[\"double\",\"long\"]}");

    branch.remove(0);
    checkUnionInRecord(mapper.readValue(schemaString1, ObjectNode.class), branch, false);
    assertEquals(branch.size(), 0);
  }


  @Test
  public void shouldReturnArrayCheckUnionInRecord() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"array\",\"type\":\"array\",\"items\":{\"name\":\"array\",\"type\":\"array\",\"items\":\"int\"}}]}";
    ArrayList<String> branch = checkForUnion(mapper.readValue(schemaString1, ObjectNode.class), true);
    assertEquals(branch.size(), 1);
    assertEquals(branch.get(0), "{\"name\":\"array\",\"type\":\"array\",\"items\":{\"name\":\"array\",\"type\":\"array\",\"items\":\"int\"}}");

    ArrayList<String> branchNoComplexTypes = checkForUnion(mapper.readValue(schemaString1, ObjectNode.class), false);
    assertEquals(branchNoComplexTypes.size(), 0);
  }


  @Test
  public void shouldNotCopyTypes() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"l\",\"type\":[\"boolean\",\"string\"]}");
    copyTypes(schema1, schema2);
    assertEquals("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}", schema1.toString());
    assertEquals("{\"name\":\"l\",\"type\":[\"boolean\",\"string\"]}", schema2.toString());
  }

  @Test
  public void shouldCopyTypesPrimitive() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"l\",\"type\":[\"long\"]}");
    copyTypes(schema1, schema2);
    assertEquals("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}", schema1.toString());
    assertEquals("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}", schema2.toString());
  }

  @Test
  public void shouldCopyTypesArray() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"arr\",\"type\":[\"int\",{\"name\":\"array\",\"type\":\"array\",\"items\":\"string\"}]}";
    ObjectNode schema1 = mapper.readValue(schemaString1, ObjectNode.class);
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"arr\",\"type\":\"record\",\"fields\":[{\"name\":\"array\",\"type\":\"array\",\"items\":\"string\"}]}");
    copyTypes(schema1, schema2);
    assertEquals(schemaString1, schema1.toString());
    assertEquals(schemaString1, schema2.toString());
  }

  @Test
  public void shouldCopyTypesRecord() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"arr\",\"type\":[\"int\",{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}]}";
    ObjectNode schema1 = mapper.readValue(schemaString1, ObjectNode.class);
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"arr\",\"type\":\"record\",\"fields\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}]}");
    copyTypes(schema1, schema2);
    assertEquals(schemaString1, schema1.toString());
    assertEquals(schemaString1, schema2.toString());
  }

  @Test
  public void shouldMatchTypesPrimitive() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"name\":\"l\",\"type\":[\"long\"]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"l\",\"type\":\"null\"}");
    matchTypes(schema1, schema2);
    assertEquals("{\"name\":\"l\",\"type\":[\"long\",\"null\"]}", schema1.toString());
    assertEquals("{\"name\":\"l\",\"type\":\"null\"}", schema2.toString());
  }

  @Test
  public void shouldMatchTypesArray() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"arr\",\"type\":[{\"name\":\"array\",\"type\":\"array\",\"items\":\"string\"}]}";
    ObjectNode schema1 = mapper.readValue(schemaString1, ObjectNode.class);
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"arr\",\"type\":[\"int\"]}");
    matchTypes(schema1, schema2);
    String expectedSchema = "{\"name\":\"arr\",\"type\":[\"int\",{\"name\":\"array\",\"type\":\"array\",\"items\":\"string\"}]}";
    assertEquals(expectedSchema, schema1.toString());
    assertEquals("{\"name\":\"arr\",\"type\":[\"int\"]}", schema2.toString());
  }

  @Test
  public void shouldMatchTypesRecord() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"arr\",\"type\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}]}";
    ObjectNode schema1 = mapper.readValue(schemaString1, ObjectNode.class);
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"name\":\"arr\",\"type\":[\"int\"]}");
    matchTypes(schema1, schema2);
    String expectedSchema = "{\"name\":\"arr\",\"type\":[\"int\",{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}]}";
    assertEquals(expectedSchema, schema1.toString());
    assertEquals("{\"name\":\"arr\",\"type\":[\"int\"]}", schema2.toString());
  }

  @Test
  public void shouldMatchRecordTypeWithRecord() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"revision\",\"type\":\"null\"}";
    String schemaString2 = "{\"name\":\"revision\",\"type\":{\"name\":\"revision\",\"type\":\"record\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}]}}";

    ObjectNode schema1 = mapper.readValue(schemaString1, ObjectNode.class);
    ObjectNode schema2 = (ObjectNode) mapper.readTree(schemaString2);

    matchRecordTypeWithRecord(schema1, schema2, false);
    String expectedSchema = "{\"name\":\"revision\",\"type\":[\"int\",\"null\"]}";
    assertEquals(expectedSchema, schema1.toString());
    assertEquals(schemaString2, schema2.toString());
  }

  @Test
  public void shouldMatchJsonArrayWithRecord() throws JsonProcessingException {

    // Field with name R is matched recursively to update field 'old' with branches : long and null
    String schemaString1 = "{\"name\":\"revision\",\"type\":[\"null\",{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}]}";
    String schemaString2 = "{\"name\":\"revision\",\"type\":{\"name\":\"revision\",\"fields\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\"]}]}],\"type\":\"record\"}}";

    ObjectNode schema1 = mapper.readValue(schemaString1, ObjectNode.class);
    ObjectNode schema2 = (ObjectNode) mapper.readTree(schemaString2);

    matchJsonArrayWithRecord(schema1, schema2, true);
    assertEquals(schemaString1, schema1.toString());
    String expectedSchema = "{\"name\":\"revision\",\"type\":{\"name\":\"revision\",\"fields\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}],\"type\":\"record\"}}";
    assertEquals(expectedSchema, schema2.toString());
  }

  @Test
  public void shouldMatchItems() throws JsonProcessingException {

    // Field with name R is matched recursively to update field 'old' with branches : long and null
    String schemaString1 = "{\"name\":\"revision\",\"items\":[\"null\",{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}]}";
    String schemaString2 = "{\"name\":\"revision\",\"items\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\"]}]}],\"type\":\"record\"}";

    ObjectNode schema1 = mapper.readValue(schemaString1, ObjectNode.class);
    ObjectNode schema2 = mapper.readValue(schemaString2, ObjectNode.class);

    matchItems(schema1, schema2, true);
    assertEquals(schemaString1, schema1.toString());
    String expectedSchema = "{\"name\":\"revision\",\"items\":[{\"name\":\"R\",\"type\":\"record\",\"fields\":[{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}],\"type\":\"record\"}";
    assertEquals(expectedSchema, schema2.toString());
  }

  @Test
  public void shouldCompressPrimitiveType() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"l\",\"type\":\"record\",\"fields\":[{\"name\":\"boolean\",\"type\":\"boolean\"}]}";
    ObjectNode schema1 = mapper.readValue(schemaString1, ObjectNode.class);
    compressRecordToUnion(schema1);
    assertEquals("{\"name\":\"l\",\"type\":[\"boolean\"]}", schema1.toString());
  }

  @Test
  public void shouldCompressTypeArray() throws JsonProcessingException {
    String schemaString1 = "{\"type\":\"array\",\"items\":{\"name\":\"l\",\"type\":\"null\"}}";
    ObjectNode schema1 = mapper.readValue(schemaString1, ObjectNode.class);
    compressRecordToUnion(schema1);
    assertEquals("{\"type\":\"array\",\"items\":\"null\"}", schema1.toString());
  }



}