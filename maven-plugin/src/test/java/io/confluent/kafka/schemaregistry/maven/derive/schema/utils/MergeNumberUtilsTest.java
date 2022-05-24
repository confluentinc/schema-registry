package io.confluent.kafka.schemaregistry.maven.derive.schema.utils;

import org.json.JSONObject;
import org.junit.Test;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeNumberUtils.*;
import static org.junit.Assert.assertEquals;

public class MergeNumberUtilsTest {

  private void assertNoChange(String schemaString1, String schemaString2) {
    JSONObject schema1 = new JSONObject(schemaString1);
    JSONObject schema2 = new JSONObject(schemaString2);
    adjustNumberTypes(schema1, schema2);
    assertEquals(schemaString1, schema1.toString());
    assertEquals(schemaString2, schema2.toString());
  }

  private void assertChange(String schemaString1, String schemaString2, String ans) {
    JSONObject schema1 = new JSONObject(schemaString1);
    JSONObject schema2 = new JSONObject(schemaString2);
    adjustNumberTypes(schema1, schema2);
    assertEquals(ans, schema1.toString());
    assertEquals(ans, schema2.toString());
  }


  @Test
  public void shouldMakeNoChangeDifferentNamePrimitiveType() {
    String schemaString1 = "{\"name\":\"K\",\"type\":\"double\"}";
    String schemaString2 = "{\"name\":\"J\",\"type\":\"long\"}";
    assertNoChange(schemaString1, schemaString2);
  }

  @Test
  public void shouldMakeNoChangeDifferentNameRecordType() {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    String schemaString2 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"long\"}]}";
    assertNoChange(schemaString1, schemaString2);
  }

  @Test
  public void shouldMakeNoChangeDifferentNameArrayType() {
    String schemaString1 = "{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}";
    String schemaString2 = "{\"name\":\"J\",\"type\":{\"type\":\"array\",\"items\":\"double\"}}";
    assertNoChange(schemaString1, schemaString2);
  }

  @Test
  public void shouldMakeChangePrimitiveType() {
    String schemaString1 = "{\"type\":\"long\"}";
    String schemaString2 = "{\"type\":\"int\"}";
    assertChange(schemaString1, schemaString2, schemaString1);
  }

  @Test
  public void shouldMakeChangePrimitiveType2() {
    String schemaString1 = "{\"type\":\"long\"}";
    String schemaString2 = "{\"type\":\"double\"}";
    assertChange(schemaString1, schemaString2, schemaString2);
  }


  @Test
  public void shouldMakeChangeNameProvidedPrimitiveType() {
    String schemaString1 = "{\"name\":\"K\",\"type\":\"double\"}";
    String schemaString2 = "{\"name\":\"K\",\"type\":\"int\"}";
    assertChange(schemaString1, schemaString2, schemaString1);
  }

  @Test
  public void shouldMakeChangeRecordType() {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    String schemaString2 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"double\"}]}";
    assertChange(schemaString1, schemaString2, schemaString2);
  }

  @Test
  public void shouldMakeChangeRecordTypeMultipleFields() {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"},{\"name\":\"B\",\"type\":\"double\"}]}";
    String schemaString2 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"double\"},{\"name\":\"B\",\"type\":\"long\"}]}";
    String expectedOutput = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"double\"},{\"name\":\"B\",\"type\":\"double\"}]}";
    assertChange(schemaString1, schemaString2, expectedOutput);
  }


  @Test
  public void shouldMakeChangeArray() {
    String schemaString1 = "{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}";
    String schemaString2 = "{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"double\"}}";
    assertChange(schemaString1, schemaString2, schemaString2);
  }

  @Test
  public void shouldMakeChangeRecordWithArray() {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}";
    String schemaString2 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"long\"}}]}";
    assertChange(schemaString1, schemaString2, schemaString2);
  }

}