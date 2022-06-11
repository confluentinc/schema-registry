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

package io.confluent.kafka.schemaregistry.maven.derive.schema.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeNumberUtils.*;
import static org.junit.Assert.assertEquals;

public class MergeNumberUtilsTest {

  private final ObjectMapper mapper = new ObjectMapper();

  private void assertNoChange(String schemaString1, String schemaString2) throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree(schemaString1);
    ObjectNode schema2 = (ObjectNode) mapper.readTree(schemaString2);
    adjustNumberTypes(schema1, schema2);
    assertEquals(schemaString1, schema1.toString());
    assertEquals(schemaString2, schema2.toString());
  }

  private void assertChange(String schemaString1, String schemaString2, String expectedSchema)
      throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree(schemaString1);
    ObjectNode schema2 = (ObjectNode) mapper.readTree(schemaString2);
    adjustNumberTypes(schema1, schema2);
    assertEquals(expectedSchema, schema1.toString());
    assertEquals(expectedSchema, schema2.toString());
  }

  @Test
  public void shouldMakeNoChangeDifferentNamePrimitiveType() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"K\",\"type\":\"double\"}";
    String schemaString2 = "{\"name\":\"J\",\"type\":\"long\"}";
    assertNoChange(schemaString1, schemaString2);
  }

  @Test
  public void shouldMakeNoChangeDifferentNameRecordType() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    String schemaString2 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"long\"}]}";
    assertNoChange(schemaString1, schemaString2);
  }

  @Test
  public void shouldMakeNoChangeDifferentNameArrayType() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}";
    String schemaString2 = "{\"name\":\"J\",\"type\":{\"type\":\"array\",\"items\":\"double\"}}";
    assertNoChange(schemaString1, schemaString2);
  }

  @Test
  public void shouldMakeChangePrimitiveType() throws JsonProcessingException {
    String schemaString1 = "{\"type\":\"long\"}";
    String schemaString2 = "{\"type\":\"int\"}";
    assertChange(schemaString1, schemaString2, schemaString1);
  }

  @Test
  public void shouldMakeChangePrimitiveType2() throws JsonProcessingException {
    String schemaString1 = "{\"type\":\"long\"}";
    String schemaString2 = "{\"type\":\"double\"}";
    assertChange(schemaString1, schemaString2, schemaString2);
  }

  @Test
  public void shouldMakeChangeNameProvidedPrimitiveType() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"K\",\"type\":\"double\"}";
    String schemaString2 = "{\"name\":\"K\",\"type\":\"int\"}";
    assertChange(schemaString1, schemaString2, schemaString1);
  }

  @Test
  public void shouldMakeChangeRecordType() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    String schemaString2 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"double\"}]}";
    assertChange(schemaString1, schemaString2, schemaString2);
  }

  @Test
  public void shouldMakeChangeRecordTypeMultipleFields() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"},{\"name\":\"B\",\"type\":\"double\"}]}";
    String schemaString2 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"double\"},{\"name\":\"B\",\"type\":\"long\"}]}";
    String expectedOutput = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"double\"},{\"name\":\"B\",\"type\":\"double\"}]}";
    assertChange(schemaString1, schemaString2, expectedOutput);
  }

  @Test
  public void shouldMakeChangeArray() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}";
    String schemaString2 = "{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"double\"}}";
    assertChange(schemaString1, schemaString2, schemaString2);
  }

  @Test
  public void shouldMakeChangeRecordWithArray() throws JsonProcessingException {
    String schemaString1 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}";
    String schemaString2 = "{\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"long\"}}]}";
    assertChange(schemaString1, schemaString2, schemaString2);
  }

}