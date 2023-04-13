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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public abstract class DeriveSchemaTest {

  protected DeriveSchema derive;
  protected final ObjectMapper mapper = new ObjectMapper();
  static final String TYPE_NUMBER = "{\"type\":\"number\"}";
  static final String TYPE_BOOLEAN = "{\"type\":\"boolean\"}";
  static final String TYPE_STRING = "{\"type\":\"string\"}";
  static final String TYPE_NULL = "{\"type\":\"null\"}";
  static final String EMPTY_ARRAY = "{\"type\":\"array\",\"items\":{}}";
  static final String ARRAY_OF_NUMBERS = "{\"type\":\"array\",\"items\":{\"type\":\"number\"}}";
  static final String ARRAY_OF_NUMBERS_AND_STRINGS = "{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"number\"},{\"type\":\"string\"}]}}";
  static final String ARRAY_OF_ARRAY_OF_NUMBERS = "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}}";
  static final String ARRAY_OF_BOOLEAN_NUMBERS_AND_STRINGS = "{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"boolean\"},{\"type\":\"number\"},{\"type\":\"string\"}]}}";
  static final String RECORD_WITH_STRING = "{\"type\":\"object\",\"properties\":{\"%s\":{\"type\":\"string\"}}}";
  static final String RECORD_WITH_ARRAY_OF_STRINGS = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}}";

  protected void generateSchemaAndCheckPrimitive(String message, String expectedSchema)
      throws JsonProcessingException {
    Optional<JsonNode> primitiveSchema = derive.getPrimitiveSchema(mapper.readTree(message));
    assert primitiveSchema.isPresent();
    assertEquals(primitiveSchema.get(), mapper.readTree(expectedSchema));
  }

  protected void generateSchemaAndCheckPrimitiveAbsent(String message)
      throws JsonProcessingException {
    Optional<JsonNode> primitiveSchema = derive.getPrimitiveSchema(mapper.readTree(message));
    assert !primitiveSchema.isPresent();
  }

  protected void generateSchemaAndCheckExpected(String message, String expectedSchema)
      throws IOException {
    ObjectNode schemaForRecord = derive.getSchemaForRecord((ObjectNode) mapper.readTree(message));
    JsonNode schema = derive.convertToFormat(schemaForRecord, "Schema");
    matchAndValidate(message, schema, expectedSchema);
  }

  protected abstract void matchAndValidate(String message, JsonNode schemaString, String expectedSchema)
      throws IOException;
}
