/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import io.confluent.kafka.schemaregistry.json.jackson.Jackson;

public class JsonSchemaUtils {

  private static final Object NONE_MARKER = new Object();

  private static final ObjectMapper jsonMapper = Jackson.newObjectMapper();

  static final String ENVELOPE_SCHEMA_FIELD_NAME = "schema";
  static final String ENVELOPE_PAYLOAD_FIELD_NAME = "payload";

  public static ObjectNode envelope(JsonNode schema, JsonNode payload) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    result.set(ENVELOPE_SCHEMA_FIELD_NAME, schema);
    result.set(ENVELOPE_PAYLOAD_FIELD_NAME, payload);
    return result;
  }

  public static JsonSchema copyOf(JsonSchema schema) {
    return JsonSchema.copy(schema);
  }

  public static JsonSchema getSchema(Object object) {
    if (object == null) {
      return null;
    }
    if (object instanceof JsonNode) {
      JsonNode jsonValue = (JsonNode) object;
      if (jsonValue.isObject() && jsonValue.has(ENVELOPE_SCHEMA_FIELD_NAME)) {
        return new JsonSchema(jsonValue.get(ENVELOPE_SCHEMA_FIELD_NAME).toString());
      }
    }
    JsonSchemaConfig config = JsonSchemaConfig.nullableJsonSchemaDraft4();  // allow nulls
    JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(jsonMapper, config);
    JsonNode jsonSchema = jsonSchemaGenerator.generateJsonSchema(object.getClass());
    return new JsonSchema(jsonSchema.toString());
  }

  public static Object getValue(Object object) {
    if (object == null) {
      return null;
    }
    if (object instanceof JsonNode) {
      JsonNode jsonValue = (JsonNode) object;
      if (jsonValue.isObject() && jsonValue.has(ENVELOPE_PAYLOAD_FIELD_NAME)) {
        return jsonValue.get(ENVELOPE_PAYLOAD_FIELD_NAME);
      }
    }
    return object;
  }

  public static Object toObject(JsonNode value, JsonSchema schema) throws IOException {
    schema.validate(value);
    return value;
  }

  public static byte[] toJson(Object value) throws IOException {
    if (value == null) {
      return null;
    }
    StringWriter out = new StringWriter();
    jsonMapper.writeValue(out, value);
    String jsonString = out.toString();
    return jsonString.getBytes(StandardCharsets.UTF_8);
  }
}