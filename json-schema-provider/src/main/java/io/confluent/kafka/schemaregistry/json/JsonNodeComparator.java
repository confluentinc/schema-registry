/*
 * Copyright 2023 Confluent Inc.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.util.Comparator;

public class JsonNodeComparator implements Comparator<JsonNode> {

  private static final JsonSchemaComparator jsonSchemaComparator = new JsonSchemaComparator();
  private static final ObjectMapper objectMapper = Jackson.newObjectMapper();

  @Override
  public int compare(JsonNode o1, JsonNode o2) {
    if (o1 == null) {
      if (o2 != null) {
        return -1;
      } else {
        return 0;
      }
    } else if (o2 == null) {
      return 1;
    }

    try {
      Schema schema1 = SchemaLoader.builder()
          .schemaJson(objectMapper.readValue(o1.toString(), JSONObject.class))
          .build().load().build();
      Schema schema2 = SchemaLoader.builder()
          .schemaJson(objectMapper.readValue(o2.toString(), JSONObject.class))
          .build().load().build();
      return jsonSchemaComparator.compare(schema1, schema2);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Cannot parse Json Schema from JsonNode", e);
    }
  }
}
