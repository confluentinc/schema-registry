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
import org.everit.json.schema.loader.SchemaClient;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.util.Comparator;

public class JsonNodeComparator implements Comparator<JsonNode> {

  private static final JsonSchemaComparator jsonSchemaComparator = new JsonSchemaComparator();
  private static final ObjectMapper objectMapper = Jackson.newObjectMapper();

  private final SchemaClient schemaClient;

  public JsonNodeComparator() {
    this(null);
  }

  // Client used to load the compared schemas; null means the loader default
  public JsonNodeComparator(SchemaClient schemaClient) {
    this.schemaClient = schemaClient;
  }

  @Override
  public int compare(JsonNode o1, JsonNode o2) {
    if (o1 == o2) {
      return 0;
    }
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
      Schema schema1 = load(o1);
      Schema schema2 = load(o2);
      return jsonSchemaComparator.compare(schema1, schema2);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Cannot parse Json Schema from JsonNode", e);
    }
  }

  private Schema load(JsonNode node) throws JsonProcessingException {
    SchemaLoader.SchemaLoaderBuilder builder = SchemaLoader.builder()
        .schemaJson(objectMapper.readValue(node.toString(), JSONObject.class));
    if (schemaClient == null) {
      return builder.build().load().build();
    }
    try {
      return builder.schemaClient(schemaClient).build().load().build();
    } catch (RuntimeException e) {
      if (JsonSchema.isRemoteRefBlocked(e)) {
        throw new IllegalArgumentException(JsonSchema.REMOTE_REF_DISABLED_MESSAGE, e);
      }
      throw e;
    }
  }
}
