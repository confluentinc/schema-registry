/*
 * Copyright 2015 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;

public class SchemaString {

  private String schemaString;

  public SchemaString() {

  }

  public SchemaString(String schemaString) {
    this.schemaString = schemaString;
  }

  public static SchemaString fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, SchemaString.class);
  }

  @JsonProperty("schema")
  public String getSchemaString() {
    return schemaString;
  }

  @JsonProperty("schema")
  public void setSchemaString(String schemaString) {
    this.schemaString = schemaString;
  }

  public String toJson(boolean pretty) throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }

  public String schemaStringToJson(boolean pretty) throws IOException {
    ObjectMapper om = new ObjectMapper();
    // Need to parse the schema first before applying indent feature
    JsonNode json = om.readValue(this.schemaString, JsonNode.class);
    return om.configure(SerializationFeature.INDENT_OUTPUT, pretty)
            .writeValueAsString(json);
  }
}
