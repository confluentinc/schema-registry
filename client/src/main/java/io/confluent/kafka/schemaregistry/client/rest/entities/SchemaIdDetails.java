/*
 * Copyright 2018 Confluent Inc.
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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class SchemaIdDetails {

  @JsonProperty("subjects")
  public Map<String, Integer> getSubjects() {
    return subjects;
  }

  @JsonProperty("subjects")
  public void setSubjects(Map<String, Integer> subjects) {
    this.subjects = subjects;
  }

  private Map<String, Integer> subjects;

  @JsonProperty("id")
  public int getId() {
    return id;
  }

  @JsonProperty("id")
  public void setId(int id) {
    this.id = id;
  }

  private int id;

  @JsonProperty("schema")
  public String getSchema() {
    return schema;
  }

  @JsonProperty("schema")
  public void setSchema(String schema) {
    this.schema = schema;
  }

  private String schema;

  public SchemaIdDetails(@JsonProperty("subject") Map<String, Integer> subjects,
                         @JsonProperty("id") Integer id,
                         @JsonProperty("schema") String schema) {
    this.subjects = subjects;
    this.id = id;
    this.schema = schema;
  }

  public String toJson() throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
