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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModelProperty;

import java.io.IOException;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SchemaString {

  private String schemaString;
  private Integer maxId;

  public SchemaString() {

  }

  public SchemaString(String schemaString) {
    this.schemaString = schemaString;
  }

  public static SchemaString fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, SchemaString.class);
  }

  @ApiModelProperty(value = "Schema string identified by the ID")
  @JsonProperty("schema")
  public String getSchemaString() {
    return schemaString;
  }

  @JsonProperty("schema")
  public void setSchemaString(String schemaString) {
    this.schemaString = schemaString;
  }

  @JsonProperty("maxId")
  public Integer getMaxId() {
    return maxId;
  }

  @JsonProperty("maxId")
  public void setMaxId(Integer maxId) {
    this.maxId = maxId;
  }

  public String toJson() throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
