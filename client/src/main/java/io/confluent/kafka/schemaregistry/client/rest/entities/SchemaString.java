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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.annotations.ApiModelProperty;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class SchemaString {

  private String schemaType = AvroSchema.TYPE;
  private String schemaString;
  private List<SchemaReference> references = Collections.emptyList();
  private Integer maxId;

  public SchemaString() {
  }

  // Visible for testing
  public SchemaString(String schemaString) {
    this.schemaString = schemaString;
  }

  public static SchemaString fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, SchemaString.class);
  }

  @ApiModelProperty(value = "Schema type")
  @JsonProperty("schemaType")
  @JsonSerialize(converter = SchemaTypeConverter.class)
  public String getSchemaType() {
    return schemaType;
  }

  @JsonProperty("schemaType")
  public void setSchemaType(String schemaType) {
    this.schemaType = schemaType;
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

  @ApiModelProperty(value = "Schema references")
  @JsonProperty("references")
  public List<SchemaReference> getReferences() {
    return this.references;
  }

  @JsonProperty("references")
  public void setReferences(List<SchemaReference> references) {
    this.references = references;
  }

  @ApiModelProperty(value = "Maximum ID")
  @JsonProperty("maxId")
  public Integer getMaxId() {
    return maxId;
  }

  @JsonProperty("maxId")
  public void setMaxId(Integer maxId) {
    this.maxId = maxId;
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }
}
