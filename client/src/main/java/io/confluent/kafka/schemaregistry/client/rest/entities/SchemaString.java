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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Schema definition")
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

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.TYPE_DESC)
  @JsonProperty("schemaType")
  @JsonSerialize(converter = SchemaTypeConverter.class)
  public String getSchemaType() {
    return schemaType;
  }

  @JsonProperty("schemaType")
  public void setSchemaType(String schemaType) {
    this.schemaType = schemaType;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Schema string identified by the ID")
  @JsonProperty("schema")
  public String getSchemaString() {
    return schemaString;
  }

  @JsonProperty("schema")
  public void setSchemaString(String schemaString) {
    this.schemaString = schemaString;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.REFERENCES_DESC)
  @JsonProperty("references")
  public List<SchemaReference> getReferences() {
    return this.references;
  }

  @JsonProperty("references")
  public void setReferences(List<SchemaReference> references) {
    this.references = references;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Maximum ID")
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaString that = (SchemaString) o;
    return Objects.equals(schemaType, that.schemaType)
        && Objects.equals(schemaString, that.schemaString)
        && Objects.equals(references, that.references)
        && Objects.equals(maxId, that.maxId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaType, schemaString, references, maxId);
  }
}
