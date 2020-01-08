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

package io.confluent.kafka.schemaregistry.client.rest.entities.requests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RegisterSchemaRequest {

  private Integer version;
  private Integer id;
  private String schemaType;
  private List<SchemaReference> references = null;
  private String schema;

  public static RegisterSchemaRequest fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, RegisterSchemaRequest.class);
  }

  @JsonProperty("version")
  public Integer getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(Integer version) {
    this.version = version;
  }

  @JsonProperty("id")
  public Integer getId() {
    return this.id;
  }

  @JsonProperty("id")
  public void setId(Integer id) {
    this.id = id;
  }

  @JsonProperty("schemaType")
  public String getSchemaType() {
    return this.schemaType;
  }

  @JsonProperty("schemaType")
  public void setSchemaType(String schemaType) {
    this.schemaType = schemaType;
  }

  @JsonProperty("references")
  public List<SchemaReference> getReferences() {
    return this.references;
  }

  @JsonProperty("references")
  public void setReferences(List<SchemaReference> references) {
    this.references = references;
  }

  @JsonProperty("schema")
  public String getSchema() {
    return this.schema;
  }

  @JsonProperty("schema")
  public void setSchema(String schema) {
    this.schema = schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RegisterSchemaRequest that = (RegisterSchemaRequest) o;
    return Objects.equals(version, that.version)
        && Objects.equals(id, that.id)
        && Objects.equals(schemaType, that.schemaType)
        && Objects.equals(references, that.references)
        && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaType, references, version, id, schema);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("{");
    if (version != null) {
      buf.append("version=").append(version).append(", ");
    }
    if (id != null) {
      buf.append("id=").append(id).append(", ");
    }
    buf.append("schemaType=").append(this.schemaType).append(",");
    buf.append("references=").append(this.references).append(",");
    buf.append("schema=").append(schema).append("}");
    return buf.toString();
  }

  public String toJson() throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }

}
