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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTypeConverter;
import java.io.IOException;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Schema register response")
public class RegisterSchemaResponse {

  private int id;
  private Integer version;
  private String schemaType;
  private List<SchemaReference> references = null;
  private Metadata metadata = null;
  private RuleSet ruleSet = null;
  private String schema;

  public RegisterSchemaResponse() {
  }

  public RegisterSchemaResponse(int id) {
    this.id = id;
  }

  public RegisterSchemaResponse(Schema schema) {
    this.version = schema.getVersion() != null && schema.getVersion() > 0
        ? schema.getVersion()
        : null;
    this.id = schema.getId();
    this.schemaType = schema.getSchemaType();
    this.references = schema.getReferences();
    this.metadata = schema.getMetadata();
    this.ruleSet = schema.getRuleSet();
    this.schema = schema.getSchema();
  }

  public static RegisterSchemaResponse fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, RegisterSchemaResponse.class);
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.ID_DESC,
      example = Schema.ID_EXAMPLE)
  @JsonProperty("id")
  public int getId() {
    return id;
  }

  @JsonProperty("id")
  public void setId(int id) {
    this.id = id;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.VERSION_DESC)
  @JsonProperty("version")
  public Integer getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(Integer version) {
    this.version = version;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.TYPE_DESC)
  @JsonProperty("schemaType")
  @JsonSerialize(converter = SchemaTypeConverter.class)
  public String getSchemaType() {
    return this.schemaType;
  }

  @JsonProperty("schemaType")
  public void setSchemaType(String schemaType) {
    this.schemaType = schemaType;
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

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.METADATA_DESC)
  @JsonProperty("metadata")
  public Metadata getMetadata() {
    return this.metadata;
  }

  @JsonProperty("metadata")
  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.RULESET_DESC)
  @JsonProperty("ruleSet")
  public RuleSet getRuleSet() {
    return this.ruleSet;
  }

  @JsonProperty("ruleSet")
  public void setRuleSet(RuleSet ruleSet) {
    this.ruleSet = ruleSet;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.SCHEMA_DESC)
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
    RegisterSchemaResponse that = (RegisterSchemaResponse) o;
    return Objects.equals(version, that.version)
        && id == that.id
        && Objects.equals(schemaType, that.schemaType)
        && Objects.equals(references, that.references)
        && Objects.equals(metadata, that.metadata)
        && Objects.equals(ruleSet, that.ruleSet)
        && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaType, references, metadata, ruleSet, version, id, schema);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("{");
    if (version != null) {
      buf.append("version=").append(version).append(", ");
    }
    buf.append("id=").append(id).append(", ");
    buf.append("schemaType=").append(this.schemaType).append(", ");
    buf.append("references=").append(this.references).append(", ");
    buf.append("metadata=").append(this.metadata).append(", ");
    buf.append("ruleSet=").append(this.ruleSet).append(", ");
    buf.append("schema=").append(schema).append("}");
    return buf.toString();
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }

}
