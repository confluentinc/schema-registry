/*
 * Copyright 2024 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "ExtendedSchema")
public class ExtendedSchema extends Schema {

  public static final String ALIASES_DESC = "Aliases for the given subject";

  public static final String ASSOCIATIONS_DESC = "Associations for the given subject";

  private List<String> aliases;
  private List<Association> associations;

  @JsonCreator
  public ExtendedSchema(@JsonProperty("subject") String subject,
      @JsonProperty("version") Integer version,
      @JsonProperty("id") Integer id,
      @JsonProperty("guid") String guid,
      @JsonProperty("schemaType") String schemaType,
      @JsonProperty("references") List<SchemaReference> references,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("ruleset") RuleSet ruleSet,
      @JsonProperty("schema") String schema,
      @JsonProperty("ts") Long timestamp,
      @JsonProperty("deleted") Boolean deleted,
      @JsonProperty("aliases") List<String> aliases,
      @JsonProperty("associations") List<Association> associations) {
    super(subject, version, id, guid, schemaType, references, metadata, ruleSet, schema,
        null, timestamp, deleted);
    this.aliases = aliases;
    this.associations = associations;
  }

  public ExtendedSchema(Schema schema, List<String> aliases, List<Association> associations) {
    super(schema.getSubject(), schema.getVersion(), schema.getId(), schema.getGuid(),
        schema.getSchemaType(), schema.getReferences(), schema.getMetadata(), schema.getRuleSet(),
        schema.getSchema(), schema.getSchemaTags(), schema.getTimestamp(), schema.getDeleted());
    this.aliases = aliases;
    this.associations = associations;
  }

  public ExtendedSchema copy() {
    return new ExtendedSchema(this, this.aliases, this.associations);
  }

  public ExtendedSchema copy(List<Association> associations) {
    return new ExtendedSchema(this, this.aliases, associations);
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = ALIASES_DESC)
  @JsonProperty("aliases")
  public List<String> getAliases() {
    return aliases;
  }

  @JsonProperty("aliases")
  public void setAliases(List<String> aliases) {
    this.aliases = aliases;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = ASSOCIATIONS_DESC)
  @JsonProperty("associations")
  public List<Association> getAssociations() {
    return associations;
  }

  @JsonProperty("associations")
  public void setAssociations(List<Association> associations) {
    this.associations = associations;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ExtendedSchema that = (ExtendedSchema) o;
    return Objects.equals(aliases, that.aliases)
        && Objects.equals(associations, that.associations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), aliases, associations);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{subject=" + getSubject() + ",");
    sb.append("version=" + getVersion() + ",");
    sb.append("id=" + getId() + ",");
    sb.append("guid=" + getGuid() + ",");
    sb.append("schemaType=" + getSchemaType() + ",");
    sb.append("references=" + getReferences() + ",");
    sb.append("metadata=" + getMetadata() + ",");
    sb.append("ruleSet=" + getRuleSet() + ",");
    sb.append("schema=" + getSchema() + ",");
    sb.append("schemaTags=" + getSchemaTags() + ",");
    sb.append("aliases=" + getAliases() + ",");
    sb.append("associations=" + getAssociations() + "}");
    return sb.toString();
  }


}
