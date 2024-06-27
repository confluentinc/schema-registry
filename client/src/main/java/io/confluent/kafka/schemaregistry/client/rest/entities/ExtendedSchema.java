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

  private List<String> aliases;

  @JsonCreator
  public ExtendedSchema(@JsonProperty("subject") String subject,
                @JsonProperty("version") Integer version,
                @JsonProperty("id") Integer id,
                @JsonProperty("schemaType") String schemaType,
                @JsonProperty("references") List<SchemaReference> references,
                @JsonProperty("metadata") Metadata metadata,
                @JsonProperty("ruleset") RuleSet ruleSet,
                @JsonProperty("schema") String schema,
                @JsonProperty("aliases") List<String> aliases) {
    super(subject, version, id, schemaType, references, metadata, ruleSet, schema);
    this.aliases = aliases;
  }

  public ExtendedSchema(Schema schema, List<String> aliases) {
    super(schema.getSubject(), schema.getVersion(), schema.getId(), schema.getSchemaType(),
        schema.getReferences(), schema.getMetadata(), schema.getRuleSet(), schema.getSchema());
    this.aliases = aliases;
  }

  public ExtendedSchema copy() {
    return new ExtendedSchema(this, aliases);
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
    return Objects.equals(aliases, that.aliases);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), aliases);
  }
}
