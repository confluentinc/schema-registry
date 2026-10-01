/*
 * Copyright 2026 Confluent Inc.
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

/**
 * One version of a {@link SchemaProvenance}: its version number, its schema id, and its fields in
 * path order — lexicographic on the index paths, so a prefix precedes its extensions.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "One version's column provenance")
public class ProvenanceVersion {

  private Integer version;
  private Integer id;
  private List<ProvenanceField> fields;

  @JsonCreator
  public ProvenanceVersion(@JsonProperty("version") Integer version,
                           @JsonProperty("id") Integer id,
                           @JsonProperty("fields") List<ProvenanceField> fields) {
    this.version = version;
    this.id = id;
    this.fields = fields;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.VERSION_DESC,
      example = Schema.VERSION_EXAMPLE)
  @JsonProperty("version")
  public Integer getVersion() {
    return version;
  }

  @JsonProperty("version")
  public void setVersion(Integer version) {
    this.version = version;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.ID_DESC,
      example = Schema.ID_EXAMPLE)
  @JsonProperty("id")
  public Integer getId() {
    return id;
  }

  @JsonProperty("id")
  public void setId(Integer id) {
    this.id = id;
  }

  /**
   * The fields in path order. Always present, empty for a schema with no fields, rather than
   * omitted the way this entity omits other empty values.
   */
  @io.swagger.v3.oas.annotations.media.Schema(description = "The version's fields, in path "
      + "order")
  @JsonInclude(JsonInclude.Include.ALWAYS)
  @JsonProperty("fields")
  public List<ProvenanceField> getFields() {
    return fields;
  }

  @JsonProperty("fields")
  public void setFields(List<ProvenanceField> fields) {
    this.fields = fields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProvenanceVersion that = (ProvenanceVersion) o;
    return Objects.equals(version, that.version)
        && Objects.equals(id, that.id)
        && Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, id, fields);
  }

  @Override
  public String toString() {
    return "{version=" + version + ",id=" + id + ",fields=" + fields + "}";
  }
}
