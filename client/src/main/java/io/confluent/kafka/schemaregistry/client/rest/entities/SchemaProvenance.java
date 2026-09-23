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
 * The provenance of every column across a range of a subject's versions: for each version, each
 * field's inlined index path and an id that is stable for as long as that field keeps its
 * identity at that location.
 *
 * <p>Versions are in ascending version order whatever order the request named them in, and each
 * carries its schema id, so a caller that asked by schema id finds its writer and reader by id
 * rather than by position — the writer may well be the newer of the two.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Column provenance across versions")
public class SchemaProvenance {

  private String subject;
  private List<ProvenanceVersion> versions;

  @JsonCreator
  public SchemaProvenance(@JsonProperty("subject") String subject,
                          @JsonProperty("versions") List<ProvenanceVersion> versions) {
    this.subject = subject;
    this.versions = versions;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.SUBJECT_DESC,
      example = Schema.SUBJECT_EXAMPLE)
  @JsonProperty("subject")
  public String getSubject() {
    return subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "The versions returned, in ascending "
      + "version order")
  @JsonProperty("versions")
  public List<ProvenanceVersion> getVersions() {
    return versions;
  }

  @JsonProperty("versions")
  public void setVersions(List<ProvenanceVersion> versions) {
    this.versions = versions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaProvenance that = (SchemaProvenance) o;
    return Objects.equals(subject, that.subject)
        && Objects.equals(versions, that.versions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subject, versions);
  }

  @Override
  public String toString() {
    return "{subject=" + subject + ",versions=" + versions + "}";
  }
}
