/*
 * Copyright 2021 Confluent Inc.
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
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Schema reference")
public class SchemaReference implements Comparable<SchemaReference> {

  private String name;
  private String subject;
  private Integer version;

  @JsonCreator
  public SchemaReference(@JsonProperty("name") String name,
                         @JsonProperty("subject") String subject,
                         @JsonProperty("version") Integer version) {
    this.name = name;
    this.subject = subject;
    this.version = version;
  }

  @Schema(description = "Reference name")
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  @Schema(description = "Name of the referenced subject")
  @JsonProperty("subject")
  public String getSubject() {
    return subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

  @Schema(description = "Version number of the referenced subject")
  @JsonProperty("version")
  public Integer getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(Integer version) {
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaReference that = (SchemaReference) o;
    return Objects.equals(name, that.name)
        && Objects.equals(subject, that.subject)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, subject, version);
  }

  @Override
  public int compareTo(SchemaReference that) {
    int result = this.subject.compareTo(that.subject);
    if (result != 0) {
      return result;
    }
    result = this.version - that.version;
    return result;
  }

  @Override
  public String toString() {
    return "{"
        + "name='"
        + name
        + '\''
        + ", subject='"
        + subject
        + '\''
        + ", version="
        + version
        + '}';
  }
}
