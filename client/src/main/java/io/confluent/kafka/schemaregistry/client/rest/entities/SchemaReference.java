/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import java.util.Objects;

public class SchemaReference implements Comparable<SchemaReference> {

  private String name;
  private String subject;
  @Min(1)
  private Integer version;

  @JsonCreator
  public SchemaReference(@JsonProperty("name") String name,
                         @JsonProperty("subject") String subject,
                         @JsonProperty("version") Integer version) {
    this.name = name;
    this.subject = subject;
    this.version = version;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("subject")
  public String getSubject() {
    return subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

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
