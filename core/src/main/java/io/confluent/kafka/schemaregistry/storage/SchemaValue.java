/**
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;

public class SchemaValue implements Comparable<SchemaValue>, SchemaRegistryValue {

  @NotEmpty
  private String subject;
  @Min(1)
  private Integer version;
  @Min(0)
  private Integer id;
  @NotEmpty
  private String schema;

  public SchemaValue(@JsonProperty("subject") String subject,
                     @JsonProperty("version") Integer version,
                     @JsonProperty("id") Integer id,
                     @JsonProperty("schema") String schema) {
    this.subject = subject;
    this.version = version;
    this.id = id;
    this.schema = schema;
  }

  public SchemaValue(Schema schemaEntity) {
    this.subject = schemaEntity.getSubject();
    this.version = schemaEntity.getVersion();
    this.id = schemaEntity.getId();
    this.schema = schemaEntity.getSchema();
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

  @JsonProperty("id")
  public Integer getId() {
    return this.id;
  }

  @JsonProperty("id")
  public void setId(Integer id) {
    this.id = id;
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

    SchemaValue that = (SchemaValue) o;

    if (!this.subject.equals(that.subject)) {
      return false;
    }
    if (!this.version.equals(that.version)) {
      return false;
    }
    if (!this.id.equals(that.getId())) {
      return false;
    }
    if (!this.schema.equals(that.schema)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = subject.hashCode();
    result = 31 * result + version;
    result = 31 * result + id.intValue();
    result = 31 * result + schema.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{subject=" + this.subject + ",");
    sb.append("version=" + this.version + ",");
    sb.append("id=" + this.id + ",");
    sb.append("schema=" + this.schema + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(SchemaValue that) {
    int result = this.subject.compareTo(that.subject);
    if (result != 0) {
      return result;
    }
    result = this.version - that.version;
    return result;
  }
}
