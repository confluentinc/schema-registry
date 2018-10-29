/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;
import javax.ws.rs.DefaultValue;

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
  private String schemaType = AvroSchema.AVRO;
  @NotEmpty
  private boolean deleted;

  public SchemaValue(@JsonProperty("subject") String subject,
                     @JsonProperty("version") Integer version,
                     @JsonProperty("id") Integer id,
                     @JsonProperty("schema") String schema,
                     @JsonProperty("deleted") boolean deleted) {
    this.subject = subject;
    this.version = version;
    this.id = id;
    this.schema = schema;
    this.deleted = deleted;
  }

  @JsonCreator
  public SchemaValue(@JsonProperty("subject") String subject,
                     @JsonProperty("version") Integer version,
                     @JsonProperty("id") Integer id,
                     @JsonProperty("schemaType") @DefaultValue("avro") String schemaType,
                     @JsonProperty("schema") String schema,
                     @JsonProperty("deleted") boolean deleted) {
    this.subject = subject;
    this.version = version;
    this.id = id;
    this.schemaType = schemaType;
    this.schema = schema;
    this.deleted = deleted;
  }

  public SchemaValue(Schema schemaEntity) {
    this.subject = schemaEntity.getSubject();
    this.version = schemaEntity.getVersion();
    this.id = schemaEntity.getId();
    this.schemaType = schemaEntity.getSchemaType();
    this.schema = schemaEntity.getSchema();
    this.deleted = false;
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

  @JsonProperty("schemaType")
  public String getSchemaType() {
    return this.schemaType;
  }

  @JsonProperty("schemaType")
  public void setSchemaType(String schemaType) {
    this.schemaType = schemaType;
  }

  @JsonProperty("schema")
  public String getSchema() {
    return this.schema;
  }

  @JsonProperty("schema")
  public void setSchema(String schema) {
    this.schema = schema;
  }

  @JsonProperty("deleted")
  public boolean isDeleted() {
    return deleted;
  }

  @JsonProperty("deleted")
  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
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
    if (!this.schemaType.equals(that.getSchemaType())) {
      return false;
    }
    if (!this.schema.equals(that.schema)) {
      return false;
    }
    if (deleted != that.deleted) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = subject.hashCode();
    result = 31 * result + version;
    result = 31 * result + id.intValue();
    result = 31 * result + schemaType.hashCode();
    result = 31 * result + schema.hashCode();
    result = 31 * result + (deleted ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{subject=" + this.subject + ",");
    sb.append("version=" + this.version + ",");
    sb.append("id=" + this.id + ",");
    sb.append("schemaType=" + this.schemaType + ",");
    sb.append("schema=" + this.schema + ",");
    sb.append("deleted=" + this.deleted + "}");
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
