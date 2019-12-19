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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.StdConverter;

import javax.ws.rs.DefaultValue;

import java.util.Collections;
import java.util.List;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Schema implements Comparable<Schema> {

  private String subject;
  private Integer version;
  private Integer id;
  private String schemaType = AvroSchema.TYPE;
  private List<SchemaReference> references = Collections.emptyList();
  private String schema;

  @JsonCreator
  public Schema(@JsonProperty("subject") String subject,
                @JsonProperty("version") Integer version,
                @JsonProperty("id") Integer id,
                @JsonProperty("schemaType") @DefaultValue("AVRO") String schemaType,
                @JsonProperty("references") List<SchemaReference> references,
                @JsonProperty("schema") String schema) {
    this.subject = subject;
    this.version = version;
    this.id = id;
    this.schemaType = schemaType;
    this.references = references != null ? references : Collections.emptyList();
    this.schema = schema;
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
  @JsonSerialize(converter = SchemaTypeConverter.class)
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

    Schema that = (Schema) o;

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
    if (!this.references.equals(that.getReferences())) {
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
    result = 31 * result + schemaType.hashCode();
    result = 31 * result + references.hashCode();
    result = 31 * result + schema.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{subject=" + this.subject + ",");
    sb.append("version=" + this.version + ",");
    sb.append("id=" + this.id + ",");
    sb.append("schemaType=" + this.schemaType + ",");
    sb.append("references=" + this.references + ",");
    sb.append("schema=" + this.schema + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(Schema that) {
    int result = this.subject.compareTo(that.subject);
    if (result != 0) {
      return result;
    }
    result = this.version - that.version;
    return result;
  }

  static class SchemaTypeConverter extends StdConverter<String, String> {
    @Override
    public String convert(final String value) {
      return AvroSchema.TYPE.equals(value) ? null : value;
    }
  }
}
