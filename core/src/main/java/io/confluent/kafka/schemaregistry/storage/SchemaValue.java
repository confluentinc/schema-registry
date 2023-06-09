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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTypeConverter;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Min;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaValue extends SubjectValue implements Comparable<SchemaValue> {

  @Min(1)
  private Integer version;
  @Min(0)
  private Integer id;
  @NotEmpty
  private String schema;
  private String schemaType = AvroSchema.TYPE;
  private List<SchemaReference> references = Collections.emptyList();
  @NotEmpty
  private boolean deleted;

  @VisibleForTesting
  public SchemaValue(@JsonProperty("subject") String subject,
                     @JsonProperty("version") Integer version,
                     @JsonProperty("id") Integer id,
                     @JsonProperty("schema") String schema,
                     @JsonProperty("deleted") boolean deleted) {
    super(subject);
    this.version = version;
    this.id = id;
    this.schema = schema;
    this.deleted = deleted;
  }

  @JsonCreator
  public SchemaValue(@JsonProperty("subject") String subject,
                     @JsonProperty("version") Integer version,
                     @JsonProperty("id") Integer id,
                     @JsonProperty("schemaType") String schemaType,
                     @JsonProperty("references") List<SchemaReference> references,
                     @JsonProperty("schema") String schema,
                     @JsonProperty("deleted") boolean deleted) {
    super(subject);
    this.version = version;
    this.id = id;
    this.schemaType = schemaType != null ? schemaType : AvroSchema.TYPE;
    this.references = references != null ? references : Collections.emptyList();
    this.schema = schema;
    this.deleted = deleted;
  }

  public SchemaValue(Schema schemaEntity) {
    super(schemaEntity.getSubject());
    this.version = schemaEntity.getVersion();
    this.id = schemaEntity.getId();
    this.schemaType = schemaEntity.getSchemaType();
    List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> refs
        = schemaEntity.getReferences();
    this.references = refs == null ? null : refs.stream()
        .map(ref -> new SchemaReference(ref.getName(), ref.getSubject(), ref.getVersion()))
        .collect(Collectors.toList());
    this.schema = schemaEntity.getSchema();
    this.deleted = false;
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
    this.schemaType = schemaType != null ? schemaType : AvroSchema.TYPE;
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
    if (!super.equals(o)) {
      return false;
    }

    SchemaValue that = (SchemaValue) o;

    if (!this.version.equals(that.version)) {
      return false;
    }
    if (!this.id.equals(that.getId())) {
      return false;
    }
    if (schemaType != null ? !schemaType.equals(that.schemaType) : that.schemaType != null) {
      return false;
    }
    if (!this.references.equals(that.getReferences())) {
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
    int result = super.hashCode();
    result = 31 * result + version;
    result = 31 * result + id.intValue();
    result = 31 * result + (schemaType != null ? schemaType.hashCode() : 0);
    result = 31 * result + schema.hashCode();
    result = 31 * result + references.hashCode();
    result = 31 * result + (deleted ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{subject=" + this.getSubject() + ",");
    sb.append("version=" + this.version + ",");
    sb.append("id=" + this.id + ",");
    sb.append("schemaType=" + this.schemaType + ",");
    sb.append("references=" + this.references + ",");
    sb.append("schema=" + this.schema + ",");
    sb.append("deleted=" + this.deleted + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(SchemaValue that) {
    int result = this.getSubject().compareTo(that.getSubject());
    if (result != 0) {
      return result;
    }
    result = this.version - that.version;
    return result;
  }

  @Override
  public SchemaKey toKey() {
    return new SchemaKey(getSubject(), getVersion());
  }
}
