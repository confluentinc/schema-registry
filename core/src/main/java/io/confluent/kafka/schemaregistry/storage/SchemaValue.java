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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTypeConverter;

import java.util.Base64;
import java.util.Objects;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Min;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaValue extends SubjectValue implements Comparable<SchemaValue> {

  public static final String ENCODED_PROPERTY = "__enc__";

  @Min(1)
  private Integer version;
  @Min(0)
  private Integer id;
  private String md5;
  @NotEmpty
  private String schema;
  private String schemaType = AvroSchema.TYPE;
  private List<SchemaReference> references = Collections.emptyList();
  private Metadata metadata = null;
  private RuleSet ruleSet = null;
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

  @VisibleForTesting
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

  @JsonCreator
  public SchemaValue(@JsonProperty("subject") String subject,
                     @JsonProperty("version") Integer version,
                     @JsonProperty("id") Integer id,
                     @JsonProperty("md5") String md5,
                     @JsonProperty("schemaType") String schemaType,
                     @JsonProperty("references") List<SchemaReference> references,
                     @JsonProperty("metadata") Metadata metadata,
                     @JsonProperty("ruleSet") RuleSet ruleSet,
                     @JsonProperty("schema") String schema,
                     @JsonProperty("deleted") boolean deleted) {
    super(subject);
    this.version = version;
    this.id = id;
    this.md5 = md5;
    this.schemaType = schemaType != null ? schemaType : AvroSchema.TYPE;
    this.references = references != null ? references : Collections.emptyList();
    this.metadata = metadata;
    this.ruleSet = ruleSet;
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
        .map(SchemaReference::new)
        .collect(Collectors.toList());
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata metadata =
        schemaEntity.getMetadata();
    this.metadata = metadata != null ? new Metadata(metadata) : null;
    io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet ruleSet =
        schemaEntity.getRuleSet();
    this.ruleSet = ruleSet != null ? new RuleSet(ruleSet) : null;
    this.schema = schemaEntity.getSchema();
    this.deleted = false;
  }

  public SchemaValue(Schema schemaEntity, RuleSetHandler ruleSetHandler) {
    super(schemaEntity.getSubject());
    this.version = schemaEntity.getVersion();
    this.id = schemaEntity.getId();
    this.schemaType = schemaEntity.getSchemaType();
    List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> refs
        = schemaEntity.getReferences();
    this.references = refs == null ? null : refs.stream()
        .map(SchemaReference::new)
        .collect(Collectors.toList());
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata metadata =
        schemaEntity.getMetadata();
    this.metadata = metadata != null ? new Metadata(metadata) : null;
    this.ruleSet = ruleSetHandler.transform(schemaEntity.getRuleSet());
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

  @JsonProperty("md5")
  public String getMd5() {
    return this.md5;
  }

  @JsonProperty("md5")
  public void setMd5(String md5) {
    this.md5 = md5;
  }

  @JsonIgnore
  public byte[] getMd5Bytes() {
    return md5 != null ? Base64.getDecoder().decode(md5) : null;
  }

  @JsonIgnore
  public void setMd5Bytes(byte[] bytes) {
    md5 = bytes != null ? Base64.getEncoder().encodeToString(bytes) : null;
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

  @JsonProperty("metadata")
  public Metadata getMetadata() {
    return this.metadata;
  }

  @JsonProperty("metadata")
  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  @JsonProperty("ruleSet")
  public RuleSet getRuleSet() {
    return this.ruleSet;
  }

  @JsonProperty("ruleSet")
  public void setRuleSet(RuleSet ruleSet) {
    this.ruleSet = ruleSet;
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
    return deleted == that.deleted
        && Objects.equals(version, that.version)
        && Objects.equals(id, that.id)
        && Objects.equals(md5, that.md5)
        && Objects.equals(schema, that.schema)
        && Objects.equals(schemaType, that.schemaType)
        && Objects.equals(references, that.references)
        && Objects.equals(metadata, that.metadata)
        && Objects.equals(ruleSet, that.ruleSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), version, id, md5, schema, schemaType, references,
        metadata, ruleSet, deleted);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{subject=" + this.getSubject() + ",");
    sb.append("version=" + this.version + ",");
    sb.append("id=" + this.id + ",");
    sb.append("md5=" + this.md5 + ",");
    sb.append("schemaType=" + this.schemaType + ",");
    sb.append("references=" + this.references + ",");
    sb.append("metadata=" + this.metadata + ",");
    sb.append("ruleSet=" + this.ruleSet + ",");
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

  public Schema toSchemaEntity() {
    return new Schema(
        getSubject(),
        getVersion(),
        getId(),
        getSchemaType(),
        getReferences() == null ? null : getReferences().stream()
            .map(SchemaReference::toRefEntity)
            .collect(Collectors.toList()),
        getMetadata() == null ? null : getMetadata().toMetadataEntity(),
        getRuleSet() == null ? null : getRuleSet().toRuleSetEntity(),
        getSchema()
    );
  }
}