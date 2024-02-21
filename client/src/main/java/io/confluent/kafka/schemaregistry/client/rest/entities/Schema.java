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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Schema")
public class Schema implements Comparable<Schema> {

  public static final String SUBJECT_DESC = "Name of the subject";
  public static final String SUBJECT_EXAMPLE = "User";

  public static final String VERSION_DESC = "Version number";
  public static final String VERSION_EXAMPLE = "1";

  public static final String ID_DESC = "Globally unique identifier of the schema";
  public static final String ID_EXAMPLE = "100001";

  public static final String TYPE_DESC = "Schema type";
  public static final String TYPE_EXAMPLE = AvroSchema.TYPE;

  public static final String REFERENCES_DESC = "References to other schemas";
  public static final String METADATA_DESC = "User-defined metadata";
  public static final String RULESET_DESC = "Schema rule set";

  public static final String SCHEMA_DESC = "Schema definition string";
  public static final String SCHEMA_EXAMPLE = "{\"schema\": \"{\"type\": \"string\"}\"}";

  private String subject;
  private Integer version;
  private Integer id;
  private String schemaType;
  private List<SchemaReference> references;
  private Metadata metadata;
  private RuleSet ruleSet;
  private String schema;

  @JsonCreator
  public Schema(@JsonProperty("subject") String subject,
                @JsonProperty("version") Integer version,
                @JsonProperty("id") Integer id,
                @JsonProperty("schemaType") String schemaType,
                @JsonProperty("references") List<SchemaReference> references,
                @JsonProperty("metadata") Metadata metadata,
                @JsonProperty("ruleset") RuleSet ruleSet,
                @JsonProperty("schema") String schema) {
    this.subject = subject;
    this.version = version;
    this.id = id;
    this.schemaType = schemaType != null ? schemaType : AvroSchema.TYPE;
    this.references = references != null ? references : Collections.emptyList();
    this.metadata = metadata;
    this.ruleSet = ruleSet;
    this.schema = schema;
  }

  public Schema(@JsonProperty("subject") String subject,
      @JsonProperty("version") Integer version,
      @JsonProperty("id") Integer id,
      @JsonProperty("schemaType") String schemaType,
      @JsonProperty("references") List<SchemaReference> references,
      @JsonProperty("schema") String schema) {
    this.subject = subject;
    this.version = version;
    this.id = id;
    this.schemaType = schemaType != null ? schemaType : AvroSchema.TYPE;
    this.references = references != null ? references : Collections.emptyList();
    this.metadata = null;
    this.ruleSet = null;
    this.schema = schema;
  }

  public Schema(String subject, SchemaMetadata schemaMetadata) {
    this.subject = subject;
    this.version = schemaMetadata.getVersion();
    this.id = schemaMetadata.getId();
    this.schemaType = schemaMetadata.getSchemaType() != null
        ? schemaMetadata.getSchemaType() : AvroSchema.TYPE;
    this.references = schemaMetadata.getReferences() != null
        ? schemaMetadata.getReferences() : Collections.emptyList();
    this.metadata = schemaMetadata.getMetadata();
    this.ruleSet = schemaMetadata.getRuleSet();
    this.schema = schemaMetadata.getSchema();
  }

  public Schema(String subject, Integer version, Integer id, SchemaString schemaString) {
    this.subject = subject;
    this.version = version;
    this.id = id;
    this.schemaType = schemaString.getSchemaType() != null
        ? schemaString.getSchemaType() : AvroSchema.TYPE;
    this.references = schemaString.getReferences() != null
        ? schemaString.getReferences() : Collections.emptyList();
    this.metadata = schemaString.getMetadata();
    this.ruleSet = schemaString.getRuleSet();
    this.schema = schemaString.getSchemaString();
  }

  public Schema(String subject, Integer version, Integer id, ParsedSchema schema) {
    this.subject = subject;
    this.version = version;
    this.id = id;
    this.schemaType = schema.schemaType() != null
        ? schema.schemaType() : AvroSchema.TYPE;
    this.references = schema.references() != null
        ? schema.references() : Collections.emptyList();
    this.metadata = schema.metadata();
    this.ruleSet = schema.ruleSet();
    this.schema = schema.canonicalString();
  }

  public Schema(String subject, Integer version, Integer id) {
    this.subject = subject;
    this.version = version;
    this.id = id;
  }

  public Schema(String subject, Integer id) {
    this.subject = subject;
    this.id = id;
  }

  public Schema(String subject, RegisterSchemaRequest request) {
    this.subject = subject;
    this.version = request.getVersion() != null ? request.getVersion() : 0;
    this.id = request.getId() != null ? request.getId() : -1;
    this.schemaType = request.getSchemaType() != null
        ? request.getSchemaType() : AvroSchema.TYPE;
    this.references = request.getReferences() != null
        ? request.getReferences() : Collections.emptyList();
    this.metadata = request.getMetadata();
    this.ruleSet = request.getRuleSet();
    this.schema = request.getSchema();
  }

  public Schema(String subject, RegisterSchemaResponse response) {
    this.subject = subject;
    this.version = response.getVersion() != null ? response.getVersion() : 0;
    this.id = response.getId();
    this.schemaType = response.getSchemaType() != null
        ? response.getSchemaType() : AvroSchema.TYPE;
    this.references = response.getReferences() != null
        ? response.getReferences() : Collections.emptyList();
    this.metadata = response.getMetadata();
    this.ruleSet = response.getRuleSet();
    this.schema = response.getSchema();
  }

  public Schema copy() {
    return new Schema(subject, version, id, schemaType, references, metadata, ruleSet, schema);
  }

  public Schema copy(Integer version, Integer id) {
    return new Schema(subject, version, id, schemaType, references, metadata, ruleSet, schema);
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = SUBJECT_DESC, example = SUBJECT_EXAMPLE)
  @JsonProperty("subject")
  public String getSubject() {
    return subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = VERSION_DESC, example = VERSION_EXAMPLE)
  @JsonProperty("version")
  public Integer getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(Integer version) {
    this.version = version;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = ID_DESC, example = ID_EXAMPLE)
  @JsonProperty("id")
  public Integer getId() {
    return this.id;
  }

  @JsonProperty("id")
  public void setId(Integer id) {
    this.id = id;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = TYPE_DESC, example = TYPE_EXAMPLE)
  @JsonProperty("schemaType")
  @JsonSerialize(converter = SchemaTypeConverter.class)
  public String getSchemaType() {
    return this.schemaType;
  }

  @JsonProperty("schemaType")
  public void setSchemaType(String schemaType) {
    this.schemaType = schemaType;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = REFERENCES_DESC)
  @JsonProperty("references")
  public List<SchemaReference> getReferences() {
    return this.references;
  }

  @JsonProperty("references")
  public void setReferences(List<SchemaReference> references) {
    this.references = references;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = METADATA_DESC)
  @JsonProperty("metadata")
  public Metadata getMetadata() {
    return this.metadata;
  }

  @JsonProperty("metadata")
  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = RULESET_DESC)
  @JsonProperty("ruleSet")
  public RuleSet getRuleSet() {
    return this.ruleSet;
  }

  @JsonProperty("ruleSet")
  public void setRuleSet(RuleSet ruleSet) {
    this.ruleSet = ruleSet;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = SCHEMA_DESC, example = SCHEMA_EXAMPLE)
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
    Schema schema1 = (Schema) o;
    return Objects.equals(subject, schema1.subject)
        && Objects.equals(version, schema1.version)
        && Objects.equals(id, schema1.id)
        && Objects.equals(schemaType, schema1.schemaType)
        && Objects.equals(references, schema1.references)
        && Objects.equals(metadata, schema1.metadata)
        && Objects.equals(ruleSet, schema1.ruleSet)
        && Objects.equals(schema, schema1.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        subject, version, id, schemaType, references, metadata, ruleSet, schema);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{subject=" + this.subject + ",");
    sb.append("version=" + this.version + ",");
    sb.append("id=" + this.id + ",");
    sb.append("schemaType=" + this.schemaType + ",");
    sb.append("references=" + this.references + ",");
    sb.append("metadata=" + this.metadata + ",");
    sb.append("ruleSet=" + this.ruleSet + ",");
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

  public void updateHash(MessageDigest md) {
    if (schema != null) {
      md.update(schema.getBytes(StandardCharsets.UTF_8));
    }
    if (references != null) {
      references.forEach(r -> r.updateHash(md));
    }
    if (metadata != null) {
      md.update((byte) 1);  // to distinguish from null
      metadata.updateHash(md);
    }
    if (ruleSet != null) {
      md.update((byte) 1);  // to distinguish from null
      ruleSet.updateHash(md);
    }
  }
}
