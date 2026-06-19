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

package io.confluent.kafka.schemaregistry.client;

import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.Collections;
import java.util.List;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.Objects;

public class SchemaMetadata {

  private String subject;
  private int id;
  private int version;
  private String guid;
  private String schemaType;
  private String schema;
  private List<SchemaReference> references;
  private Metadata metadata = null;
  private RuleSet ruleSet = null;
  private Long timestamp = null;
  private Boolean deleted = null;

  public SchemaMetadata(int id,
                        int version,
                        String schema
  ) {
    this(id, version, AvroSchema.TYPE, Collections.emptyList(), schema);
  }

  public SchemaMetadata(int id,
                        int version,
                        String schemaType,
                        List<SchemaReference> references,
                        String schema
  ) {
    this.id = id;
    this.version = version;
    this.schemaType = schemaType;
    this.schema = schema;
    this.references = references;
  }

  public SchemaMetadata(Schema schema) {
    this.subject = schema.getSubject();
    this.id = schema.getId();
    this.guid = schema.getGuid();
    this.version = schema.getVersion();
    this.guid = schema.getGuid();
    this.schemaType = schema.getSchemaType();
    this.schema = schema.getSchema();
    this.references = schema.getReferences();
    this.metadata = schema.getMetadata();
    this.ruleSet = schema.getRuleSet();
    this.timestamp = schema.getTimestamp();
    this.deleted = schema.getDeleted();
  }

  public String getSubject() {
    return subject;
  }

  public int getId() {
    return id;
  }

  public int getVersion() {
    return version;
  }

  public String getGuid() {
    return guid;
  }

  public String getSchemaType() {
    return schemaType;
  }

  public String getSchema() {
    return schema;
  }

  public List<SchemaReference> getReferences() {
    return references;
  }

  public Metadata getMetadata() {
    return this.metadata;
  }

  public RuleSet getRuleSet() {
    return this.ruleSet;
  }

  public Long getTimestamp() {
    return this.timestamp;
  }

  public Boolean getDeleted() {
    return this.deleted;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaMetadata that = (SchemaMetadata) o;
    return Objects.equals(subject, that.subject)
        && id == that.id
        && version == that.version
        && Objects.equals(guid, that.guid)
        && Objects.equals(schemaType, that.schemaType)
        && Objects.equals(schema, that.schema)
        && Objects.equals(references, that.references)
        && Objects.equals(metadata, that.metadata)
        && Objects.equals(ruleSet, that.ruleSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        subject, id, version, guid, schemaType, schema, references, metadata, ruleSet);
  }
}
