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

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.Collections;
import java.util.List;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

public class SchemaMetadata {

  private int id;
  private int version;
  private String schemaType;
  private String schema;
  private List<SchemaReference> references;

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
    this.id = schema.getId();
    this.version = schema.getVersion();
    this.schemaType = schema.getSchemaType();
    this.schema = schema.getSchema();
    this.references = schema.getReferences();
  }

  public int getId() {
    return id;
  }

  public int getVersion() {
    return version;
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
}
