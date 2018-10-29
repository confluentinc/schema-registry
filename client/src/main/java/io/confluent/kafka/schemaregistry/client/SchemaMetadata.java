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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;

public class SchemaMetadata {

  private int id;
  private int version;
  private String schemaType = AvroSchema.AVRO;
  private String schema;

  public SchemaMetadata(int id, int version, String schema) {
    this(id, version, AvroSchema.AVRO, schema);
  }

  public SchemaMetadata(int id, int version, String schemaType, String schema) {
    this.id = id;
    this.version = version;
    this.schemaType = schemaType;
    this.schema = schema;
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
}
