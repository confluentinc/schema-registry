/*
 * Copyright 2023 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import java.lang.ref.SoftReference;

public class LazyParsedSchemaHolder implements ParsedSchemaHolder {

  private SchemaRegistry schemaRegistry;
  private SchemaKey schemaKey;
  private SoftReference<SchemaValue> schemaValueRef;

  public LazyParsedSchemaHolder(SchemaRegistry schemaRegistry, SchemaKey schemaKey) {
    this.schemaRegistry = schemaRegistry;
    this.schemaKey = schemaKey;
    this.schemaValueRef = new SoftReference<>(null);
  }

  /**
   * Returns the schema.
   *
   * @return the schema
   */
  @Override
  public ParsedSchema schema() {
    try {
      return schemaRegistry.parseSchema(schemaRegistry.toSchemaEntity(schemaValue()));
    } catch (SchemaRegistryException e) {
      throw new IllegalStateException(e);
    }
  }

  public SchemaValue schemaValue() throws SchemaRegistryException {
    SchemaValue schemaValue = schemaValueRef.get();
    if (schemaValue == null) {
      schemaValue = schemaRegistry.getSchemaValue(schemaKey);
      schemaValueRef = new SoftReference<>(schemaValue);
    }
    return schemaValue;
  }

  /**
   * Clears the schema if it can be lazily retrieved in the future.
   */
  @Override
  public void clear() {
    schemaValueRef.clear();
  }
}
