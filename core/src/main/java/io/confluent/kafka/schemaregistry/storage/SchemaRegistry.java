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

import java.util.Iterator;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryInitializationException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;

public interface SchemaRegistry {

  void init() throws SchemaRegistryInitializationException, SchemaRegistryTimeoutException;

  int register(String subject, Schema schema) throws SchemaRegistryStoreException,
                                                     InvalidSchemaException,
                                                     SchemaRegistryTimeoutException;

  Schema get(String subject, int version)
      throws SchemaRegistryStoreException, InvalidVersionException;

  SchemaString get(int id) throws SchemaRegistryStoreException;

  Set<String> listSubjects() throws SchemaRegistryStoreException;

  Iterator<Schema> getAllVersions(String subject) throws SchemaRegistryStoreException;

  Schema getLatestVersion(String subject) throws SchemaRegistryStoreException;

  public boolean isCompatible(String subject,
                              String inputSchema,
                              String targetSchema)
      throws InvalidSchemaException, SchemaRegistryStoreException;

  void close();
}
