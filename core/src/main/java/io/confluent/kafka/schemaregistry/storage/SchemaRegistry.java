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
import java.util.List;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryInitializationException;

public interface SchemaRegistry {

  void init() throws SchemaRegistryInitializationException;

  int register(String subject, Schema schema) throws SchemaRegistryException;

  Schema get(String subject, int version) throws SchemaRegistryException;

  SchemaString get(int id) throws SchemaRegistryException;

  Set<String> listSubjects() throws SchemaRegistryException;

  Iterator<Schema> getAllVersions(String subject) throws SchemaRegistryException;

  Schema getLatestVersion(String subject) throws SchemaRegistryException;

  Schema lookUpSchemaUnderSubject(String subject, Schema schema) throws SchemaRegistryException;

  boolean isCompatible(String subject,
                       String inputSchema,
                       String targetSchema) throws SchemaRegistryException;

  boolean isCompatible(String subject,
                       String newSchema,
                       List<String> previousSchemas) throws SchemaRegistryException;

  void close();

}
