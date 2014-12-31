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

import io.confluent.kafka.schemaregistry.rest.RegisterSchemaForwardingAgent;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;

public interface SchemaRegistry {

  void init() throws SchemaRegistryException;

  int register(String subject, Schema schema, RegisterSchemaForwardingAgent forwardingAgent)
      throws SchemaRegistryException;

  Schema get(String subject, int version) throws SchemaRegistryException;

  Set<String> listSubjects() throws SchemaRegistryException;

  Iterator<Schema> getAllVersions(String subject) throws SchemaRegistryException;

  void close();
}
