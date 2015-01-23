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
package io.confluent.kafka.schemaregistry.client;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalSchemaRegistryClient implements SchemaRegistryClient {

  private final Map<String, Map<Schema, Integer>> schemaCache;
  private final Map<Integer, Schema> idCache;
  private final AtomicInteger ids;

  public LocalSchemaRegistryClient() {
    schemaCache = new HashMap<String, Map<Schema, Integer>>();
    idCache = new HashMap<Integer, Schema>();
    ids = new AtomicInteger(0);
  }

  private int getIdFromRegistry(Schema schema, String subject) throws IOException {
    int id = ids.incrementAndGet();
    idCache.put(id, schema);
    return id;
  }

  private Schema getSchemaByIdFromRegisty(int id) throws IOException {
    if (idCache.containsKey(id)) {
      return idCache.get(id);
    } else {
      throw new IOException("Cannot get schema from schema registry!");
    }
  }

  @Override
  public synchronized int register(Schema schema, String subject) throws IOException {
    Map<Schema, Integer> schemaIdMap;
    if (schemaCache.containsKey(subject)) {
      schemaIdMap = schemaCache.get(subject);
    } else {
      schemaIdMap = new IdentityHashMap<Schema, Integer>();
      schemaCache.put(subject, schemaIdMap);
    }

    if (schemaIdMap.containsKey(schema)) {
      return schemaIdMap.get(schema);
    } else {
      int id = getIdFromRegistry(schema, subject);
      schemaIdMap.put(schema, id);
      return id;
    }
  }

  @Override
  public synchronized Schema getByID(int id) throws IOException {
    if (idCache.containsKey(id)) {
      return idCache.get(id);
    } else {
      Schema schema = getSchemaByIdFromRegisty(id);
      idCache.put(id, schema);
      return schema;
    }
  }
}
