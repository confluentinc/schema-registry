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
package io.confluent.kafka.schemaregistry;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.rest.entities.requests.RegisterSchemaRequest;

public class SchemaRegistryClient {

  private final String baseUrl;
  private final Map<String, Map<Schema, Integer>> schemaCache;
  private final Map<String, Map<Integer, Schema>> idCache;

  public SchemaRegistryClient(String baseUrl) {
    this.baseUrl = baseUrl;
    schemaCache = new HashMap<String, Map<Schema, Integer>>();
    idCache = new HashMap<String, Map<Integer, Schema>>();
  }

  public synchronized int register(Schema schema, String subject) throws IOException {
    String schemaString = schema.toString();
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);

    if (schemaCache.containsKey(subject)) {
      Map<Schema, Integer> schemaIdMap = schemaCache.get(subject);
      if (schemaIdMap.containsKey(schema)) {
        return schemaIdMap.get(schema);
      } else {
        int version = RestUtils.registerSchema(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES,
                                               request, subject);
        schemaIdMap.put(schema, version);
        return version;
      }
    } else {
      Map<Schema, Integer> schemaIdMap = new IdentityHashMap<Schema, Integer>();
      int version = RestUtils.registerSchema(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, request, subject);
      schemaIdMap.put(schema, version);
      schemaCache.put(subject, schemaIdMap);
      return version;
    }
  }

  public synchronized Schema getByID(String subject, int version) throws IOException {
    if (idCache.containsKey(subject)) {
      Map<Integer, Schema> idSchemaMap = idCache.get(subject);
      if (idSchemaMap.containsKey(version)) {
        return idSchemaMap.get(version);
      } else {
        io.confluent.kafka.schemaregistry.rest.entities.Schema restSchema =
            RestUtils.getVersion(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, subject, version);
        Schema schema = new Schema.Parser().parse(restSchema.getName());
        idSchemaMap.put(version, schema);
        return schema;
      }
    } else {
      Map<Integer, Schema> idSchemaMap = new IdentityHashMap<Integer, Schema>();
      io.confluent.kafka.schemaregistry.rest.entities.Schema restSchema =
          RestUtils.getVersion(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, subject, version);
      Schema schema = new Schema.Parser().parse(restSchema.getName());
      idSchemaMap.put(version, schema);
      idCache.put(subject, idSchemaMap);
      return schema;
    }
  }
}