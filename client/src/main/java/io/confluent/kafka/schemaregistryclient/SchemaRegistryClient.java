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
package io.confluent.kafka.schemaregistryclient;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistryclient.rest.entities.requests.RegisterSchemaRequest;

public class SchemaRegistryClient {

  private final String baseUrl;
  private final Map<String, Map<Schema, Long>> schemaCache;
  private final Map<Long, Schema> idCache;
  private final Schema.Parser parser = new Schema.Parser();

  public SchemaRegistryClient(String baseUrl) {
    this.baseUrl = baseUrl;
    schemaCache = new HashMap<String, Map<Schema, Long>>();
    idCache = new HashMap<Long, Schema>();
  }

  private RegisterSchemaRequest createRequest(Schema schema) {
    String schemaString = schema.toString();
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    return request;
  }

  public synchronized long register(Schema schema, String subject) throws IOException {

    Map<Schema, Long> schemaIdMap;
    if (schemaCache.containsKey(subject)) {
      schemaIdMap = schemaCache.get(subject);
    } else {
      schemaIdMap = new IdentityHashMap<Schema, Long>();
      schemaCache.put(subject, schemaIdMap);
    }

    if (schemaIdMap.containsKey(schema)) {
      return schemaIdMap.get(schema);
    } else {
      RegisterSchemaRequest request = createRequest(schema);
      long id = RestUtils.registerSchema(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES,
                                               request, subject);
      schemaIdMap.put(schema, id);
      return id;
    }
  }

  public synchronized Schema getByID(long id) throws IOException {
    if (idCache.containsKey(id)) {
      return idCache.get(id);
    } else {
      io.confluent.kafka.schemaregistryclient.rest.entities.Schema restSchema =
            RestUtils.getId(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, id);
      Schema schema = parser.parse(restSchema.getSchema());
      idCache.put(id, schema);
      return schema;
    }
  }
}
