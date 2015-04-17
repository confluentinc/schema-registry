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

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.RestUtils;

public class CachedSchemaRegistryClient implements SchemaRegistryClient {

  private final String baseUrl;
  private final int identityMapCapacity;
  private final Map<String, Map<Schema, Integer>> schemaCache;
  private final Map<Integer, Schema> idCache;
  private final Map<String, Map<Schema, Integer>> versionCache;

  public CachedSchemaRegistryClient(String baseUrl, int identityMapCapacity) {
    this.baseUrl = baseUrl;
    this.identityMapCapacity = identityMapCapacity;
    schemaCache = new HashMap<String, Map<Schema, Integer>>();
    idCache = new HashMap<Integer, Schema>();
    versionCache = new HashMap<String, Map<Schema, Integer>>();
  }

  private int registerAndGetId(String subject, Schema schema)
      throws IOException, RestClientException {
    String schemaString = schema.toString();
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    return RestUtils.registerSchema(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES,
                                    request, subject);
  }

  private Schema getSchemaByIdFromRegistry(int id) throws IOException, RestClientException {
    SchemaString restSchema =
        RestUtils.getId(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, id);
    return new Schema.Parser().parse(restSchema.getSchemaString());
  }


  private int getVersionFromRegistry(String subject, Schema schema)
      throws IOException, RestClientException{
    String schemaString = schema.toString();
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response =
        RestUtils.lookUpSubjectVersion(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, request, subject);
    return response.getVersion();
  }

  @Override
  public synchronized int register(String subject, Schema schema)
      throws IOException, RestClientException {
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
      if (schemaIdMap.size() >= identityMapCapacity) {
        throw new IllegalStateException("Two many schema objects created for " + subject + "!");
      }
      int id = registerAndGetId(subject, schema);
      schemaIdMap.put(schema, id);
      return id;
    }
  }

  @Override
  public synchronized Schema getByID(int id) throws IOException, RestClientException {
    if (idCache.containsKey(id)) {
      return idCache.get(id);
    } else {
      Schema schema = getSchemaByIdFromRegistry(id);
      idCache.put(id, schema);
      return schema;
    }
  }

  @Override
  public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response
     = RestUtils.getLatestVersion(
        baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, subject);
    int id = response.getId();
    int version = response.getVersion();
    String schema = response.getSchema();
    return new SchemaMetadata(id, version, schema);
  }

  @Override
  public synchronized int getVersion(String subject, Schema schema)
      throws IOException, RestClientException{
    Map<Schema, Integer> schemaVersionMap;
    if (versionCache.containsKey(subject)) {
      schemaVersionMap = versionCache.get(subject);
    } else {
      schemaVersionMap = new IdentityHashMap<Schema, Integer>();
      versionCache.put(subject, schemaVersionMap);
    }

    if (schemaVersionMap.containsKey(schema)) {
      return schemaVersionMap.get(schema);
    }  else {
      if (schemaVersionMap.size() >= identityMapCapacity) {
        throw new IllegalStateException("Two many schema objects created for " + subject + "!");
      }
      int version = getVersionFromRegistry(subject, schema);
      schemaVersionMap.put(schema, version);
      return version;
    }
  }
}
