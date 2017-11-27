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
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProviderFactory;

public class CachedSchemaRegistryClient implements SchemaRegistryClient {

  private final RestService restService;
  private final int identityMapCapacity;
  private final Map<String, Map<Schema, Integer>> schemaCache;
  private final Map<String, Map<Integer, Schema>> idCache;
  private final Map<String, Map<Schema, Integer>> versionCache;

  public CachedSchemaRegistryClient(
      String baseUrl,
      int identityMapCapacity,
      Map<String, Object> configs) {
    this(new RestService(baseUrl), identityMapCapacity, configs);
  }

  public CachedSchemaRegistryClient(
      List<String> baseUrls,
      int identityMapCapacity,
      Map<String, ?> configs
  ) {
    this(new RestService(baseUrls), identityMapCapacity, configs);
  }

  public CachedSchemaRegistryClient(
      RestService restService,
      int identityMapCapacity,
      Map<String, ?> configs) {
    this.identityMapCapacity = identityMapCapacity;
    this.schemaCache = new HashMap<String, Map<Schema, Integer>>();
    this.idCache = new HashMap<String, Map<Integer, Schema>>();
    this.versionCache = new HashMap<String, Map<Schema, Integer>>();
    this.restService = restService;
    this.idCache.put(null, new HashMap<Integer, Schema>());
    configureRestService(configs);
  }

  private void configureRestService(Map<String, ?> configs) {
    if (configs != null) {
      BasicAuthCredentialProvider basicAuthCredentialProvider =
          BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider(
              (String) configs.get(BasicAuthCredentialProvider.BASIC_AUTH_CREDENTIALS_SOURCE),
              configs
          );
      restService.setBasicAuthCredentialProvider(basicAuthCredentialProvider);
    }
  }

  private int registerAndGetId(String subject, Schema schema)
      throws IOException, RestClientException {
    return restService.registerSchema(schema.toString(), subject);
  }

  private Schema getSchemaByIdFromRegistry(int id) throws IOException, RestClientException {
    SchemaString restSchema = restService.getId(id);
    return new Schema.Parser().parse(restSchema.getSchemaString());
  }

  private int getVersionFromRegistry(String subject, Schema schema)
      throws IOException, RestClientException {
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response =
        restService.lookUpSubjectVersion(schema.toString(), subject, true);
    return response.getVersion();
  }

  private int getIdFromRegistry(String subject, Schema schema)
      throws IOException, RestClientException {
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response =
        restService.lookUpSubjectVersion(schema.toString(), subject, false);
    return response.getId();
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
        throw new IllegalStateException("Too many schema objects created for " + subject + "!");
      }
      int id = registerAndGetId(subject, schema);
      schemaIdMap.put(schema, id);
      idCache.get(null).put(id, schema);
      return id;
    }
  }

  @Override
  public Schema getByID(final int id) throws IOException, RestClientException {
    return getById(id);
  }

  @Override
  public synchronized Schema getById(int id) throws IOException, RestClientException {
    return getBySubjectAndId(null, id);
  }

  @Override
  public Schema getBySubjectAndID(final String subject, final int id)
      throws IOException, RestClientException {
    return getBySubjectAndId(subject, id);
  }

  @Override
  public synchronized Schema getBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {

    Map<Integer, Schema> idSchemaMap;
    if (idCache.containsKey(subject)) {
      idSchemaMap = idCache.get(subject);
    } else {
      idSchemaMap = new HashMap<Integer, Schema>();
      idCache.put(subject, idSchemaMap);
    }

    if (idSchemaMap.containsKey(id)) {
      return idSchemaMap.get(id);
    } else {
      Schema schema = getSchemaByIdFromRegistry(id);
      idSchemaMap.put(id, schema);
      return schema;
    }
  }

  @Override
  public SchemaMetadata getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException {
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response
        = restService.getVersion(subject, version);
    int id = response.getId();
    String schema = response.getSchema();
    return new SchemaMetadata(id, version, schema);
  }

  @Override
  public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response
        = restService.getLatestVersion(subject);
    int id = response.getId();
    int version = response.getVersion();
    String schema = response.getSchema();
    return new SchemaMetadata(id, version, schema);
  }

  @Override
  public synchronized int getVersion(String subject, Schema schema)
      throws IOException, RestClientException {
    Map<Schema, Integer> schemaVersionMap;
    if (versionCache.containsKey(subject)) {
      schemaVersionMap = versionCache.get(subject);
    } else {
      schemaVersionMap = new IdentityHashMap();
      versionCache.put(subject, schemaVersionMap);
    }

    if (schemaVersionMap.containsKey(schema)) {
      return schemaVersionMap.get(schema);
    } else {
      if (schemaVersionMap.size() >= identityMapCapacity) {
        throw new IllegalStateException("Too many schema objects created for " + subject + "!");
      }
      int version = getVersionFromRegistry(subject, schema);
      schemaVersionMap.put(schema, version);
      return version;
    }
  }

  @Override
  public synchronized int getId(String subject, Schema schema)
      throws IOException, RestClientException {
    Map<Schema, Integer> schemaIdMap;
    if (schemaCache.containsKey(subject)) {
      schemaIdMap = schemaCache.get(subject);
    } else {
      schemaIdMap = new IdentityHashMap();
      schemaCache.put(subject, schemaIdMap);
    }

    if (schemaIdMap.containsKey(schema)) {
      return schemaIdMap.get(schema);
    } else {
      if (schemaIdMap.size() >= identityMapCapacity) {
        throw new IllegalStateException("Too many schema objects created for " + subject + "!");
      }
      int id = getIdFromRegistry(subject, schema);
      schemaIdMap.put(schema, id);
      idCache.get(null).put(id, schema);
      return id;
    }
  }

  @Override
  public boolean testCompatibility(String subject, Schema schema)
      throws IOException, RestClientException {
    return restService.testCompatibility(schema.toString(), subject, "latest");
  }

  @Override
  public String updateCompatibility(String subject, String compatibility)
      throws IOException, RestClientException {
    ConfigUpdateRequest response = restService.updateCompatibility(compatibility, subject);
    return response.getCompatibilityLevel();
  }

  @Override
  public String getCompatibility(String subject) throws IOException, RestClientException {
    Config response = restService.getConfig(subject);
    return response.getCompatibilityLevel();
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    return restService.getAllSubjects();
  }

}
