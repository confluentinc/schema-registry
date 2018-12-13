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

import java.util.Collections;
import java.util.Objects;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProviderFactory;

/**
 * Thread-safe Schema Registry Client with client side caching.
 */
public class CachedSchemaRegistryClient implements SchemaRegistryClient {

  private final RestService restService;
  private final int identityMapCapacity;
  private final Map<String, Map<Schema, Integer>> schemaCache;
  private final Map<String, Map<Integer, Schema>> idCache;
  private final Map<String, Map<Schema, Integer>> versionCache;

  public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;

  static {
    DEFAULT_REQUEST_PROPERTIES = new HashMap<String, String>();
    DEFAULT_REQUEST_PROPERTIES.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
  }

  public CachedSchemaRegistryClient(String baseUrl, int identityMapCapacity) {
    this(new RestService(baseUrl), identityMapCapacity);
  }

  public CachedSchemaRegistryClient(List<String> baseUrls, int identityMapCapacity) {
    this(new RestService(baseUrls), identityMapCapacity);
  }

  public CachedSchemaRegistryClient(RestService restService, int identityMapCapacity) {
    this(restService, identityMapCapacity, null);
  }

  public CachedSchemaRegistryClient(
      String baseUrl,
      int identityMapCapacity,
      Map<String, ?> originals) {
    this(new RestService(baseUrl), identityMapCapacity, originals);
  }

  public CachedSchemaRegistryClient(
      List<String> baseUrls,
      int identityMapCapacity,
      Map<String, ?> originals) {
    this(new RestService(baseUrls), identityMapCapacity, originals);
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

      String credentialSourceConfig =
          (String) configs.get(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE);

      if (credentialSourceConfig != null && !credentialSourceConfig.isEmpty()) {

        BasicAuthCredentialProvider basicAuthCredentialProvider =
            BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider(
                credentialSourceConfig,
                configs);

        restService.setBasicAuthCredentialProvider(basicAuthCredentialProvider);
      }
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
    final Map<Schema, Integer> schemaIdMap =
        schemaCache.computeIfAbsent(subject, k -> new IdentityHashMap<>());

    final Integer cachedId = schemaIdMap.get(schema);
    if (cachedId != null) {
      return cachedId;
    }

    if (schemaIdMap.size() >= identityMapCapacity) {
      throw new IllegalStateException("Too many schema objects created for " + subject + "!");
    }

    final int retrievedId = registerAndGetId(subject, schema);
    schemaIdMap.put(schema, retrievedId);
    idCache.get(null).put(retrievedId, schema);
    return retrievedId;
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

    final Map<Integer, Schema> idSchemaMap = idCache
        .computeIfAbsent(subject, k -> new HashMap<>());

    final Schema cachedSchema = idSchemaMap.get(id);
    if (cachedSchema != null) {
      return cachedSchema;
    }

    final Schema retrievedSchema = getSchemaByIdFromRegistry(id);
    idSchemaMap.put(id, retrievedSchema);
    return retrievedSchema;
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
    final Map<Schema, Integer> schemaVersionMap =
        versionCache.computeIfAbsent(subject, k -> new IdentityHashMap<>());

    final Integer cachedVersion = schemaVersionMap.get(schema);
    if (cachedVersion != null) {
      return cachedVersion;
    }

    if (schemaVersionMap.size() >= identityMapCapacity) {
      throw new IllegalStateException("Too many schema objects created for " + subject + "!");
    }

    final int retrievedVersion = getVersionFromRegistry(subject, schema);
    schemaVersionMap.put(schema, retrievedVersion);
    return retrievedVersion;
  }

  @Override
  public List<Integer> getAllVersions(String subject)
      throws IOException, RestClientException {
    return restService.getAllVersions(subject);
  }

  @Override
  public synchronized int getId(String subject, Schema schema)
      throws IOException, RestClientException {
    final Map<Schema, Integer> schemaIdMap =
        schemaCache.computeIfAbsent(subject, k -> new IdentityHashMap<>());

    final Integer cachedId = schemaIdMap.get(schema);
    if (cachedId != null) {
      return cachedId;
    }

    if (schemaIdMap.size() >= identityMapCapacity) {
      throw new IllegalStateException("Too many schema objects created for " + subject + "!");
    }

    final int retrievedId = getIdFromRegistry(subject, schema);
    schemaIdMap.put(schema, retrievedId);
    idCache.get(null).put(retrievedId, schema);
    return retrievedId;
  }

  @Override
  public List<Integer> deleteSubject(String subject) throws IOException, RestClientException {
    return deleteSubject(DEFAULT_REQUEST_PROPERTIES, subject);
  }

  @Override
  public synchronized List<Integer> deleteSubject(
      Map<String, String> requestProperties, String subject)
      throws IOException, RestClientException {
    Objects.requireNonNull(subject, "subject");
    versionCache.remove(subject);
    idCache.remove(subject);
    schemaCache.remove(subject);
    return restService.deleteSubject(requestProperties, subject);
  }

  @Override
  public Integer deleteSchemaVersion(String subject, String version)
      throws IOException, RestClientException {
    return deleteSchemaVersion(DEFAULT_REQUEST_PROPERTIES, subject, version);
  }

  @Override
  public synchronized Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version)
      throws IOException, RestClientException {
    versionCache
        .getOrDefault(subject, Collections.emptyMap())
        .values()
        .remove(Integer.valueOf(version));

    return restService.deleteSchemaVersion(requestProperties, subject, version);
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
