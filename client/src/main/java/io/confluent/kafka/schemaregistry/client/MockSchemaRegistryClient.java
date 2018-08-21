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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Mock implementation of SchemaRegistryClient that can be used for tests. This version is NOT
 * thread safe. Schema data is stored in memory and is not persistent or shared across instances.
 */
public class MockSchemaRegistryClient implements SchemaRegistryClient {

  private String defaultCompatibility = "BACKWARD";
  private final Map<String, Map<Schema, Integer>> schemaCache;
  private final Map<Schema, Integer> schemaIdCache;
  private final Map<String, Map<Integer, Schema>> idCache;
  private final Map<String, Map<Schema, Integer>> versionCache;
  private final Map<String, String> compatibilityCache;
  private final AtomicInteger ids;

  public MockSchemaRegistryClient() {
    schemaCache = new HashMap<String, Map<Schema, Integer>>();
    schemaIdCache = new HashMap<>();
    idCache = new HashMap<String, Map<Integer, Schema>>();
    versionCache = new HashMap<String, Map<Schema, Integer>>();
    compatibilityCache = new HashMap<String, String>();
    ids = new AtomicInteger(0);
    idCache.put(null, new HashMap<Integer, Schema>());
  }

  private int getIdFromRegistry(String subject, Schema schema, boolean registerRequest)
      throws IOException, RestClientException {
    Map<Integer, Schema> idSchemaMap;
    if (idCache.containsKey(subject)) {
      idSchemaMap = idCache.get(subject);
      for (Map.Entry<Integer, Schema> entry : idSchemaMap.entrySet()) {
        if (entry.getValue().toString().equals(schema.toString())) {
          if (registerRequest) {
            generateVersion(subject, schema);
          }
          return entry.getKey();
        }
      }
    } else {
      idSchemaMap = new HashMap<Integer, Schema>();
    }
    if (registerRequest) {
      int id;
      if(schemaIdCache.containsKey(schema)){
        id = schemaIdCache.get(schema);
      } else {
        id = ids.incrementAndGet();
        schemaIdCache.put(schema, id);
      }
      idSchemaMap.put(id, schema);
      idCache.put(subject, idSchemaMap);
      generateVersion(subject, schema);
      return id;
    } else {
      throw new RestClientException("Schema Not Found", 404, 404001);
    }
  }

  private void generateVersion(String subject, Schema schema) {
    List<Integer> versions = getAllVersions(subject);
    Map<Schema, Integer> schemaVersionMap;
    int currentVersion;
    if (versions.isEmpty()) {
      schemaVersionMap = new IdentityHashMap<Schema, Integer>();
      currentVersion = 1;
    } else {
      schemaVersionMap = versionCache.get(subject);
      currentVersion = versions.get(versions.size() - 1) + 1;
    }
    schemaVersionMap.put(schema, currentVersion);
    versionCache.put(subject, schemaVersionMap);
  }

  @Override
  public synchronized List<Integer> getAllVersions(String subject) {
    ArrayList<Integer> versions = new ArrayList<Integer>();
    if (versionCache.containsKey(subject)) {
      versions.addAll(versionCache.get(subject).values());
      Collections.sort(versions);
    }
    return versions;
  }

  private Schema getSchemaBySubjectAndIdFromRegistry(String subject, int id) throws IOException {
    if (idCache.containsKey(subject)) {
      Map<Integer, Schema> idSchemaMap = idCache.get(subject);
      if (idSchemaMap.containsKey(id)) {
        return idSchemaMap.get(id);
      }
    }
    throw new IOException("Cannot get schema from schema registry!");
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
      int id = getIdFromRegistry(subject, schema, true);
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
      Schema schema = getSchemaBySubjectAndIdFromRegistry(subject, id);
      idSchemaMap.put(id, schema);
      return schema;
    }
  }

  private int getLatestVersion(String subject)
      throws IOException, RestClientException {
    List<Integer> versions = getAllVersions(subject);
    if (versions.isEmpty()) {
      throw new IOException("No schema registered under subject!");
    } else {
      return versions.get(versions.size() - 1);
    }
  }

  @Override
  public synchronized SchemaMetadata getSchemaMetadata(String subject, int version) {
    String schemaString = null;
    Map<Schema, Integer> schemaVersionMap = versionCache.get(subject);
    for (Map.Entry<Schema, Integer> entry : schemaVersionMap.entrySet()) {
      if (entry.getValue() == version) {
        schemaString = entry.getKey().toString();
      }
    }
    int id = -1;
    Map<Integer, Schema> idSchemaMap = idCache.get(subject);
    for (Map.Entry<Integer, Schema> entry : idSchemaMap.entrySet()) {
      if (entry.getValue().toString().equals(schemaString)) {
        id = entry.getKey();
      }
    }
    return new SchemaMetadata(id, version, schemaString);
  }

  @Override
  public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    int version = getLatestVersion(subject);
    return getSchemaMetadata(subject, version);
  }

  @Override
  public synchronized int getVersion(String subject, Schema schema)
      throws IOException, RestClientException {
    if (versionCache.containsKey(subject)) {
      return versionCache.get(subject).get(schema);
    } else {
      throw new IOException("Cannot get version from schema registry!");
    }
  }

  @Override
  public boolean testCompatibility(String subject, Schema newSchema) throws IOException,
                                                                            RestClientException {
    String compatibility = compatibilityCache.get(subject);
    if (compatibility == null) {
      compatibility = defaultCompatibility;
    }

    AvroCompatibilityLevel compatibilityLevel = AvroCompatibilityLevel.forName(compatibility);
    if (compatibilityLevel == null) {
      return false;
    }

    List<Schema> schemaHistory = new ArrayList<>();
    for (int version : getAllVersions(subject)) {
      SchemaMetadata schemaMetadata = getSchemaMetadata(subject, version);
      schemaHistory.add(getSchemaBySubjectAndIdFromRegistry(subject, schemaMetadata.getId()));
    }

    return compatibilityLevel.compatibilityChecker.isCompatible(newSchema, schemaHistory);
  }

  @Override
  public String updateCompatibility(String subject, String compatibility)
      throws IOException,
             RestClientException {
    if (subject == null) {
      defaultCompatibility = compatibility;
      return compatibility;
    }
    compatibilityCache.put(subject, compatibility);
    return compatibility;
  }

  @Override
  public String getCompatibility(String subject) throws IOException, RestClientException {
    String compatibility = compatibilityCache.get(subject);
    if (compatibility == null) {
      compatibility = defaultCompatibility;
    }
    return compatibility;
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    List<String> results = new ArrayList<>();
    results.addAll(this.schemaCache.keySet());
    Collections.sort(results, String.CASE_INSENSITIVE_ORDER);
    return results;
  }

  @Override
  public int getId(String subject, Schema schema) throws IOException, RestClientException {
    return getIdFromRegistry(subject, schema, false);
  }

  @Override
  public List<Integer> deleteSubject(String subject) throws IOException, RestClientException {
    return deleteSubject(null, subject);
  }

  @Override
  public List<Integer> deleteSubject(
      Map<String, String> requestProperties,
      String subject)
      throws IOException, RestClientException {
    schemaCache.remove(subject);
    versionCache.remove(subject);
    compatibilityCache.remove(subject);
    return Arrays.asList(0);
  }

  @Override
  public Integer deleteSchemaVersion(String subject, String version)
      throws IOException, RestClientException {
    return deleteSchemaVersion(null, subject, version);
  }

  @Override
  public Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version)
      throws IOException, RestClientException {
    if (versionCache.containsKey(subject)) {
      versionCache.get(subject).values().remove(Integer.valueOf(version));
      return 0;
    }
    return -1;
  }
}
