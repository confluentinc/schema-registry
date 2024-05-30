/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.dekregistry.client;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.dekregistry.client.rest.DekRegistryRestService;
import io.confluent.dekregistry.client.rest.entities.CreateDekRequest;
import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.client.rest.entities.UpdateKekRequest;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class CachedDekRegistryClient extends CachedSchemaRegistryClient
    implements DekRegistryClient {

  private final DekRegistryRestService restService;
  private final Cache<KekId, Kek> kekCache;
  private final Cache<DekId, Dek> dekCache;
  private final Ticker ticker;

  public CachedDekRegistryClient(
      List<String> baseUrls,
      int cacheCapacity,
      int cacheExpirySecs,
      Map<String, ?> configs,
      Map<String, String> httpHeaders) {
    this(new DekRegistryRestService(baseUrls),
        cacheCapacity, cacheExpirySecs, configs, httpHeaders, Ticker.systemTicker());
  }

  public CachedDekRegistryClient(
      DekRegistryRestService restService,
      int cacheCapacity,
      int cacheExpirySecs,
      Map<String, ?> configs,
      Map<String, String> httpHeaders) {
    this(restService, cacheCapacity, cacheExpirySecs, configs, httpHeaders, Ticker.systemTicker());
  }

  public CachedDekRegistryClient(
      DekRegistryRestService restService,
      int cacheCapacity,
      int cacheExpirySecs,
      Map<String, ?> configs,
      Map<String, String> httpHeaders,
      Ticker ticker) {
    super(restService, cacheCapacity, Collections.emptyList(), configs, httpHeaders, ticker);
    this.restService = restService;
    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .ticker(ticker);
    if (cacheExpirySecs >= 0) {
      // Allow expiry in case shared flag changes
      cacheBuilder = cacheBuilder.expireAfterWrite(Duration.ofSeconds(cacheExpirySecs));
    }
    this.kekCache = cacheBuilder.build();
    cacheBuilder = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .ticker(ticker);
    if (cacheExpirySecs >= 0) {
      // Allow expiry in case shared flag changes
      cacheBuilder = cacheBuilder.expireAfterWrite(Duration.ofSeconds(cacheExpirySecs));
    }
    this.dekCache = cacheBuilder.build();
    this.ticker = ticker;
  }

  @Override
  public Ticker ticker() {
    return ticker;
  }

  @Override
  public List<String> listKeks(boolean lookupDeleted)
      throws IOException, RestClientException {
    return restService.listKeks(lookupDeleted);
  }

  @Override
  public List<String> listKeks(String subject, boolean lookupDeleted)
      throws IOException, RestClientException {
    return restService.listKeks(subject, lookupDeleted);
  }

  @Override
  public Kek getKek(String name, boolean lookupDeleted)
      throws IOException, RestClientException {
    try {
      return kekCache.get(new KekId(name, lookupDeleted), () ->
          restService.getKek(name, lookupDeleted));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else if (e.getCause() instanceof RestClientException) {
        throw (RestClientException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public List<String> listDeks(String kekName, boolean lookupDeleted)
      throws IOException, RestClientException {
    return restService.listDeks(kekName, lookupDeleted);
  }

  @Override
  public List<Integer> listDekVersions(String kekName, String subject,
      DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    return restService.listDekVersions(kekName, subject, algorithm, lookupDeleted);
  }

  @Override
  public Dek getDek(String kekName, String subject, DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    try {
      return dekCache.get(new DekId(kekName, subject, null, algorithm, lookupDeleted), () ->
          restService.getDek(kekName, subject, algorithm, lookupDeleted));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else if (e.getCause() instanceof RestClientException) {
        throw (RestClientException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public Dek getDekVersion(String kekName, String subject, int version,
      DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    try {
      return dekCache.get(new DekId(kekName, subject, version, algorithm, lookupDeleted), () ->
          restService.getDekVersion(kekName, subject, version, algorithm, lookupDeleted));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else if (e.getCause() instanceof RestClientException) {
        throw (RestClientException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public Dek getDekLatestVersion(String kekName, String subject,
      DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    try {
      return dekCache.get(
          new DekId(kekName, subject, LATEST_VERSION, algorithm, lookupDeleted), () ->
              restService.getDekVersion(
                  kekName, subject, LATEST_VERSION, algorithm, lookupDeleted));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else if (e.getCause() instanceof RestClientException) {
        throw (RestClientException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public Kek createKek(
      String name,
      String kmsType,
      String kmsKeyId,
      Map<String, String> kmsProps,
      String doc,
      boolean shared)
      throws IOException, RestClientException {
    return createKek(DEFAULT_REQUEST_PROPERTIES, name, kmsType, kmsKeyId,
        kmsProps, doc, shared, false);
  }

  @Override
  public Kek createKek(
      String name,
      String kmsType,
      String kmsKeyId,
      Map<String, String> kmsProps,
      String doc,
      boolean shared,
      boolean deleted)
      throws IOException, RestClientException {
    return createKek(DEFAULT_REQUEST_PROPERTIES, name, kmsType, kmsKeyId,
        kmsProps, doc, shared, deleted);
  }

  public Kek createKek(
      Map<String, String> requestProperties,
      String name,
      String kmsType,
      String kmsKeyId,
      Map<String, String> kmsProps,
      String doc,
      boolean shared,
      boolean deleted)
      throws IOException, RestClientException {
    CreateKekRequest request = new CreateKekRequest();
    request.setName(name);
    request.setKmsType(kmsType);
    request.setKmsKeyId(kmsKeyId);
    request.setKmsProps(kmsProps);
    request.setDoc(doc);
    request.setShared(shared);
    request.setDeleted(deleted);
    Kek kek = restService.createKek(requestProperties, request);
    kekCache.put(new KekId(name, deleted), kek);
    return kek;
  }

  @Override
  public Dek createDek(
      String kekName,
      String subject,
      DekFormat algorithm,
      String encryptedKeyMaterial)
      throws IOException, RestClientException {
    return createDek(DEFAULT_REQUEST_PROPERTIES, kekName, subject, null,
        algorithm, encryptedKeyMaterial, false);
  }

  @Override
  public Dek createDek(
      String kekName,
      String subject,
      int version,
      DekFormat algorithm,
      String encryptedKeyMaterial)
      throws IOException, RestClientException {
    return createDek(DEFAULT_REQUEST_PROPERTIES, kekName, subject, version,
        algorithm, encryptedKeyMaterial, false);
  }

  @Override
  public Dek createDek(
      String kekName,
      String subject,
      int version,
      DekFormat algorithm,
      String encryptedKeyMaterial,
      boolean deleted)
      throws IOException, RestClientException {
    return createDek(DEFAULT_REQUEST_PROPERTIES, kekName, subject, version,
        algorithm, encryptedKeyMaterial, deleted);
  }

  public Dek createDek(
      Map<String, String> requestProperties,
      String kekName,
      String subject,
      Integer version,
      DekFormat algorithm,
      String encryptedKeyMaterial,
      boolean deleted)
      throws IOException, RestClientException {
    CreateDekRequest request = new CreateDekRequest();
    request.setSubject(subject);
    request.setVersion(version);
    request.setAlgorithm(algorithm);
    request.setEncryptedKeyMaterial(encryptedKeyMaterial);
    request.setDeleted(deleted);
    try {
      Dek dek = restService.createDek(requestProperties, kekName, request);
      dekCache.put(new DekId(kekName, subject, version, algorithm, deleted), dek);
      return dek;
    } finally {
      // Ensure latest dek is invalidated, such as in case of conflict (409)
      dekCache.invalidate(new DekId(kekName, subject, LATEST_VERSION, algorithm, false));
      dekCache.invalidate(new DekId(kekName, subject, LATEST_VERSION, algorithm, true));
    }
  }

  @Override
  public Kek updateKek(
      String name,
      Map<String, String> kmsProps,
      String doc,
      Boolean shared)
      throws IOException, RestClientException {
    return updateKek(DEFAULT_REQUEST_PROPERTIES, name, kmsProps, doc, shared);
  }

  public Kek updateKek(
      Map<String, String> requestProperties,
      String name,
      Map<String, String> kmsProps,
      String doc,
      Boolean shared)
      throws IOException, RestClientException {
    UpdateKekRequest request = new UpdateKekRequest();
    request.setKmsProps(kmsProps);
    request.setDoc(doc);
    request.setShared(shared);
    Kek kek = restService.updateKek(requestProperties, name, request);
    kekCache.put(new KekId(name, false), kek);
    return kek;
  }

  @Override
  public void deleteKek(String kekName, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteKek(DEFAULT_REQUEST_PROPERTIES, kekName, permanentDelete);
  }

  public void deleteKek(
      Map<String, String> requestProperties, String kekName, boolean permanentDelete)
      throws IOException, RestClientException {
    restService.deleteKek(requestProperties, kekName, permanentDelete);
    kekCache.invalidate(new KekId(kekName, false));
    kekCache.invalidate(new KekId(kekName, true));
  }

  @Override
  public void deleteDek(
      String kekName, String subject, DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDek(DEFAULT_REQUEST_PROPERTIES, kekName, subject, algorithm, permanentDelete);
  }

  public void deleteDek(
      Map<String, String> requestProperties, String kekName, String subject, DekFormat algorithm,
      boolean permanentDelete)
      throws IOException, RestClientException {
    restService.deleteDek(requestProperties, kekName, subject, algorithm, permanentDelete);
    dekCache.invalidateAll();
  }

  @Override
  public void deleteDekVersion(
      String kekName, String subject, int version, DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDekVersion(DEFAULT_REQUEST_PROPERTIES, kekName, subject, version,
        algorithm, permanentDelete);
  }

  public void deleteDekVersion(
      Map<String, String> requestProperties, String kekName, String subject, int version,
      DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    restService.deleteDekVersion(requestProperties, kekName, subject, version,
        algorithm, permanentDelete);
    // Just invalidate all since the version can be represented many ways,
    // such as null for first or -1 for latest
    dekCache.invalidateAll();
  }

  @Override
  public void undeleteKek(String kekName)
      throws IOException, RestClientException {
    undeleteKek(DEFAULT_REQUEST_PROPERTIES, kekName);
  }

  public void undeleteKek(
      Map<String, String> requestProperties, String kekName)
      throws IOException, RestClientException {
    restService.undeleteKek(requestProperties, kekName);
    kekCache.invalidate(new KekId(kekName, false));
    kekCache.invalidate(new KekId(kekName, true));
  }

  @Override
  public void undeleteDek(
      String kekName, String subject, DekFormat algorithm)
      throws IOException, RestClientException {
    undeleteDek(DEFAULT_REQUEST_PROPERTIES, kekName, subject, algorithm);
  }

  public void undeleteDek(
      Map<String, String> requestProperties, String kekName, String subject, DekFormat algorithm)
      throws IOException, RestClientException {
    restService.undeleteDek(requestProperties, kekName, subject, algorithm);
    dekCache.invalidateAll();
  }

  @Override
  public void undeleteDekVersion(
      String kekName, String subject, int version, DekFormat algorithm)
      throws IOException, RestClientException {
    undeleteDekVersion(DEFAULT_REQUEST_PROPERTIES, kekName, subject, version,
        algorithm);
  }

  public void undeleteDekVersion(
      Map<String, String> requestProperties, String kekName, String subject, int version,
      DekFormat algorithm)
      throws IOException, RestClientException {
    restService.undeleteDekVersion(requestProperties, kekName, subject, version, algorithm);
    // Just invalidate all since the version can be represented many ways,
    // such as null for first or -1 for latest
    dekCache.invalidateAll();
  }

  @Override
  public void reset() {
    kekCache.invalidateAll();
    dekCache.invalidateAll();
  }

  public static class KekId {

    private final String name;
    private final boolean lookupDeleted;

    public KekId(String name, boolean lookupDeleted) {
      this.name = name;
      this.lookupDeleted = lookupDeleted;
    }

    public String getName() {
      return name;
    }

    public boolean isLookupDeleted() {
      return lookupDeleted;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KekId kekId = (KekId) o;
      return lookupDeleted == kekId.lookupDeleted
          && Objects.equals(name, kekId.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, lookupDeleted);
    }
  }

  public static class DekId {

    private final String kekName;
    private final String subject;
    private final Integer version;
    private final DekFormat dekFormat;
    private final boolean lookupDeleted;

    public DekId(String kekName, String subject, Integer version,
        DekFormat dekFormat, boolean lookupDeleted) {
      this.kekName = kekName;
      this.subject = subject;
      this.version = version;
      this.dekFormat = dekFormat;
      this.lookupDeleted = lookupDeleted;
    }

    public String getKekName() {
      return kekName;
    }

    public String getSubject() {
      return subject;
    }

    public Integer getVersion() {
      return version;
    }

    public DekFormat getDekFormat() {
      return dekFormat;
    }

    public boolean isLookupDeleted() {
      return lookupDeleted;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DekId dekId = (DekId) o;
      return lookupDeleted == dekId.lookupDeleted
          && Objects.equals(kekName, dekId.kekName)
          && Objects.equals(subject, dekId.subject)
          && Objects.equals(version, dekId.version)
          && dekFormat == dekId.dekFormat;
    }

    @Override
    public int hashCode() {
      return Objects.hash(kekName, subject, version, dekFormat, lookupDeleted);
    }
  }
}
