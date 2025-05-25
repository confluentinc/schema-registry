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

package io.confluent.dpregistry.client;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.dpregistry.client.rest.DataProductRegistryRestService;
import io.confluent.dpregistry.client.rest.entities.DataProduct;
import io.confluent.dpregistry.client.rest.entities.RegisteredDataProduct;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class CachedDataProductRegistryClient extends CachedSchemaRegistryClient
    implements DataProductRegistryClient {

  private final DataProductRegistryRestService restService;
  private final Cache<DataProductKey, RegisteredDataProduct> dataProductCache;
  private final Ticker ticker;

  public CachedDataProductRegistryClient(
      List<String> baseUrls,
      int cacheCapacity,
      int cacheExpirySecs,
      Map<String, ?> configs,
      Map<String, String> httpHeaders) {
    this(new DataProductRegistryRestService(baseUrls),
        cacheCapacity, cacheExpirySecs, configs, httpHeaders, Ticker.systemTicker());
  }

  public CachedDataProductRegistryClient(
      DataProductRegistryRestService restService,
      int cacheCapacity,
      int cacheExpirySecs,
      Map<String, ?> configs,
      Map<String, String> httpHeaders) {
    this(restService, cacheCapacity, cacheExpirySecs, configs, httpHeaders, Ticker.systemTicker());
  }

  public CachedDataProductRegistryClient(
      DataProductRegistryRestService restService,
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
    this.dataProductCache = cacheBuilder.build();
    this.ticker = ticker;
  }

  @Override
  public Ticker ticker() {
    return ticker;
  }

  @Override
  public List<String> listDataProductNames(String env, String cluster, boolean lookupDeleted)
      throws IOException, RestClientException {
    return restService.listDataProductNames(env, cluster, lookupDeleted);
  }

  @Override
  public List<Integer> listDataProductVersions(
      String env, String cluster, String name, boolean lookupDeleted)
      throws IOException, RestClientException {
    return restService.listDataProductVersions(env, cluster, name, lookupDeleted);
  }

  @Override
  public RegisteredDataProduct getDataProduct(
      String env, String cluster, String name, int version, boolean lookupDeleted)
      throws IOException, RestClientException {
    try {
      return dataProductCache.get(
          new DataProductKey(env, cluster, name, version, lookupDeleted),
          () -> restService.getDataProduct(env, cluster, name, version, lookupDeleted));
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
  public RegisteredDataProduct getLatestDataProduct(String env, String cluster, String name,
      boolean lookupDeleted)
      throws IOException, RestClientException {
    try {
      return dataProductCache.get(
          new DataProductKey(env, cluster, name, LATEST_VERSION, lookupDeleted),
          () -> restService.getDataProduct(env, cluster, name, LATEST_VERSION, lookupDeleted));
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
  public RegisteredDataProduct createDataProduct(
      String env,
      String cluster,
      DataProduct dataProduct)
      throws IOException, RestClientException {
    return createDataProduct(DEFAULT_REQUEST_PROPERTIES, env, cluster, dataProduct);
  }

  public RegisteredDataProduct createDataProduct(
      Map<String, String> requestProperties,
      String env,
      String cluster,
      DataProduct dataProduct)
      throws IOException, RestClientException {
    RegisteredDataProduct product = restService.createDataProduct(
        requestProperties, env, cluster, dataProduct);
    dataProductCache.put(
        new DataProductKey(env, cluster, product.getInfo().getName(), product.getVersion(), false),
        product
    );
    return product;
  }

  @Override
  public void deleteDataProduct(String env, String cluster, String name, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDataProduct(DEFAULT_REQUEST_PROPERTIES, env, cluster, name, permanentDelete);
  }

  public void deleteDataProduct(
      Map<String, String> requestProperties, String env, String cluster,
      String name, boolean permanentDelete)
      throws IOException, RestClientException {
    restService.deleteDataProduct(requestProperties, env, cluster, name, permanentDelete);
    dataProductCache.invalidateAll();
  }

  @Override
  public void deleteDataProductVersion(
      String env, String cluster, String name, int version, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDekVersion(DEFAULT_REQUEST_PROPERTIES, env, cluster, name, version, permanentDelete);
  }

  public void deleteDekVersion(
      Map<String, String> requestProperties, String env, String cluster, String name, int version,
      boolean permanentDelete)
      throws IOException, RestClientException {
    restService.deleteDataProductVersion(requestProperties, env, cluster, name, version,
        permanentDelete);
    dataProductCache.invalidate(new DataProductKey(env, cluster, name, version, true));
    dataProductCache.invalidate(new DataProductKey(env, cluster, name, version, false));
  }

  @Override
  public void reset() {
    dataProductCache.invalidateAll();
  }

  public static class DataProductKey {

    private final String env;
    private final String cluster;
    private final String name;
    private final Integer version;
    private final boolean lookupDeleted;

    public DataProductKey(
        String env, String cluster, String name, Integer version, boolean lookupDeleted) {
      this.env = env;
      this.cluster = cluster;
      this.name = name;
      this.version = version;
      this.lookupDeleted = lookupDeleted;
    }

    public String getEnv() {
      return env;
    }

    public String getCluster() {
      return cluster;
    }

    public String getName() {
      return name;
    }

    public Integer getVersion() {
      return version;
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
      DataProductKey key = (DataProductKey) o;
      return lookupDeleted == key.lookupDeleted
          && Objects.equals(env, key.env)
          && Objects.equals(cluster, key.cluster)
          && Objects.equals(name, key.name)
          && Objects.equals(version, key.version);
    }

    @Override
    public int hashCode() {
      return Objects.hash(env, cluster, name, version, lookupDeleted);
    }
  }
}
