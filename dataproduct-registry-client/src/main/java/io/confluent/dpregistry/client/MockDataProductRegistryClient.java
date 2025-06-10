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

import io.confluent.dpregistry.client.rest.entities.DataProduct;
import io.confluent.dpregistry.client.rest.entities.RegisteredDataProduct;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MockDataProductRegistryClient implements DataProductRegistryClient {

  public static final byte[] EMPTY_AAD = new byte[0];

  private final Map<String, ?> configs;
  private final Map<DataProductKey, RegisteredDataProduct> dataProducts;

  public MockDataProductRegistryClient(Map<String, ?> configs) {
    this.configs = configs;
    this.dataProducts = new ConcurrentHashMap<>();
  }

  @Override
  public List<String> listDataProductNames(String env, String cluster, boolean lookupDeleted)
      throws IOException, RestClientException {
    return dataProducts.entrySet().stream()
        .filter(kv -> !kv.getValue().isDeleted() || lookupDeleted)
        .map(kv -> kv.getKey().getName())
        .collect(Collectors.toList());
  }

  @Override
  public List<Integer> listDataProductVersions(String env, String cluster, String name,
      boolean lookupDeleted)
      throws IOException, RestClientException {
    return dataProducts.entrySet().stream()
        .filter(kv -> kv.getKey().getName().equals(name)
            && (!kv.getValue().isDeleted() || lookupDeleted))
        .map(kv -> kv.getKey().getVersion())
        .collect(Collectors.toList());
  }

  @Override
  public RegisteredDataProduct getDataProduct(
      String env, String cluster, String name, int version, boolean lookupDeleted)
      throws IOException, RestClientException {
    DataProductKey key = new DataProductKey(env, cluster, name, version);
    RegisteredDataProduct product = dataProducts.get(key);
    if (product != null && (!product.isDeleted() || lookupDeleted)) {
      return product;
    } else {
      throw new RestClientException("Data product not found", 404, 40470);
    }
  }

  @Override
  public RegisteredDataProduct getLatestDataProduct(
      String env, String cluster, String name, boolean lookupDeleted)
      throws IOException, RestClientException {
    List<Integer> versions = dataProducts.entrySet().stream()
        .filter(kv -> kv.getKey().getName().equals(name)
            && (!kv.getValue().isDeleted() || lookupDeleted))
        .map(kv -> kv.getKey().getVersion())
        .sorted()
        .collect(Collectors.toList());
    return versions.isEmpty()
        ? null
        : getDataProduct(env, cluster, name, versions.get(versions.size() - 1), lookupDeleted);
  }

  @Override
  public RegisteredDataProduct createDataProduct(
      String env,
      String cluster,
      DataProduct dataProduct)
      throws IOException, RestClientException {
    RegisteredDataProduct oldValue = getLatestDataProduct(
        env, cluster, dataProduct.getInfo().getName(), false);
    int newVersion = oldValue != null ? oldValue.getVersion() + 1 : 1;
    String newGuid = UUID.randomUUID().toString();
    RegisteredDataProduct value = new RegisteredDataProduct(newVersion, newGuid,
        dataProduct.getInfo(), dataProduct.getSchemas(), dataProduct.getConfigs(),
        System.currentTimeMillis(), false);
    if (oldValue != null
        && !oldValue.isDeleted()
        && value.isEquivalent(oldValue)) {
      return oldValue;
    }
    DataProductKey key = new DataProductKey(
        env, cluster, dataProduct.getInfo().getName(), newVersion);
    dataProducts.put(key, value);
    return value;
  }

  @Override
  public void deleteDataProduct(
      String env, String cluster, String name, boolean permanentDelete)
      throws IOException, RestClientException {
    if (permanentDelete) {
      for (Iterator<Map.Entry<DataProductKey, RegisteredDataProduct>> iter =
          dataProducts.entrySet().iterator();
          iter.hasNext(); ) {
        Map.Entry<DataProductKey, RegisteredDataProduct> entry = iter.next();
        DataProductKey key = entry.getKey();
        RegisteredDataProduct value = entry.getValue();
        if (key.getName().equals(name)) {
          if (!value.isDeleted()) {
            throw new RestClientException(
                "Data product " + name
                    + " was not deleted first before being permanently deleted",
                404,
                40471);
          }
        }
      }
      for (Iterator<Map.Entry<DataProductKey, RegisteredDataProduct>> iter =
          dataProducts.entrySet().iterator();
          iter.hasNext(); ) {
        Map.Entry<DataProductKey, RegisteredDataProduct> entry = iter.next();
        DataProductKey key = entry.getKey();
        if (key.getName().equals(name)) {
          iter.remove();
        }
      }
    } else {
      for (Iterator<Map.Entry<DataProductKey, RegisteredDataProduct>> iter =
          dataProducts.entrySet().iterator();
          iter.hasNext(); ) {
        Map.Entry<DataProductKey, RegisteredDataProduct> entry = iter.next();
        DataProductKey key = entry.getKey();
        RegisteredDataProduct value = entry.getValue();
        if (key.getName().equals(name) && !value.isDeleted()) {
          RegisteredDataProduct newValue = new RegisteredDataProduct(
              value.getVersion(), value.getGuid(), value.getInfo(),
              value.getSchemas(), value.getConfigs(), value.getTimestamp(), true);
          entry.setValue(newValue);
        }
      }
    }
  }

  @Override
  public void deleteDataProductVersion(
      String env, String cluster, String name, int version, boolean permanentDelete)
      throws IOException, RestClientException {
    DataProductKey key = new DataProductKey(env, cluster, name, version);
    RegisteredDataProduct value = dataProducts.get(key);
    if (value == null) {
      return;
    }
    if (permanentDelete) {
      if (!value.isDeleted()) {
        throw new RestClientException(
            "Data product " + name + " was not deleted first before being permanently deleted",
            404,
            40471);
      }
      dataProducts.remove(key);
    } else {
      if (!value.isDeleted()) {
        RegisteredDataProduct newValue = new RegisteredDataProduct(
            value.getVersion(), value.getGuid(), value.getInfo(),
            value.getSchemas(), value.getConfigs(), value.getTimestamp(), true);
        dataProducts.put(key, newValue);
      }
    }
  }

  @Override
  public void reset() {
    dataProducts.clear();
  }

  static class KekId {

    private final String name;

    public KekId(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KekId that = (KekId) o;
      return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }

  static class DekId {

    private final String kekName;
    private final String subject;
    private final Integer version;
    private final DekFormat dekFormat;

    public DekId(String kekName, String subject, Integer version, DekFormat dekFormat) {
      this.kekName = kekName;
      this.subject = subject;
      this.version = version;
      this.dekFormat = dekFormat;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DekId that = (DekId) o;
      return Objects.equals(kekName, that.kekName)
          && Objects.equals(subject, that.subject)
          && Objects.equals(version, that.version)
          && dekFormat == that.dekFormat;
    }

    @Override
    public int hashCode() {
      return Objects.hash(kekName, subject, version, dekFormat);
    }
  }

  public static class DataProductKey {

    private final String env;
    private final String cluster;
    private final String name;
    private final Integer version;

    public DataProductKey(
        String env, String cluster, String name, Integer version) {
      this.env = env;
      this.cluster = cluster;
      this.name = name;
      this.version = version;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DataProductKey key = (DataProductKey) o;
      return Objects.equals(env, key.env)
          && Objects.equals(cluster, key.cluster)
          && Objects.equals(name, key.name)
          && Objects.equals(version, key.version);
    }

    @Override
    public int hashCode() {
      return Objects.hash(env, cluster, name, version);
    }
  }
}
