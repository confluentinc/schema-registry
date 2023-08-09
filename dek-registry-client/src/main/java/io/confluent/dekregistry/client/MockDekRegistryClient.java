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

import static io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver.TEST_CLIENT;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriverManager;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MockDekRegistryClient implements DekRegistryClient {

  public static final byte[] EMPTY_AAD = new byte[0];
  public static final String KMS_TYPE_SUFFIX = "://";

  private final Map<String, ?> configs;
  private final Map<KekId, KekInfo> keks;
  private final Map<DekId, DekInfo> deks;
  private final Map<DekFormat, Cryptor> cryptors;

  public MockDekRegistryClient(Map<String, ?> configs) {
    this.configs = configs;
    this.keks = new ConcurrentHashMap<>();
    this.deks = new ConcurrentHashMap<>();
    this.cryptors = new ConcurrentHashMap<>();
  }

  protected Cryptor getCryptor(DekFormat dekFormat) {
    return cryptors.computeIfAbsent(dekFormat, k -> {
      try {
        return new Cryptor(dekFormat);
      } catch (GeneralSecurityException e) {
        throw new IllegalArgumentException("Invalid format " + dekFormat, e);
      }
    });
  }

  public List<String> listKeks(boolean lookupDeleted)
      throws IOException, RestClientException {
    return keks.entrySet().stream()
        .filter(kv -> !kv.getValue().isDeleted() || lookupDeleted)
        .map(kv -> kv.getKey().getName())
        .collect(Collectors.toList());
  }

  public Kek getKek(String name, boolean lookupDeleted)
      throws IOException, RestClientException {
    KekId keyId = new KekId(name);
    KekInfo key = keks.get(keyId);
    if (key != null && (!key.isDeleted() || lookupDeleted)) {
      return key;
    } else {
      throw new RestClientException("Key not found", 404, 40470);
    }
  }

  public List<String> listDeks(String kekName, boolean lookupDeleted)
      throws IOException, RestClientException {
    return deks.entrySet().stream()
        .filter(kv -> !kv.getValue().isDeleted() || lookupDeleted)
        .map(kv -> kv.getKey().getScope())
        .collect(Collectors.toList());
  }

  public Dek getDek(String name, String scope, boolean lookupDeleted)
      throws IOException, RestClientException {
    return getDek(name, scope, null, lookupDeleted);
  }

  public Dek getDek(String name, String scope, DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    DekId keyId = new DekId(name, scope, algorithm);
    DekInfo key = deks.get(keyId);
    if (key != null && (!key.isDeleted() || lookupDeleted)) {
      key = maybeGenerateRawDek(key);
      return key;
    } else {
      throw new RestClientException("Key not found", 404, 40470);
    }
  }

  public Kek createKek(
      String name,
      String kmsType,
      String kmsKeyId,
      Map<String, String> kmsProps,
      String doc,
      boolean shared)
      throws IOException, RestClientException {
    KekId keyId = new KekId(name);
    if (keks.containsKey(keyId)) {
      throw new RestClientException("Key " + name + " already exists", 409, 40972);
    }
    KekInfo key = new KekInfo(name, kmsType, kmsKeyId, kmsProps, doc, shared, false);
    keks.put(keyId, key);
    return key;
  }

  public Dek createDek(
      String kekName,
      String kmsType,
      String kmsKeyId,
      String scope,
      DekFormat algorithm,
      String encryptedKeyMaterial)
      throws IOException, RestClientException {
    DekId keyId = new DekId(kekName, scope, algorithm);
    if (deks.containsKey(keyId)) {
      throw new RestClientException("Key " + scope + " already exists", 409, 40972);
    }
    DekInfo key = new DekInfo(kekName, scope, algorithm, encryptedKeyMaterial, null, false);
    key = maybeGenerateEncryptedDek(key);
    deks.put(keyId, key);
    key = maybeGenerateRawDek(key);
    return key;
  }

  protected DekInfo maybeGenerateEncryptedDek(DekInfo key)
      throws IOException, RestClientException {
    try {
      if (key.getEncryptedKeyMaterial() == null) {
        Kek kek = getKek(key.getKekName(), true);
        Aead aead = getAead(configs, kek);
        // Generate new dek
        byte[] rawDek = getCryptor(key.getAlgorithm()).generateKey();
        byte[] encryptedDek = aead.encrypt(rawDek, EMPTY_AAD);
        String encryptedDekStr =
            new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);
        key = new DekInfo(key.getKekName(), key.getScope(), key.getAlgorithm(),
            encryptedDekStr, null, key.isDeleted());
      }
      return key;
    } catch (GeneralSecurityException e) {
      throw new RestClientException(e.getMessage(), 500, 50000);
    }
  }

  protected DekInfo maybeGenerateRawDek(DekInfo key)
      throws IOException, RestClientException {
    try {
      Kek kek = getKek(key.getKekName(), true);
      if (kek.isShared()) {
        // Decrypt dek
        Aead aead = getAead(configs, kek);
        byte[] encryptedDek = Base64.getDecoder().decode(
            key.getEncryptedKeyMaterial().getBytes(StandardCharsets.UTF_8));
        byte[] rawDek = aead.decrypt(encryptedDek, EMPTY_AAD);
        String rawDekStr =
            new String(Base64.getEncoder().encode(rawDek), StandardCharsets.UTF_8);
        // Copy dek
        key = new DekInfo(key.getKekName(), key.getScope(), key.getAlgorithm(),
            key.getEncryptedKeyMaterial(), rawDekStr, key.isDeleted());
      }
      return key;
    } catch (GeneralSecurityException e) {
      throw new RestClientException(e.getMessage(), 500, 50000);
    }
  }

  protected static Aead getAead(Map<String, ?> configs, Kek kek)
      throws GeneralSecurityException {
    String kekUrl = kek.getKmsType() + KMS_TYPE_SUFFIX + kek.getKmsKeyId();
    Map<String, Object> props = new HashMap<>(kek.getKmsProps());
    if (configs.containsKey(TEST_CLIENT)) {
      props.put(TEST_CLIENT, configs.get(TEST_CLIENT));
    }
    KmsClient kmsClient = getKmsClient(props, kekUrl);
    if (kmsClient == null) {
      throw new GeneralSecurityException("No kms client found for " + kekUrl);
    }
    return kmsClient.getAead(kekUrl);
  }

  protected static KmsClient getKmsClient(Map<String, ?> configs, String kekUrl)
      throws GeneralSecurityException {
    try {
      return KmsDriverManager.getDriver(kekUrl).getKmsClient(kekUrl);
    } catch (GeneralSecurityException e) {
      return KmsDriverManager.getDriver(kekUrl).registerKmsClient(configs, Optional.of(kekUrl));
    }
  }

  public Kek updateKek(
      String name,
      Map<String, String> kmsProps,
      String doc,
      Boolean shared)
      throws IOException, RestClientException {
    KekId keyId = new KekId(name);
    KekInfo key = keks.get(keyId);
    if (key == null) {
      throw new RestClientException("Key not found", 404, 40470);
    }
    if (kmsProps == null) {
      kmsProps = key.getKmsProps();
    }
    if (doc == null) {
      doc = key.getDoc();
    }
    if (shared == null) {
      shared = key.isShared();
    }
    KekInfo newKey = new KekInfo(name, key.getKmsType(),
        key.getKmsKeyId(), kmsProps, doc, shared, false);
    keks.put(keyId, newKey);
    return key;
  }

  public void deleteKek(String name, boolean permanentDelete)
      throws IOException, RestClientException {
    KekId keyId = new KekId(name);
    KekInfo key = keks.get(keyId);
    if (key == null) {
      return;
    }
    if (permanentDelete) {
      keks.remove(keyId);
    } else {
      KekInfo newKey = new KekInfo(name, key.getKmsType(),
          key.getKmsKeyId(), key.getKmsProps(), key.getDoc(), key.isShared(), true);
      keks.put(keyId, newKey);
    }
  }

  public void deleteDek(String name, String scope, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDek(name, scope, null, permanentDelete);

  }

  public void deleteDek(String name, String scope, DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    DekId keyId = new DekId(name, scope, algorithm);
    DekInfo key = deks.get(keyId);
    if (key == null) {
      return;
    }
    if (permanentDelete) {
      deks.remove(keyId);
    } else {
      DekInfo newKey = new DekInfo(name, key.getScope(), key.getAlgorithm(),
          key.getEncryptedKeyMaterial(), key.getKeyMaterial(), true);
      deks.put(keyId, newKey);
    }
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
    private final String scope;
    private final DekFormat dekFormat;

    public DekId(String kekName, String scope, DekFormat dekFormat) {
      this.kekName = kekName;
      this.scope = scope;
      this.dekFormat = dekFormat;
    }

    public String getKekName() {
      return kekName;
    }

    public String getScope() {
      return scope;
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
          && Objects.equals(scope, that.scope)
          && dekFormat == that.dekFormat;
    }

    @Override
    public int hashCode() {
      return Objects.hash(kekName, scope, dekFormat);
    }
  }

  static class KekInfo extends Kek {

    private final boolean deleted;

    public KekInfo(String name, String kmsType, String kmsKeyId, Map<String, String> kmsProps,
          String doc, boolean shared, boolean deleted) {
      super(name, kmsType, kmsKeyId, kmsProps, doc, shared);
      this.deleted = deleted;
    }

    public boolean isDeleted() {
      return deleted;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      KekInfo kekInfo = (KekInfo) o;
      return deleted == kekInfo.deleted;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), deleted);
    }
  }

  static class DekInfo extends Dek {

    private final boolean deleted;

    public DekInfo(String kekName, String scope, DekFormat algorithm,
        String encryptedKeyMaterial, String keyMaterial, boolean deleted) {
      super(kekName, scope, algorithm, encryptedKeyMaterial, keyMaterial);
      this.deleted = deleted;
    }

    public boolean isDeleted() {
      return deleted;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      DekInfo dekInfo = (DekInfo) o;
      return deleted == dekInfo.deleted;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), deleted);
    }
  }
}
