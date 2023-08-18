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

import com.google.crypto.tink.Aead;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MockDekRegistryClient implements DekRegistryClient {

  public static final byte[] EMPTY_AAD = new byte[0];

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

  @Override
  public List<String> listKeks(boolean lookupDeleted)
      throws IOException, RestClientException {
    return keks.entrySet().stream()
        .filter(kv -> !kv.getValue().isDeleted() || lookupDeleted)
        .map(kv -> kv.getKey().getName())
        .collect(Collectors.toList());
  }

  @Override
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

  @Override
  public List<String> listDeks(String kekName, boolean lookupDeleted)
      throws IOException, RestClientException {
    return deks.entrySet().stream()
        .filter(kv -> !kv.getValue().isDeleted() || lookupDeleted)
        .map(kv -> kv.getKey().getSubject())
        .collect(Collectors.toList());
  }

  @Override
  public Dek getDek(String kekName, String subject, boolean lookupDeleted)
      throws IOException, RestClientException {
    return getDek(kekName, subject, null, lookupDeleted);
  }

  @Override
  public Dek getDek(String kekName, String subject, DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    DekId keyId = new DekId(kekName, subject, algorithm);
    DekInfo key = deks.get(keyId);
    if (key != null && (!key.isDeleted() || lookupDeleted)) {
      key = maybeGenerateRawDek(key);
      return key;
    } else {
      throw new RestClientException("Key not found", 404, 40470);
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
    KekId keyId = new KekId(name);
    if (keks.containsKey(keyId)) {
      throw new RestClientException("Key " + name + " already exists", 409, 40972);
    }
    KekInfo key = new KekInfo(name, kmsType, kmsKeyId,
        kmsProps, doc, shared, System.currentTimeMillis(), false);
    keks.put(keyId, key);
    return key;
  }

  @Override
  public Dek createDek(
      String kekName,
      String subject,
      DekFormat algorithm,
      String encryptedKeyMaterial)
      throws IOException, RestClientException {
    DekId keyId = new DekId(kekName, subject, algorithm);
    if (deks.containsKey(keyId)) {
      throw new RestClientException("Key " + subject + " already exists", 409, 40972);
    }
    // NOTE (version): in the future we may allow a version to be passed
    int version = 1;
    DekInfo key = new DekInfo(kekName, subject, version, algorithm,
        encryptedKeyMaterial, null, System.currentTimeMillis(), false);
    key = maybeGenerateEncryptedDek(key);
    if (key.getEncryptedKeyMaterial() == null) {
      throw new RestClientException("Could not generate dek for " + subject, 500, 50070);
    }
    deks.put(keyId, key);
    key = maybeGenerateRawDek(key);
    return key;
  }

  protected DekInfo maybeGenerateEncryptedDek(DekInfo key)
      throws IOException, RestClientException {
    try {
      if (key.getEncryptedKeyMaterial() == null) {
        Kek kek = getKek(key.getKekName(), true);
        Aead aead = kek.toAead(configs);
        // Generate new dek
        byte[] rawDek = getCryptor(key.getAlgorithm()).generateKey();
        byte[] encryptedDek = aead.encrypt(rawDek, EMPTY_AAD);
        String encryptedDekStr =
            new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);
        key = new DekInfo(key.getKekName(), key.getSubject(), key.getVersion(), key.getAlgorithm(),
            encryptedDekStr, null, key.getTimestamp(), key.isDeleted());
      }
      return key;
    } catch (GeneralSecurityException e) {
      return key;
    }
  }

  protected DekInfo maybeGenerateRawDek(DekInfo key)
      throws IOException, RestClientException {
    try {
      Kek kek = getKek(key.getKekName(), true);
      if (kek.isShared()) {
        // Decrypt dek
        Aead aead = kek.toAead(configs);
        byte[] encryptedDek = Base64.getDecoder().decode(
            key.getEncryptedKeyMaterial().getBytes(StandardCharsets.UTF_8));
        byte[] rawDek = aead.decrypt(encryptedDek, EMPTY_AAD);
        String rawDekStr =
            new String(Base64.getEncoder().encode(rawDek), StandardCharsets.UTF_8);
        // Copy dek
        key = new DekInfo(key.getKekName(), key.getSubject(), key.getVersion(), key.getAlgorithm(),
            key.getEncryptedKeyMaterial(), rawDekStr, key.getTimestamp(), key.isDeleted());
      }
      return key;
    } catch (GeneralSecurityException e) {
      return key;
    }
  }

  @Override
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
    KekInfo newKey = new KekInfo(name, key.getKmsType(), key.getKmsKeyId(),
        kmsProps, doc, shared, System.currentTimeMillis(), false);
    keks.put(keyId, newKey);
    return key;
  }

  @Override
  public void deleteKek(String kekName, boolean permanentDelete)
      throws IOException, RestClientException {
    KekId keyId = new KekId(kekName);
    KekInfo key = keks.get(keyId);
    if (key == null) {
      return;
    }
    if (permanentDelete) {
      keks.remove(keyId);
    } else {
      KekInfo newKey = new KekInfo(kekName, key.getKmsType(), key.getKmsKeyId(),
          key.getKmsProps(), key.getDoc(), key.isShared(), System.currentTimeMillis(), true);
      keks.put(keyId, newKey);
    }
  }

  @Override
  public void deleteDek(String kekName, String subject, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDek(kekName, subject, null, permanentDelete);

  }

  @Override
  public void deleteDek(
      String kekName, String subject, DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    DekId keyId = new DekId(kekName, subject, algorithm);
    DekInfo key = deks.get(keyId);
    if (key == null) {
      return;
    }
    if (permanentDelete) {
      deks.remove(keyId);
    } else {
      DekInfo newKey = new DekInfo(kekName, key.getSubject(), key.getVersion(), key.getAlgorithm(),
          key.getEncryptedKeyMaterial(), key.getKeyMaterial(), System.currentTimeMillis(), true);
      deks.put(keyId, newKey);
    }
  }

  @Override
  public void reset() {
    keks.clear();
    deks.clear();
    cryptors.clear();
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
    private final DekFormat dekFormat;

    public DekId(String kekName, String subject, DekFormat dekFormat) {
      this.kekName = kekName;
      this.subject = subject;
      this.dekFormat = dekFormat;
    }

    public String getKekName() {
      return kekName;
    }

    public String getSubject() {
      return subject;
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
          && dekFormat == that.dekFormat;
    }

    @Override
    public int hashCode() {
      return Objects.hash(kekName, subject, dekFormat);
    }
  }

  static class KekInfo extends Kek {

    private final boolean deleted;

    public KekInfo(String name, String kmsType, String kmsKeyId, Map<String, String> kmsProps,
          String doc, boolean shared, Long timestamp, boolean deleted) {
      super(name, kmsType, kmsKeyId, kmsProps, doc, shared, timestamp);
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

    public DekInfo(String kekName, String subject, int version, DekFormat algorithm,
        String encryptedKeyMaterial, String keyMaterial, Long timestamp, boolean deleted) {
      super(kekName, subject, version, algorithm, encryptedKeyMaterial, keyMaterial, timestamp);
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
