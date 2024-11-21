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
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MockDekRegistryClient implements DekRegistryClient {

  public static final byte[] EMPTY_AAD = new byte[0];

  private final Map<String, ?> configs;
  private final Map<KekId, Kek> keks;
  private final Map<DekId, Dek> deks;
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
  public List<Integer> listDekVersions(String kekName, String subject,
      DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    DekFormat algo = algorithm;
    return deks.entrySet().stream()
        .filter(kv -> kv.getKey().getKekName().equals(kekName)
            && kv.getKey().getSubject().equals(subject)
            && kv.getValue().getAlgorithm().equals(algo)
            && (!kv.getValue().isDeleted() || lookupDeleted))
        .map(kv -> kv.getKey().getVersion())
        .collect(Collectors.toList());
  }

  @Override
  public Kek getKek(String name, boolean lookupDeleted)
      throws IOException, RestClientException {
    KekId keyId = new KekId(name);
    Kek key = keks.get(keyId);
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
  public Dek getDek(String kekName, String subject, DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    return getDekVersion(kekName, subject, 1, algorithm, lookupDeleted);
  }

  @Override
  public Dek getDekVersion(String kekName, String subject, int version,
      DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    if (version == LATEST_VERSION) {
      return getDekLatestVersion(kekName, subject, algorithm, lookupDeleted);
    }
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    DekId keyId = new DekId(kekName, subject, version, algorithm);
    Dek key = deks.get(keyId);
    if (key != null && (!key.isDeleted() || lookupDeleted)) {
      key = maybeGenerateRawDek(key);
      return key;
    } else {
      throw new RestClientException("Key not found", 404, 40470);
    }
  }

  @Override
  public Dek getDekLatestVersion(String kekName, String subject,
      DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    DekFormat algo = algorithm;
    List<Integer> versions = deks.entrySet().stream()
        .filter(kv -> kv.getKey().getKekName().equals(kekName)
            && kv.getKey().getSubject().equals(subject)
            && kv.getValue().getAlgorithm().equals(algo)
            && (!kv.getValue().isDeleted() || lookupDeleted))
        .map(kv -> kv.getKey().getVersion())
        .sorted()
        .collect(Collectors.toList());
    return versions.isEmpty()
        ? null
        : getDekVersion(kekName, subject, versions.get(versions.size() - 1),
            algorithm, lookupDeleted);
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
    return createKek(name, kmsType, kmsKeyId, kmsProps, doc, shared, false);
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
    KekId keyId = new KekId(name);
    Kek oldKey = keks.get(keyId);
    Kek key = new Kek(name, kmsType, kmsKeyId,
        kmsProps, doc, shared, System.currentTimeMillis(), deleted);
    if (oldKey != null
        && (deleted == oldKey.isDeleted() || !oldKey.equals(key))) {
      throw new RestClientException("Key " + name + " already exists", 409, 40972);
    }
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
    return createDek(kekName, subject, 1, algorithm, encryptedKeyMaterial, false);
  }

  @Override
  public Dek createDek(
      String kekName,
      String subject,
      int version,
      DekFormat algorithm,
      String encryptedKeyMaterial)
      throws IOException, RestClientException {
    return createDek(kekName, subject, version, algorithm, encryptedKeyMaterial, false);
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
    DekId keyId = new DekId(kekName, subject, version, algorithm);
    Dek oldKey = deks.get(keyId);
    Dek key = new Dek(kekName, subject, version, algorithm,
        encryptedKeyMaterial, null, System.currentTimeMillis(), deleted);
    key = maybeGenerateEncryptedDek(key);
    if (key.getEncryptedKeyMaterial() == null) {
      throw new RestClientException("Could not generate dek for " + subject, 500, 50070);
    }
    if (oldKey != null
        && (deleted == oldKey.isDeleted() || !oldKey.equals(key))) {
      throw new RestClientException("Key " + subject + " already exists", 409, 40972);
    }
    deks.put(keyId, key);
    key = maybeGenerateRawDek(key);
    return key;
  }

  protected Dek maybeGenerateEncryptedDek(Dek key)
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
        key = new Dek(key.getKekName(), key.getSubject(), key.getVersion(), key.getAlgorithm(),
            encryptedDekStr, null, key.getTimestamp(), key.isDeleted());
      }
      return key;
    } catch (GeneralSecurityException e) {
      return key;
    }
  }

  protected Dek maybeGenerateRawDek(Dek key)
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
        key = new Dek(key.getKekName(), key.getSubject(), key.getVersion(), key.getAlgorithm(),
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
    Kek key = keks.get(keyId);
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
    Kek newKey = new Kek(name, key.getKmsType(), key.getKmsKeyId(),
        kmsProps, doc, shared, System.currentTimeMillis(), false);
    keks.put(keyId, newKey);
    return key;
  }

  @Override
  public void deleteKek(String kekName, boolean permanentDelete)
      throws IOException, RestClientException {
    KekId keyId = new KekId(kekName);
    Kek key = keks.get(keyId);
    if (key == null) {
      return;
    }
    if (permanentDelete) {
      if (!key.isDeleted()) {
        throw new RestClientException(
            "Key " + kekName
                + " was not deleted first before being permanently deleted",
            404,
            40471);
      }
      keks.remove(keyId);
    } else {
      if (!key.isDeleted()) {
        Kek newKey = new Kek(kekName, key.getKmsType(), key.getKmsKeyId(),
            key.getKmsProps(), key.getDoc(), key.isShared(), System.currentTimeMillis(), true);
        keks.put(keyId, newKey);
      }
    }
  }

  @Override
  public void deleteDek(
      String kekName, String subject, DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    if (permanentDelete) {
      for (Iterator<Map.Entry<DekId, Dek>> iter = deks.entrySet().iterator();
          iter.hasNext(); ) {
        Map.Entry<DekId, Dek> entry = iter.next();
        DekId dekId = entry.getKey();
        Dek dekInfo = entry.getValue();
        if (dekId.getKekName().equals(kekName)
            && dekId.getSubject().equals(subject)
            && dekInfo.getAlgorithm().equals(algorithm)) {
          if (!dekInfo.isDeleted()) {
            throw new RestClientException(
                "Key " + dekId.getKekName()
                    + " was not deleted first before being permanently deleted",
                404,
                40471);
          }
        }
      }
      for (Iterator<Map.Entry<DekId, Dek>> iter = deks.entrySet().iterator();
          iter.hasNext(); ) {
        Map.Entry<DekId, Dek> entry = iter.next();
        DekId dekId = entry.getKey();
        Dek dekInfo = entry.getValue();
        if (dekId.getKekName().equals(kekName)
            && dekId.getSubject().equals(subject)
            && dekInfo.getAlgorithm().equals(algorithm)) {
          iter.remove();
        }
      }
    } else {
      for (Iterator<Map.Entry<DekId, Dek>> iter = deks.entrySet().iterator();
          iter.hasNext(); ) {
        Map.Entry<DekId, Dek> entry = iter.next();
        DekId dekId = entry.getKey();
        Dek dekInfo = entry.getValue();
        if (dekId.getKekName().equals(kekName)
            && dekId.getSubject().equals(subject)
            && dekInfo.getAlgorithm().equals(algorithm)
            && !dekInfo.isDeleted()) {
          Dek newKey = new Dek(kekName, dekInfo.getSubject(),
              dekInfo.getVersion(), dekInfo.getAlgorithm(), dekInfo.getEncryptedKeyMaterial(),
              dekInfo.getKeyMaterial(), dekInfo.getTimestamp(), true);
          entry.setValue(newKey);
        }
      }
    }
  }

  @Override
  public void deleteDekVersion(
      String kekName, String subject, int version, DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    DekId keyId = new DekId(kekName, subject, version, algorithm);
    Dek key = deks.get(keyId);
    if (key == null) {
      return;
    }
    if (permanentDelete) {
      if (!key.isDeleted()) {
        throw new RestClientException(
            "Key " + key.getKekName() + " was not deleted first before being permanently deleted",
            404,
            40471);
      }
      deks.remove(keyId);
    } else {
      if (!key.isDeleted()) {
        Dek newKey = new Dek(kekName, key.getSubject(), key.getVersion(),
            key.getAlgorithm(),
            key.getEncryptedKeyMaterial(), key.getKeyMaterial(), System.currentTimeMillis(), true);
        deks.put(keyId, newKey);
      }
    }
  }

  @Override
  public void undeleteKek(String kekName)
      throws IOException, RestClientException {
    KekId keyId = new KekId(kekName);
    Kek key = keks.get(keyId);
    if (key == null) {
      return;
    }
    if (key.isDeleted()) {
      Kek newKey = new Kek(kekName, key.getKmsType(), key.getKmsKeyId(),
          key.getKmsProps(), key.getDoc(), key.isShared(), System.currentTimeMillis(), false);
      keks.put(keyId, newKey);
    }
  }

  @Override
  public void undeleteDek(
      String kekName, String subject, DekFormat algorithm)
      throws IOException, RestClientException {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    KekId keyId = new KekId(kekName);
    Kek key = keks.get(keyId);
    if (key == null) {
      return;
    }
    if (key.isDeleted()) {
      throw new RestClientException(
          "Key " + kekName + " must be undeleted first",
          404,
          40472);
    }
    for (Iterator<Map.Entry<DekId, Dek>> iter = deks.entrySet().iterator();
        iter.hasNext(); ) {
      Map.Entry<DekId, Dek> entry = iter.next();
      DekId dekId = entry.getKey();
      Dek dekInfo = entry.getValue();
      if (dekId.getKekName().equals(kekName)
          && dekId.getSubject().equals(subject)
          && dekInfo.getAlgorithm().equals(algorithm)
          && !dekInfo.isDeleted()) {
        Dek newKey = new Dek(kekName, dekInfo.getSubject(),
            dekInfo.getVersion(), dekInfo.getAlgorithm(), dekInfo.getEncryptedKeyMaterial(),
            dekInfo.getKeyMaterial(), dekInfo.getTimestamp(), false);
        entry.setValue(newKey);
      }
    }
  }

  @Override
  public void undeleteDekVersion(
      String kekName, String subject, int version, DekFormat algorithm)
      throws IOException, RestClientException {
    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    KekId keyId = new KekId(kekName);
    Kek key = keks.get(keyId);
    if (key == null) {
      return;
    }
    if (key.isDeleted()) {
      throw new RestClientException(
          "Key " + kekName + " must be undeleted first",
          404,
          40472);
    }
    DekId id = new DekId(kekName, subject, version, algorithm);
    Dek oldKey = deks.get(id);
    if (oldKey == null) {
      return;
    }
    if (oldKey.isDeleted()) {
      Dek newKey = new Dek(kekName, oldKey.getSubject(), oldKey.getVersion(),
          oldKey.getAlgorithm(),
          oldKey.getEncryptedKeyMaterial(), oldKey.getKeyMaterial(), System.currentTimeMillis(),
          false);
      deks.put(id, newKey);
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
}
