/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.protobuf.ByteString;
import io.confluent.dekregistry.client.CachedDekRegistryClient.DekId;
import io.confluent.dekregistry.client.CachedDekRegistryClient.KekId;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.DekRegistryClientFactory;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriverManager;
import io.confluent.kafka.schemaregistry.rules.FieldRuleExecutor;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.Type;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.config.ConfigException;

/**
 * In envelope encryption, a user generates a data encryption key (DEK) locally, encrypts data with
 * the DEK, sends the DEK to a KMS to be encrypted (with a key managed by KMS - KEK), and then
 * stores the encrypted DEK. At a later point, a user can retrieve the encrypted DEK for the
 * encrypted data, use the KEK from KMS to decrypt the DEK, and use the decrypted DEK to decrypt
 * the data.
 */
public class FieldEncryptionExecutor implements FieldRuleExecutor {

  public static final String TYPE = "ENCRYPT";

  public static final String ENCRYPT_KEK_NAME = "encrypt.kek.name";
  public static final String ENCRYPT_KMS_KEY_ID = "encrypt.kms.key.id";
  public static final String ENCRYPT_KMS_TYPE = "encrypt.kms.type";
  public static final String ENCRYPT_DEK_ALGORITHM = "encrypt.dek.algorithm";

  public static final String KMS_TYPE_SUFFIX = "://";
  public static final byte[] EMPTY_AAD = new byte[0];
  public static final String CACHE_EXPIRY_SECS = "cache.expiry.secs";
  public static final String CACHE_SIZE = "cache.size";

  private Map<DekFormat, Cryptor> cryptors;
  private Map<String, ?> configs;
  private int cacheExpirySecs = -1;
  private int cacheSize = 10000;
  private DekRegistryClient client;

  public FieldEncryptionExecutor() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.configs = configs;
    Object cacheExpirySecsConfig = configs.get(CACHE_EXPIRY_SECS);
    if (cacheExpirySecsConfig != null) {
      try {
        this.cacheExpirySecs = Integer.parseInt(cacheExpirySecsConfig.toString());
      } catch (NumberFormatException e) {
        throw new ConfigException("Cannot parse " + CACHE_EXPIRY_SECS);
      }
    }
    Object cacheSizeConfig = configs.get(CACHE_SIZE);
    if (cacheSizeConfig != null) {
      try {
        this.cacheSize = Integer.parseInt(cacheSizeConfig.toString());
      } catch (NumberFormatException e) {
        throw new ConfigException("Cannot parse " + CACHE_SIZE);
      }
    }
    Object url = configs.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    if (url == null) {
      throw new ConfigException("Missing schema registry url!");
    }
    List<String> baseUrls = Arrays.asList(url.toString().split("\\s*,\\s*"));
    this.client = DekRegistryClientFactory.newClient(
        baseUrls, cacheSize, cacheExpirySecs, configs, Collections.emptyMap());
    this.cryptors = new ConcurrentHashMap<>();
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public FieldTransform newTransform(RuleContext ctx) throws RuleException {
    FieldTransform transform = new FieldEncryptionExecutorTransform();
    transform.init(ctx);
    return transform;
  }

  private Cryptor getCryptor(RuleContext ctx) {
    String algorithm = ctx.getParameter(ENCRYPT_DEK_ALGORITHM);
    DekFormat dekFormat = algorithm != null
        ? DekFormat.valueOf(algorithm)
        : DekFormat.AES256_GCM;
    return getCryptor(dekFormat);
  }

  private Cryptor getCryptor(DekFormat dekFormat) {
    return cryptors.computeIfAbsent(dekFormat, k -> {
      try {
        return new Cryptor(dekFormat);
      } catch (GeneralSecurityException e) {
        throw new IllegalArgumentException("Invalid format " + dekFormat, e);
      }
    });
  }

  // Visible for testing
  public Map<DekFormat, Cryptor> getCryptors() {
    return cryptors;
  }

  private static byte[] toBytes(Type type, Object obj) {
    switch (type) {
      case BYTES:
        if (obj instanceof ByteBuffer) {
          return ((ByteBuffer) obj).array();
        } else if (obj instanceof ByteString) {
          return ((ByteString) obj).toByteArray();
        } else if (obj instanceof byte[]) {
          return (byte[]) obj;
        } else {
          throw new IllegalArgumentException(
              "Unrecognized bytes object of type: " + obj.getClass().getName());
        }
      case STRING:
        return obj.toString().getBytes(StandardCharsets.UTF_8);
      default:
        return null;
    }
  }

  private static Object toObject(Type type, byte[] bytes) {
    switch (type) {
      case BYTES:
        return bytes;
      case STRING:
        return new String(bytes, StandardCharsets.UTF_8);
      default:
        return null;
    }
  }

  @Override
  public void close() throws RuleException {
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        throw new RuleException(e);
      }
    }
  }

  class FieldEncryptionExecutorTransform implements FieldTransform {
    private Cryptor cryptor;
    private String kekName;
    private KekInfo kek;

    public void init(RuleContext ctx) throws RuleException {
      cryptor = getCryptor(ctx);
      kekName = getKekName(ctx);
      kek = getKek(ctx, kekName);
    }

    protected String getKekName(RuleContext ctx) throws RuleException {
      String name = ctx.getParameter(ENCRYPT_KEK_NAME);
      if (name == null) {
        throw new RuleException("No kek name found");
      }
      int length = name.length();
      if (length == 0) {
        throw new RuleException("Empty kek name");
      }
      char first = name.charAt(0);
      if (!(Character.isLetter(first) || first == '_')) {
        throw new RuleException("Illegal initial character in kek name: " + name);
      }
      for (int i = 1; i < length; i++) {
        char c = name.charAt(i);
        if (!(Character.isLetterOrDigit(c) || c == '_' || c == '-')) {
          throw new RuleException("Illegal character in kek name: " + name);
        }
      }
      return name;
    }

    protected KekInfo getKek(RuleContext ctx, String kekName) throws RuleException {
      KekId kekId = new KekId(kekName, ctx.ruleMode() == RuleMode.READ);
      String kmsType = ctx.getParameter(ENCRYPT_KMS_TYPE);
      String kmsKeyId = ctx.getParameter(ENCRYPT_KMS_KEY_ID);

      KekInfo kek = retrieveKekFromRegistry(ctx, kekId);
      if (kek == null) {
        if (ctx.ruleMode() == RuleMode.READ) {
          throw new RuleException("No kek found for " + kekName + " during consume");
        }
        if (kmsType == null) {
          throw new RuleException("No kms type found for " + kekName + " during produce");
        }
        if (kmsKeyId == null) {
          throw new RuleException("No kms key id found for " + kekName + " during produce");
        }
        kek = new KekInfo(kmsType, kmsKeyId, false);
        kek = storeKekToRegistry(ctx, kekId, kek);
        if (kek == null) {
          // Handle conflicts (409)
          kek = retrieveKekFromRegistry(ctx, kekId);
        }
        if (kek == null) {
          throw new RuleException("No kek found for " + kekName + " during produce");
        }
      }
      if (kmsType != null && !kmsType.equals(kek.getKmsType())) {
        throw new RuleException("Found " + kekName + " with different kms type: "
            + kek.getKmsType());
      }
      if (kmsKeyId != null && !kmsKeyId.equals(kek.getKmsKeyId())) {
        throw new RuleException("Found " + kekName + " with different kms key id: "
            + kek.getKmsKeyId());
      }
      return kek;
    }

    private KekInfo retrieveKekFromRegistry(RuleContext ctx, KekId key) throws RuleException {
      try {
        Kek kek = client.getKek(key.getName(), key.isLookupDeleted());
        if (kek == null) {
          return null;
        }
        return new KekInfo(kek.getKmsType(), kek.getKmsKeyId(), kek.isShared());
      } catch (RestClientException e) {
        if (e.getStatus() == 404) {
          return null;
        }
        throw new RuleException("Could not get kek", e);
      } catch (IOException e) {
        throw new RuleException("Could not get kek", e);
      }
    }

    private KekInfo storeKekToRegistry(RuleContext ctx, KekId key, KekInfo kekInfo)
        throws RuleException {
      try {
        Kek kek = client.createKek(
            key.getName(), kekInfo.getKmsType(), kekInfo.getKmsKeyId(),
            null, null, kekInfo.isShared());
        return new KekInfo(kek.getKmsType(), kek.getKmsKeyId(), kek.isShared());
      } catch (RestClientException e) {
        if (e.getStatus() == 409) {
          return null;
        }
        throw new RuleException("Could not store kek", e);
      } catch (IOException e) {
        throw new RuleException("Could not store kek", e);
      }
    }

    protected DekInfo getDek(RuleContext ctx, String kekName, KekInfo kek)
        throws RuleException, GeneralSecurityException {
      DekId dekId = new DekId(
          kekName, ctx.subject(), cryptor.getDekFormat(), ctx.ruleMode() == RuleMode.READ);

      Aead aead = null;
      DekInfo dek = retrieveDekFromRegistry(ctx, dekId);
      if (dek == null) {
        if (ctx.ruleMode() == RuleMode.READ) {
          throw new RuleException("No dek found for " + kekName + " during consume");
        }
        if (!kek.isShared()) {
          aead = getAead(configs, kek);
          // Generate new dek
          byte[] rawDek = getCryptor(dekId.getDekFormat()).generateKey();
          byte[] encryptedDek = aead.encrypt(rawDek, EMPTY_AAD);
          dek = new DekInfo(rawDek, encryptedDek);
        }
        // dek may be passed as null if kek is shared
        dek = storeDekToRegistry(ctx, dekId, dek);
        if (dek == null) {
          // Handle conflicts (409)
          dek = retrieveDekFromRegistry(ctx, dekId);
        }
        if (dek == null) {
          throw new RuleException("No dek found for " + kekName + " during produce");
        }
      }
      if (dek.getRawDek() == null) {
        if (aead == null) {
          aead = getAead(configs, kek);
        }
        byte[] rawDek = aead.decrypt(dek.getEncryptedDek(), EMPTY_AAD);
        dek.setRawDek(rawDek);
      }
      return dek;
    }

    private DekInfo retrieveDekFromRegistry(RuleContext ctx, DekId key) throws RuleException {
      try {
        Dek dek = client.getDek(
            key.getKekName(), key.getSubject(), key.getDekFormat(), key.isLookupDeleted());
        if (dek == null) {
          return null;
        }
        byte[] rawDek = dek.getKeyMaterial() != null
            ? Base64.getDecoder().decode(toBytes(Type.STRING, dek.getKeyMaterial()))
            : null;
        byte[] encryptedDek = dek.getEncryptedKeyMaterial() != null
            ? Base64.getDecoder().decode(toBytes(Type.STRING, dek.getEncryptedKeyMaterial()))
            : null;
        return encryptedDek != null ? new DekInfo(rawDek, encryptedDek) : null;
      } catch (RestClientException e) {
        if (e.getStatus() == 404) {
          return null;
        }
        throw new RuleException("Could not get dek", e);
      } catch (IOException e) {
        throw new RuleException("Could not get dek", e);
      }
    }

    private DekInfo storeDekToRegistry(RuleContext ctx, DekId key, DekInfo dekInfo)
        throws RuleException {
      try {
        String encryptedDekStr = dekInfo != null && dekInfo.getEncryptedDek() != null
            ? (String) toObject(Type.STRING, Base64.getEncoder().encode(dekInfo.getEncryptedDek()))
            : null;
        Dek dek = client.createDek(
            key.getKekName(), key.getSubject(), key.getDekFormat(), encryptedDekStr);
        byte[] rawDek = dek.getKeyMaterial() != null
            ? Base64.getDecoder().decode(toBytes(Type.STRING, dek.getKeyMaterial()))
            : null;
        byte[] encryptedDek = dek.getEncryptedKeyMaterial() != null
            ? Base64.getDecoder().decode(toBytes(Type.STRING, dek.getEncryptedKeyMaterial()))
            : null;
        return new DekInfo(rawDek, encryptedDek);
      } catch (RestClientException e) {
        if (e.getStatus() == 409) {
          return null;
        }
        throw new RuleException("Could not store dek", e);
      } catch (IOException e) {
        throw new RuleException("Could not store dek", e);
      }
    }

    public Object transform(RuleContext ctx, FieldContext fieldCtx, Object fieldValue)
        throws RuleException {
      try {
        DekInfo dek = getDek(ctx, kekName, kek);
        byte[] plaintext;
        byte[] ciphertext;
        switch (ctx.ruleMode()) {
          case WRITE:
            plaintext = toBytes(fieldCtx.getType(), fieldValue);
            if (plaintext == null) {
              return fieldValue;
            }
            ciphertext = cryptor.encrypt(dek.getRawDek(), plaintext, EMPTY_AAD);
            if (fieldCtx.getType() == Type.STRING) {
              ciphertext = Base64.getEncoder().encode(ciphertext);
            }
            return toObject(fieldCtx.getType(), ciphertext);
          case READ:
            ciphertext = toBytes(fieldCtx.getType(), fieldValue);
            if (fieldCtx.getType() == Type.STRING) {
              ciphertext = Base64.getDecoder().decode(ciphertext);
            }
            plaintext = cryptor.decrypt(dek.getRawDek(), ciphertext, EMPTY_AAD);
            Object result = toObject(fieldCtx.getType(), plaintext);
            return result != null ? result : fieldValue;
          default:
            throw new IllegalArgumentException("Unsupported rule mode " + ctx.ruleMode());
        }
      } catch (GeneralSecurityException e) {
        throw new RuleException(e);
      }
    }

    @Override
    public void close() {
    }
  }

  private static Aead getAead(Map<String, ?> configs, KekInfo kek)
      throws GeneralSecurityException, RuleException {
    String kekUrl = kek.getKmsType() + KMS_TYPE_SUFFIX + kek.getKmsKeyId();
    KmsClient kmsClient = getKmsClient(configs, kekUrl);
    if (kmsClient == null) {
      throw new RuleException("No kms client found for " + kekUrl);
    }
    return kmsClient.getAead(kekUrl);
  }

  private static KmsClient getKmsClient(Map<String, ?> configs, String kekUrl)
      throws GeneralSecurityException {
    try {
      return KmsDriverManager.getDriver(kekUrl).getKmsClient(kekUrl);
    } catch (GeneralSecurityException e) {
      return KmsDriverManager.getDriver(kekUrl).registerKmsClient(configs, Optional.of(kekUrl));
    }
  }

  static class KekInfo {

    private final String kmsType;
    private final String kmsKeyId;
    private final boolean shared;

    public KekInfo(String kmsType, String kmsKeyId, boolean shared) {
      this.kmsType = kmsType;
      this.kmsKeyId = kmsKeyId;
      this.shared = shared;
    }

    public String getKmsType() {
      return kmsType;
    }

    public String getKmsKeyId() {
      return kmsKeyId;
    }

    public boolean isShared() {
      return shared;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KekInfo kek = (KekInfo) o;
      return shared == kek.shared
          && Objects.equals(kmsType, kek.kmsType)
          && Objects.equals(kmsKeyId, kek.kmsKeyId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kmsType, kmsKeyId, shared);
    }
  }

  static class DekInfo {

    private byte[] rawDek;
    private final byte[] encryptedDek;

    public DekInfo(byte[] rawDek, byte[] encryptedDek) {
      this.rawDek = rawDek;
      this.encryptedDek = encryptedDek;
    }

    public byte[] getRawDek() {
      return rawDek;
    }

    public void setRawDek(byte[] rawDek) {
      this.rawDek = rawDek;
    }

    public byte[] getEncryptedDek() {
      return encryptedDek;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DekInfo dek = (DekInfo) o;
      return Arrays.equals(rawDek, dek.rawDek)
          && Arrays.equals(encryptedDek, dek.encryptedDek);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(rawDek);
      result = 31 * result + Arrays.hashCode(encryptedDek);
      return result;
    }
  }
}

