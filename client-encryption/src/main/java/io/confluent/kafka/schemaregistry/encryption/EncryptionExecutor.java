/*
 * Copyright 2025 Confluent Inc.
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
import com.google.crypto.tink.proto.AesGcmKey;
import com.google.crypto.tink.proto.AesSivKey;
import com.google.protobuf.ByteString;
import io.confluent.dekregistry.client.CachedDekRegistryClient.DekId;
import io.confluent.dekregistry.client.CachedDekRegistryClient.KekId;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.DekRegistryClientFactory;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriverManager;
import io.confluent.kafka.schemaregistry.rules.RuleClientException;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.Type;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In envelope encryption, a user generates a data encryption key (DEK) locally, encrypts data with
 * the DEK, sends the DEK to a KMS to be encrypted (with a key managed by KMS - KEK), and then
 * stores the encrypted DEK. At a later point, a user can retrieve the encrypted DEK for the
 * encrypted data, use the KEK from KMS to decrypt the DEK, and use the decrypted DEK to decrypt
 * the data.
 */
public class EncryptionExecutor implements RuleExecutor {

  private static final Logger log = LoggerFactory.getLogger(EncryptionExecutor.class);

  public static final String TYPE = "ENCRYPT_PAYLOAD";

  public static final String ENCRYPT_KEK_NAME = "encrypt.kek.name";
  public static final String ENCRYPT_KMS_KEY_ID = "encrypt.kms.key.id";
  public static final String ENCRYPT_KMS_TYPE = "encrypt.kms.type";
  public static final String ENCRYPT_DEK_ALGORITHM = "encrypt.dek.algorithm";
  public static final String ENCRYPT_DEK_EXPIRY_DAYS = "encrypt.dek.expiry.days";
  public static final String ENCRYPT_ALTERNATE_KMS_KEY_IDS = "encrypt.alternate.kms.key.ids";

  public static final String KMS_TYPE_SUFFIX = "://";
  public static final byte[] EMPTY_AAD = new byte[0];
  public static final String CACHE_EXPIRY_SECS = "cache.expiry.secs";
  public static final String CACHE_SIZE = "cache.size";
  public static final String CLOCK = "clock";

  protected static final int LATEST_VERSION = -1;
  protected static final byte MAGIC_BYTE = 0x0;
  protected static final int MILLIS_IN_DAY = 24 * 60 * 60 * 1000;
  protected static final int VERSION_SIZE = 4;

  private Map<DekFormat, Cryptor> cryptors;
  private Map<String, ?> configs;
  private int cacheExpirySecs = -1;
  private int cacheSize = 10000;
  private Clock clock = Clock.systemUTC();
  private DekRegistryClient client;

  public EncryptionExecutor() {
  }

  @Override
  public boolean addOriginalConfigs() {
    return true;
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
    Object clock = configs.get(CLOCK);
    if (clock instanceof Clock) {
      this.clock = (Clock) clock;
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
  public Object transform(RuleContext ctx, Object message) throws RuleException {
    EncryptionExecutorTransform transform = newTransform(ctx);
    return transform.transform(ctx, Type.BYTES, message);
  }

  public EncryptionExecutorTransform newTransform(RuleContext ctx) throws RuleException {
    EncryptionExecutorTransform transform = new EncryptionExecutorTransform();
    transform.init(ctx);
    return transform;
  }

  private Cryptor getCryptor(RuleContext ctx) {
    String algorithm = ctx.getParameter(ENCRYPT_DEK_ALGORITHM);
    DekFormat dekFormat = algorithm != null && !algorithm.isEmpty()
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

  private byte[] generateKey(DekFormat dekFormat) throws GeneralSecurityException {
    byte[] dek = generateDek(dekFormat);
    if (dek != null) {
      switch (dekFormat) {
        case AES128_GCM:
        case AES256_GCM:
          return AesGcmKey.newBuilder()
              .setKeyValue(ByteString.copyFrom(dek))
              .build()
              .toByteArray();
        case AES256_SIV:
          return AesSivKey.newBuilder()
              .setKeyValue(ByteString.copyFrom(dek))
              .build()
              .toByteArray();
        default:
          throw new IllegalArgumentException("Invalid format " + dekFormat);
      }
    } else {
      return getCryptor(dekFormat).generateKey();
    }
  }

  // Can be overridden to generate a custom dek
  protected byte[] generateDek(DekFormat dekFormat) throws GeneralSecurityException {
    return null;
  }

  private static byte[] toBytes(Type type, Object obj) {
    switch (type) {
      case BYTES:
        if (obj instanceof ByteBuffer) {
          ByteBuffer buffer = (ByteBuffer) obj;
          int remainingSize = buffer.remaining();
          byte[] remaining = new byte[remainingSize];
          buffer.get(remaining, 0, remainingSize);
          return remaining;
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

  public class EncryptionExecutorTransform {
    private Cryptor cryptor;
    private String kekName;
    private Kek kek;
    private int dekExpiryDays;

    public void init(RuleContext ctx) throws RuleException {
      cryptor = getCryptor(ctx);
      kekName = getKekName(ctx);
      kek = getOrCreateKek(ctx);
      dekExpiryDays = getDekExpiryDays(ctx);
    }

    public boolean isDekRotated() {
      return dekExpiryDays > 0;
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

    protected Kek getOrCreateKek(RuleContext ctx) throws RuleException {
      boolean isRead = ctx.ruleMode() == RuleMode.READ;
      KekId kekId = new KekId(kekName, isRead);

      String kmsType = ctx.getParameter(ENCRYPT_KMS_TYPE);
      String kmsKeyId = ctx.getParameter(ENCRYPT_KMS_KEY_ID);

      Kek kek = retrieveKekFromRegistry(kekId);
      if (kek == null) {
        if (isRead) {
          throw new RuleException("No kek found for " + kekName + " during consume");
        }
        if (kmsType == null || kmsType.isEmpty()) {
          throw new RuleException("No kms type found for " + kekName + " during produce");
        }
        if (kmsKeyId == null || kmsKeyId.isEmpty()) {
          throw new RuleException("No kms key id found for " + kekName + " during produce");
        }
        kek = storeKekToRegistry(kekId, kmsType, kmsKeyId, false);
        if (kek == null) {
          // Handle conflicts (409)
          kek = retrieveKekFromRegistry(kekId);
        }
        if (kek == null) {
          throw new RuleException("No kek found for " + kekName + " during produce");
        }
      }
      if (kmsType != null && !kmsType.isEmpty() && !kmsType.equals(kek.getKmsType())) {
        throw new RuleException("Found " + kekName + " with kms type '"
            + kek.getKmsType() + "' which differs from rule kms type '" + kmsType + "'");
      }
      if (kmsKeyId != null && !kmsKeyId.isEmpty() && !kmsKeyId.equals(kek.getKmsKeyId())) {
        throw new RuleException("Found " + kekName + " with kms key id '"
            + kek.getKmsKeyId() + "' which differs from rule kms key id '" + kmsKeyId + "'");
      }
      return kek;
    }

    private int getDekExpiryDays(RuleContext ctx) throws RuleException {
      String expiryStr = ctx.getParameter(ENCRYPT_DEK_EXPIRY_DAYS);
      if (expiryStr == null || expiryStr.isEmpty()) {
        return 0;
      }
      int dekExpiryDays;
      try {
        dekExpiryDays = Integer.parseInt(expiryStr);
      } catch (NumberFormatException e) {
        throw new RuleException("Invalid value for " + ENCRYPT_DEK_EXPIRY_DAYS + ": " + expiryStr);
      }
      if (dekExpiryDays < 0) {
        throw new RuleException("Invalid value for " + ENCRYPT_DEK_EXPIRY_DAYS + ": " + expiryStr);
      }
      return dekExpiryDays;
    }

    private Kek retrieveKekFromRegistry(KekId key) throws RuleException {
      try {
        return client.getKek(key.getName(), key.isLookupDeleted());
      } catch (RestClientException e) {
        if (e.getStatus() == 404) {
          return null;
        }
        throw new RuleClientException("Could not get kek " + key.getName(), e);
      } catch (IOException e) {
        throw new RuleClientException("Could not get kek " + key.getName(), e);
      }
    }

    private Kek storeKekToRegistry(KekId key, String kmsType, String kmsKeyId, boolean shared)
        throws RuleException {
      try {
        Kek kek = client.createKek(
            key.getName(), kmsType, kmsKeyId,
            null, null, shared);
        log.info("Registered kek " + key.getName());
        return kek;
      } catch (RestClientException e) {
        if (e.getStatus() == 409) {
          return null;
        }
        throw new RuleClientException("Could not register kek " + key.getName(), e);
      } catch (IOException e) {
        throw new RuleClientException("Could not register kek " + key.getName(), e);
      }
    }

    public Dek getOrCreateDek(RuleContext ctx, Integer version)
        throws RuleException, GeneralSecurityException {
      boolean isRead = ctx.ruleMode() == RuleMode.READ;
      DekId dekId = new DekId(kekName, ctx.subject(), version, cryptor.getDekFormat(), isRead);

      Aead aead = null;
      Dek dek = retrieveDekFromRegistry(dekId);
      boolean isExpired = isExpired(ctx, dek);
      if (isExpired) {
        log.info("Dek with ts " + dek.getTimestamp()
            + " expired after " + dekExpiryDays + " day(s)");
      }
      if (dek == null || isExpired) {
        if (isRead) {
          throw new RuleException("No dek found for " + kekName + " during consume");
        }
        byte[] encryptedDek = null;
        if (!kek.isShared()) {
          aead = new AeadWrapper(configs, kek);
          // Generate new dek
          byte[] rawDek = generateKey(dekId.getDekFormat());
          encryptedDek = aead.encrypt(rawDek, EMPTY_AAD);
        }
        Integer newVersion = isExpired ? dek.getVersion() + 1 : null;
        try {
          dek = createDek(dekId, newVersion, encryptedDek);
        } catch (RuleException e) {
          if (dek == null) {
            throw e;
          }
          log.warn("Failed to create dek for " + kekName + ", subject " + ctx.subject()
              + ", version " + newVersion + ", using existing dek");
        }
      }
      if (dek.getKeyMaterialBytes() == null) {
        if (aead == null) {
          aead = new AeadWrapper(configs, kek);
        }
        byte[] rawDek = aead.decrypt(dek.getEncryptedKeyMaterialBytes(), EMPTY_AAD);
        dek.setKeyMaterial(rawDek);
      }
      return dek;
    }

    private Dek createDek(DekId dekId, Integer newVersion, byte[] encryptedDek)
        throws RuleException {
      DekId newDekId = new DekId(dekId.getKekName(), dekId.getSubject(), newVersion,
          dekId.getDekFormat(), dekId.isLookupDeleted());
      // encryptedDek may be passed as null if kek is shared
      Dek dek = storeDekToRegistry(newDekId, encryptedDek);
      if (dek == null) {
        // Handle conflicts (409)
        // Use the original version, which should be null or LATEST_VERSION
        dek = retrieveDekFromRegistry(dekId);
      }
      if (dek == null) {
        throw new RuleException("No dek found for " + dekId.getKekName() + " during produce");
      }
      return dek;
    }

    private boolean isExpired(RuleContext ctx, Dek dek) {
      return ctx.ruleMode() != RuleMode.READ
          && dekExpiryDays > 0
          && dek != null
          && (clock.millis() - dek.getTimestamp()) / MILLIS_IN_DAY >= dekExpiryDays;
    }

    private Dek retrieveDekFromRegistry(DekId key)
        throws RuleException {
      try {
        Dek dek;
        if (key.getVersion() != null) {
          dek = client.getDekVersion(
              key.getKekName(), key.getSubject(), key.getVersion(), key.getDekFormat(),
              key.isLookupDeleted());
        } else {
          dek = client.getDek(
              key.getKekName(), key.getSubject(), key.getDekFormat(), key.isLookupDeleted());
        }
        return dek != null && dek.getEncryptedKeyMaterial() != null ? dek : null;
      } catch (RestClientException e) {
        if (e.getStatus() == 404) {
          return null;
        }
        throw new RuleClientException("Could not get dek for kek " + key.getKekName()
            + ", subject " + key.getSubject(), e);
      } catch (IOException e) {
        throw new RuleClientException("Could not get dek for kek " + key.getKekName()
            + ", subject " + key.getSubject(), e);
      }
    }

    private Dek storeDekToRegistry(DekId key, byte[] encryptedDek)
        throws RuleException {
      try {
        String encryptedDekStr = encryptedDek != null
            ? (String) toObject(Type.STRING, Base64.getEncoder().encode(encryptedDek))
            : null;
        Dek dek;
        if (key.getVersion() != null) {
          dek = client.createDek(
              key.getKekName(), key.getSubject(), key.getVersion(),
              key.getDekFormat(), encryptedDekStr);
        } else {
          dek = client.createDek(
              key.getKekName(), key.getSubject(), key.getDekFormat(), encryptedDekStr);
        }
        log.info("Registered dek for kek " + key.getKekName() + ", subject " + key.getSubject());
        return dek;
      } catch (RestClientException e) {
        if (e.getStatus() == 409) {
          return null;
        }
        throw new RuleClientException("Could not register dek for kek " + key.getKekName()
            + ", subject " + key.getSubject(), e);
      } catch (IOException e) {
        throw new RuleClientException("Could not register dek for kek " + key.getKekName()
            + ", subject " + key.getSubject(), e);
      }
    }

    public Object transform(RuleContext ctx, Type type, Object value)
        throws RuleException {
      try {
        if (value == null) {
          return null;
        }
        Dek dek;
        byte[] plaintext;
        byte[] ciphertext;
        switch (ctx.ruleMode()) {
          case WRITE:
            plaintext = toBytes(type, value);
            if (plaintext == null) {
              throw new RuleException(
                  "Type '" + type + "' not supported for encryption");
            }
            dek = getOrCreateDek(ctx, isDekRotated() ? LATEST_VERSION : null);
            ciphertext = cryptor.encrypt(dek.getKeyMaterialBytes(), plaintext, EMPTY_AAD);
            if (isDekRotated()) {
              ciphertext = prefixVersion(dek.getVersion(), ciphertext);
            }
            if (type == Type.STRING) {
              ciphertext = Base64.getEncoder().encode(ciphertext);
            }
            return toObject(type, ciphertext);
          case READ:
            ciphertext = toBytes(type, value);
            if (ciphertext == null) {
              return value;
            }
            if (type == Type.STRING) {
              ciphertext = Base64.getDecoder().decode(ciphertext);
            }
            Integer version = null;
            if (isDekRotated()) {
              Map.Entry<Integer, byte[]> kv = extractVersion(ciphertext);
              version = kv.getKey();
              ciphertext = kv.getValue();
            }
            dek = getOrCreateDek(ctx, version);
            plaintext = cryptor.decrypt(dek.getKeyMaterialBytes(), ciphertext, EMPTY_AAD);
            return toObject(type, plaintext);
          default:
            throw new IllegalArgumentException("Unsupported rule mode " + ctx.ruleMode());
        }
      } catch (Exception e) {
        throw new RuleException(e);
      }
    }

    private byte[] prefixVersion(int version, byte[] ciphertext) {
      byte[] combined = new byte[ciphertext.length + 1 + VERSION_SIZE];
      ByteBuffer buffer = ByteBuffer.wrap(combined);
      buffer.put(MAGIC_BYTE);
      buffer.putInt(version);
      buffer.put(ciphertext);
      return combined;
    }

    private Map.Entry<Integer, byte[]> extractVersion(byte[] ciphertext) throws RuleException {
      ByteBuffer buffer = ByteBuffer.wrap(ciphertext);
      if (buffer.get() != MAGIC_BYTE) {
        throw new RuleException("Unknown magic byte!");
      }
      int version = buffer.getInt();
      int remainingSize = ciphertext.length - 1 - VERSION_SIZE;
      byte[] remaining = new byte[remainingSize];
      buffer.get(remaining, 0, remainingSize);
      return new SimpleEntry<>(version, remaining);
    }
  }

  private static KmsClient getKmsClient(Map<String, ?> configs, String kekUrl)
      throws GeneralSecurityException {
    try {
      return KmsDriverManager.getDriver(kekUrl).getKmsClient(kekUrl);
    } catch (GeneralSecurityException e) {
      return KmsDriverManager.getDriver(kekUrl).registerKmsClient(configs, Optional.of(kekUrl));
    }
  }

  static class AeadWrapper implements Aead {
    private final Map<String, ?> configs;
    private final Kek kek;
    private final List<String> kmsKeyIds;

    public AeadWrapper(Map<String, ?> configs, Kek kek) {
      this.configs = configs;
      this.kek = kek;
      this.kmsKeyIds = getKmsKeyIds();
    }

    @Override
    public byte[] encrypt(byte[] plaintext, byte[] associatedData)
        throws GeneralSecurityException {
      for (int i = 0; i < kmsKeyIds.size(); i++) {
        try {
          Aead aead = getAead(configs, kek.getKmsType(), kmsKeyIds.get(i));
          return aead.encrypt(plaintext, associatedData);
        } catch (Exception e) {
          log.warn("Failed to encrypt with kek {} and kms key id {}: {}",
              kek.getName(), kmsKeyIds.get(i), e.getMessage());
          if (i == kmsKeyIds.size() - 1) {
            throw e instanceof GeneralSecurityException
                ? (GeneralSecurityException) e
                : new GeneralSecurityException("Failed to encrypt with all KEKs", e);
          }
        }
      }
      return null;
    }

    @Override
    public byte[] decrypt(byte[] ciphertext, byte[] associatedData)
        throws GeneralSecurityException {
      for (int i = 0; i < kmsKeyIds.size(); i++) {
        try {
          Aead aead = getAead(configs, kek.getKmsType(), kmsKeyIds.get(i));
          return aead.decrypt(ciphertext, associatedData);
        } catch (Exception e) {
          log.warn("Failed to decrypt with kek {} and kms key id {}: {}",
              kek.getName(), kmsKeyIds.get(i), e.getMessage());
          if (i == kmsKeyIds.size() - 1) {
            throw e instanceof GeneralSecurityException
                ? (GeneralSecurityException) e
                : new GeneralSecurityException("Failed to decrypt with all KEKs", e);
          }
        }
      }
      return null;
    }

    private List<String> getKmsKeyIds() {
      List<String> kmsKeyIds = new ArrayList<>();
      kmsKeyIds.add(kek.getKmsKeyId());
      if (kek.getKmsProps() != null) {
        String alternateKmsKeyIds = kek.getKmsProps().get(ENCRYPT_ALTERNATE_KMS_KEY_IDS);
        if (alternateKmsKeyIds != null && !alternateKmsKeyIds.isEmpty()) {
          String[] ids = alternateKmsKeyIds.split("\\s*,\\s*");
          for (String id : ids) {
            if (!id.isEmpty()) {
              kmsKeyIds.add(id);
            }
          }
        }
      }
      return kmsKeyIds;
    }

    private static Aead getAead(Map<String, ?> configs, String kmsType, String kmsKeyId)
        throws GeneralSecurityException {
      String kekUrl = kmsType + KMS_TYPE_SUFFIX + kmsKeyId;
      KmsClient kmsClient = getKmsClient(configs, kekUrl);
      if (kmsClient == null) {
        throw new GeneralSecurityException("No kms client found for " + kekUrl);
      }
      return kmsClient.getAead(kekUrl);
    }
  }
}

