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
import com.google.crypto.tink.proto.AesGcmKey;
import com.google.crypto.tink.proto.AesSivKey;
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
import java.time.Clock;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public class FieldEncryptionExecutor extends FieldRuleExecutor {

  private static final Logger log = LoggerFactory.getLogger(FieldEncryptionExecutor.class);

  public static final String TYPE = "ENCRYPT";

  public static final String ENCRYPT_KEK_NAME = "encrypt.kek.name";
  public static final String ENCRYPT_KMS_KEY_ID = "encrypt.kms.key.id";
  public static final String ENCRYPT_KMS_TYPE = "encrypt.kms.type";
  public static final String ENCRYPT_DEK_ALGORITHM = "encrypt.dek.algorithm";
  public static final String ENCRYPT_DEK_EXPIRY_DAYS = "encrypt.dek.expiry.days";

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

  public FieldEncryptionExecutor() {
  }

  @Override
  public boolean addOriginalConfigs() {
    return true;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
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
  public FieldEncryptionExecutorTransform newTransform(RuleContext ctx) throws RuleException {
    FieldEncryptionExecutorTransform transform = new FieldEncryptionExecutorTransform();
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

  public class FieldEncryptionExecutorTransform implements FieldTransform {
    private Cryptor cryptor;
    private String kekName;
    private KekInfo kek;
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

    protected KekInfo getOrCreateKek(RuleContext ctx) throws RuleException {
      boolean isRead = ctx.ruleMode() == RuleMode.READ;
      KekId kekId = new KekId(kekName, isRead);

      String kmsType = ctx.getParameter(ENCRYPT_KMS_TYPE);
      String kmsKeyId = ctx.getParameter(ENCRYPT_KMS_KEY_ID);

      KekInfo kek = retrieveKekFromRegistry(ctx, kekId);
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
        throw new RuleException("Found " + kekName + " with kms type '"
            + kek.getKmsType() + "' which differs from rule kms type '" + kmsType + "'");
      }
      if (kmsKeyId != null && !kmsKeyId.equals(kek.getKmsKeyId())) {
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
        throw new RuleException("Could not get kek " + key.getName(), e);
      } catch (IOException e) {
        throw new RuleException("Could not get kek " + key.getName(), e);
      }
    }

    private KekInfo storeKekToRegistry(RuleContext ctx, KekId key, KekInfo kekInfo)
        throws RuleException {
      try {
        Kek kek = client.createKek(
            key.getName(), kekInfo.getKmsType(), kekInfo.getKmsKeyId(),
            null, null, kekInfo.isShared());
        log.info("Registered kek " + key.getName());
        return new KekInfo(kek.getKmsType(), kek.getKmsKeyId(), kek.isShared());
      } catch (RestClientException e) {
        if (e.getStatus() == 409) {
          return null;
        }
        throw new RuleException("Could not register kek " + key.getName(), e);
      } catch (IOException e) {
        throw new RuleException("Could not register kek " + key.getName(), e);
      }
    }

    public DekInfo getOrCreateDek(RuleContext ctx, Integer version)
        throws RuleException, GeneralSecurityException {
      boolean isRead = ctx.ruleMode() == RuleMode.READ;
      DekId dekId = new DekId(kekName, ctx.subject(), version, cryptor.getDekFormat(), isRead);

      Aead aead = null;
      DekInfo dek = retrieveDekFromRegistry(dekId);
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
          aead = getAead(configs, kek);
          // Generate new dek
          byte[] rawDek = generateKey(dekId.getDekFormat());
          encryptedDek = aead.encrypt(rawDek, EMPTY_AAD);
        }
        Integer newVersion = isExpired ? dek.getVersion() + 1 : null;
        DekId newDekId = new DekId(kekName, ctx.subject(), newVersion,
            cryptor.getDekFormat(), isRead);
        // encryptedDek may be passed as null if kek is shared
        dek = storeDekToRegistry(newDekId, encryptedDek);
        if (dek == null) {
          // Handle conflicts (409)
          // Use the original version, which should be null or LATEST_VERSION
          dek = retrieveDekFromRegistry(dekId);
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

    private boolean isExpired(RuleContext ctx, DekInfo dek) {
      return ctx.ruleMode() != RuleMode.READ
          && dekExpiryDays > 0
          && dek != null
          && (clock.millis() - dek.getTimestamp()) / MILLIS_IN_DAY >= dekExpiryDays;
    }

    private DekInfo retrieveDekFromRegistry(DekId key)
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
        if (dek == null) {
          return null;
        }
        byte[] rawDek = dek.getKeyMaterial() != null
            ? Base64.getDecoder().decode(toBytes(Type.STRING, dek.getKeyMaterial()))
            : null;
        byte[] encryptedDek = dek.getEncryptedKeyMaterial() != null
            ? Base64.getDecoder().decode(toBytes(Type.STRING, dek.getEncryptedKeyMaterial()))
            : null;
        return encryptedDek != null
            ? new DekInfo(dek.getVersion(), rawDek, encryptedDek, dek.getTimestamp())
            : null;
      } catch (RestClientException e) {
        if (e.getStatus() == 404) {
          return null;
        }
        throw new RuleException("Could not get dek for kek " + key.getKekName()
            + ", subject " + key.getSubject(), e);
      } catch (IOException e) {
        throw new RuleException("Could not get dek for kek " + key.getKekName()
            + ", subject " + key.getSubject(), e);
      }
    }

    private DekInfo storeDekToRegistry(DekId key, byte[] encryptedDek)
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
        byte[] rawDek = dek.getKeyMaterial() != null
            ? Base64.getDecoder().decode(toBytes(Type.STRING, dek.getKeyMaterial()))
            : null;
        encryptedDek = dek.getEncryptedKeyMaterial() != null
            ? Base64.getDecoder().decode(toBytes(Type.STRING, dek.getEncryptedKeyMaterial()))
            : null;
        log.info("Registered dek for kek " + key.getKekName() + ", subject " + key.getSubject());
        return new DekInfo(dek.getVersion(), rawDek, encryptedDek, dek.getTimestamp());
      } catch (RestClientException e) {
        if (e.getStatus() == 409) {
          return null;
        }
        throw new RuleException("Could not register dek for kek " + key.getKekName()
            + ", subject " + key.getSubject(), e);
      } catch (IOException e) {
        throw new RuleException("Could not register dek for kek " + key.getKekName()
            + ", subject " + key.getSubject(), e);
      }
    }

    public Object transform(RuleContext ctx, FieldContext fieldCtx, Object fieldValue)
        throws RuleException {
      try {
        if (fieldValue == null) {
          return null;
        }
        DekInfo dek;
        byte[] plaintext;
        byte[] ciphertext;
        switch (ctx.ruleMode()) {
          case WRITE:
            plaintext = toBytes(fieldCtx.getType(), fieldValue);
            if (plaintext == null) {
              throw new RuleException(
                  "Type '" + fieldCtx.getType() + "' not supported for encryption");
            }
            dek = getOrCreateDek(ctx, isDekRotated() ? LATEST_VERSION : null);
            ciphertext = cryptor.encrypt(dek.getRawDek(), plaintext, EMPTY_AAD);
            if (isDekRotated()) {
              ciphertext = prefixVersion(dek.getVersion(), ciphertext);
            }
            if (fieldCtx.getType() == Type.STRING) {
              ciphertext = Base64.getEncoder().encode(ciphertext);
            }
            return toObject(fieldCtx.getType(), ciphertext);
          case READ:
            ciphertext = toBytes(fieldCtx.getType(), fieldValue);
            if (ciphertext == null) {
              return fieldValue;
            }
            if (fieldCtx.getType() == Type.STRING) {
              ciphertext = Base64.getDecoder().decode(ciphertext);
            }
            Integer version = null;
            if (isDekRotated()) {
              Map.Entry<Integer, byte[]> kv = extractVersion(ciphertext);
              version = kv.getKey();
              ciphertext = kv.getValue();
            }
            dek = getOrCreateDek(ctx, version);
            plaintext = cryptor.decrypt(dek.getRawDek(), ciphertext, EMPTY_AAD);
            return toObject(fieldCtx.getType(), plaintext);
          default:
            throw new IllegalArgumentException("Unsupported rule mode " + ctx.ruleMode());
        }
      } catch (Exception e) {
        throw new RuleException(e);
      }
    }

    private byte[] prefixVersion(int version, byte[] ciphertext) throws RuleException {
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

    private final Integer version;
    private byte[] rawDek;
    private final byte[] encryptedDek;
    private final Long ts;

    public DekInfo(Integer version, byte[] rawDek, byte[] encryptedDek, Long ts) {
      this.version = version;
      this.rawDek = rawDek;
      this.encryptedDek = encryptedDek;
      this.ts = ts;
    }

    public Integer getVersion() {
      return version;
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

    public Long getTimestamp() {
      return ts;
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
      return Objects.equals(version, dek.version)
          && Arrays.equals(rawDek, dek.rawDek)
          && Arrays.equals(encryptedDek, dek.encryptedDek);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(version);
      result = 31 * result + Arrays.hashCode(rawDek);
      result = 31 * result + Arrays.hashCode(encryptedDek);
      return result;
    }
  }
}

