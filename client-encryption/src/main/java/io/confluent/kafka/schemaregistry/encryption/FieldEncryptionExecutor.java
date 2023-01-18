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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.KmsClients;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.daead.DeterministicAeadConfig;
import io.confluent.kafka.schemaregistry.rules.FieldRuleExecutor;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;

/**
 * In envelope encryption, a user generates a data encryption key (DEK) locally, encrypts data with
 * the DEK, sends the DEK to a KMS to be encrypted (with a key managed by KMS), and then stores the
 * encrypted DEK with the encrypted data. At a later point, a user can retrieve the encrypted data
 * and the encrypted DEK, use the KMS to decrypt the DEK, and use the decrypted DEK to decrypt the
 * data.
 */
public class FieldEncryptionExecutor implements FieldRuleExecutor {

  public static final String TYPE = "ENCRYPT";

  public static final byte[] EMPTY_AAD = new byte[0];
  public static final String HEADER_NAME_PREFIX = "encrypt";
  public static final String CACHE_EXPIRY_SECS = "cache.expiry.secs";
  public static final String CACHE_SIZE = "cache.size";
  public static final String KEY_DETERMINISTIC = "key.deterministic";
  public static final String VALUE_DETERMINISTIC = "value.deterministic";
  public static final String TEST_CLIENT = "test.client";

  private static final byte VERSION = (byte) 0;
  private static final int LENGTH_VERSION = 1;
  private static final int LENGTH_ENCRYPTED_DEK = 4;
  private static final int LENGTH_KEK_ID = 4;
  private static final int LENGTH_DEK_FORMAT = 4;

  private String kekId;
  private Map<String, Cryptor> cryptors;
  private int cacheExpirySecs = 300;
  private int cacheSize = 1000;
  private boolean keyDeterministic = false;
  private boolean valueDeterministic = false;
  private Object testClient;  // for testing
  private LoadingCache<EncryptKey, Dek> dekEncryptCache;
  private LoadingCache<DecryptKey, Dek> dekDecryptCache;

  static {
    try {
      AeadConfig.register();
      DeterministicAeadConfig.register();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public FieldEncryptionExecutor() {
  }

  public void configure(Map<String, ?> configs) {
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
    Object keyDeterministicConfig = configs.get(KEY_DETERMINISTIC);
    if (keyDeterministicConfig != null) {
      this.keyDeterministic = Boolean.parseBoolean(keyDeterministicConfig.toString());
    }
    Object valueDeterministicConfig = configs.get(VALUE_DETERMINISTIC);
    if (valueDeterministicConfig != null) {
      this.valueDeterministic = Boolean.parseBoolean(valueDeterministicConfig.toString());
    }
    this.testClient = configs.get(TEST_CLIENT);
    this.dekEncryptCache = CacheBuilder.newBuilder()
        .expireAfterWrite(Duration.ofSeconds(cacheExpirySecs))
        .maximumSize(cacheSize)
        .build(new CacheLoader<EncryptKey, Dek>() {
          @Override
          public Dek load(EncryptKey encryptKey) throws Exception {
            String kekId = encryptKey.getKekId();
            String dekFormat = encryptKey.getDekFormat();
            // Generate new dek
            byte[] rawDek = getCryptor(dekFormat).generateKey();
            // Encrypt dek with kek
            KmsClient kmsClient = KmsClients.get(kekId);
            Aead aead = kmsClient.getAead(kekId);
            byte[] encryptedDek = aead.encrypt(rawDek, EMPTY_AAD);
            return new Dek(rawDek, encryptedDek);
          }
        });
    this.dekDecryptCache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .build(new CacheLoader<DecryptKey, Dek>() {
          @Override
          public Dek load(DecryptKey decryptKey) throws Exception {
            String kekId = decryptKey.getKekId();
            byte[] encryptedDek = decryptKey.getEncryptedDek();
            KmsClient kmsClient = KmsClients.get(kekId);
            Aead aead = kmsClient.getAead(kekId);
            byte[] rawDek = aead.decrypt(encryptedDek, EMPTY_AAD);
            return new Dek(rawDek, encryptedDek);
          }
        });
    this.cryptors = new HashMap<>();
  }

  private static String getKeyFormat(boolean isDeterministic) {
    return isDeterministic ? Cryptor.DETERMINISTIC_KEY_FORMAT : Cryptor.RANDOM_KEY_FORMAT;
  }

  public String getKekId() {
    return kekId;
  }

  public void setKekId(String kekId) {
    this.kekId = kekId;
  }

  public Object getTestClient() {
    return testClient;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public FieldTransform newTransform(RuleContext ruleContext) throws RuleException {
    FieldTransform transform = new FieldEncryptionExecutorTransform();
    transform.init(ruleContext);
    return transform;
  }

  private Cryptor getCryptor(boolean isKey) {
    return getCryptor(isKey ? getKeyFormat(keyDeterministic) : getKeyFormat(valueDeterministic));
  }

  private Cryptor getCryptor(String dekFormat) {
    return cryptors.computeIfAbsent(dekFormat, k -> {
      try {
        return new Cryptor(dekFormat);
      } catch (GeneralSecurityException e) {
        throw new IllegalArgumentException("Invalid format " + dekFormat, e);
      }
    });
  }

  // Visible for testing
  public void setCryptor(String dekFormat, Cryptor cryptor) {
    cryptors.put(dekFormat, cryptor);
  }

  private static byte[] toBytes(FieldContext fieldCtx, Object obj) {
    switch (fieldCtx.getType()) {
      case BYTES:
        return (byte[]) obj;
      case STRING:
        return obj.toString().getBytes(StandardCharsets.UTF_8);
      default:
        return null;
    }
  }

  private static Object toObject(FieldContext fieldCtx, byte[] bytes) {
    switch (fieldCtx.getType()) {
      case BYTES:
        return bytes;
      case STRING:
        return new String(bytes, StandardCharsets.UTF_8);
      default:
        return null;
    }
  }

  private static String getHeaderName(RuleContext ctx) {
    return HEADER_NAME_PREFIX + (ctx.isKey() ? "-key" : "-value");
  }

  class FieldEncryptionExecutorTransform implements FieldTransform {
    private RuleContext ctx;
    private Cryptor cryptor;
    private Dek dek;
    private int count = 0;

    public void init(RuleContext ctx) throws RuleException {
      try {
        this.ctx = ctx;
        switch (ctx.ruleMode()) {
          case WRITE:
            cryptor = getCryptor(ctx.isKey());
            dek = getDekForEncrypt(cryptor.getDekFormat());
            break;
          case READ:
            String headerName = getHeaderName(ctx);
            Header header = ctx.headers().lastHeader(headerName);
            if (header == null) {
              // Not encrypted
              return;
            }
            byte[] metadata = header.value();
            setCryptorAndDekFromHeader(metadata);
            break;
          default:
            throw new IllegalArgumentException("Unsupported rule mode " + ctx.ruleMode());
        }
      } catch (GeneralSecurityException e) {
        throw new RuleException(e);
      }
    }

    protected Dek getDekForEncrypt(String dekFormat) {
      EncryptKey key = new EncryptKey(kekId, dekFormat);
      try {
        return dekEncryptCache.get(key);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    protected Dek getDekForDecrypt(String kekId, byte[] encryptedDek) {
      DecryptKey key = new DecryptKey(kekId, encryptedDek);
      try {
        return dekDecryptCache.get(key);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    private void setCryptorAndDekFromHeader(byte[] metadata)
        throws GeneralSecurityException {
      int remainingSize = metadata.length;
      ByteBuffer buffer = ByteBuffer.wrap(metadata);
      buffer.get();  // version
      remainingSize--;
      int kekIdSize = buffer.getInt();
      remainingSize -= LENGTH_KEK_ID;
      if (kekIdSize <= 0 || kekIdSize > remainingSize) {
        throw new GeneralSecurityException("invalid ciphertext");
      }
      byte[] kekId = new byte[kekIdSize];
      buffer.get(kekId, 0, kekIdSize);
      remainingSize -= kekIdSize;
      int dekFormatSize = buffer.getInt();
      remainingSize -= LENGTH_DEK_FORMAT;
      if (dekFormatSize <= 0 || dekFormatSize > remainingSize) {
        throw new GeneralSecurityException("invalid ciphertext");
      }
      byte[] dekFormat = new byte[dekFormatSize];
      buffer.get(dekFormat, 0, dekFormatSize);
      remainingSize -= dekFormatSize;
      int encryptedDekSize = buffer.getInt();
      remainingSize -= LENGTH_ENCRYPTED_DEK;
      if (encryptedDekSize <= 0 || encryptedDekSize > remainingSize) {
        throw new GeneralSecurityException("invalid ciphertext");
      }
      byte[] encryptedDek = new byte[encryptedDekSize];
      buffer.get(encryptedDek, 0, encryptedDekSize);
      remainingSize -= encryptedDekSize;
      if (remainingSize != 0) {
        throw new GeneralSecurityException("invalid ciphertext");
      }

      this.cryptor = getCryptor(new String(dekFormat, StandardCharsets.UTF_8));
      this.dek = getDekForDecrypt(new String(kekId, StandardCharsets.UTF_8), encryptedDek);
    }

    public Object transform(RuleContext ctx, FieldContext fieldCtx, Object fieldValue)
        throws RuleException {
      try {
        byte[] plaintext;
        byte[] ciphertext;
        String headerName = getHeaderName(ctx);
        switch (ctx.ruleMode()) {
          case WRITE:
            plaintext = toBytes(fieldCtx, fieldValue);
            if (plaintext == null) {
              return fieldValue;
            }
            if (ctx.headers().lastHeader(headerName) != null) {
              // Already encrypted
              return fieldValue;
            }
            ciphertext = cryptor.encrypt(dek.getRawDek(), plaintext);
            count++;
            return Base64.getEncoder().encodeToString(ciphertext);
          case READ:
            Header header = ctx.headers().lastHeader(headerName);
            if (header == null) {
              // Not encrypted
              return fieldValue;
            }
            ciphertext = Base64.getDecoder().decode(fieldValue.toString());
            plaintext = cryptor.decrypt(dek.getRawDek(), ciphertext);
            count++;
            Object result = toObject(fieldCtx, plaintext);
            return result != null ? result : fieldValue;
          default:
            throw new IllegalArgumentException("Unsupported rule mode " + ctx.ruleMode());
        }
      } catch (GeneralSecurityException e) {
        throw new RuleException(e);
      }
    }

    public void close() {
      byte[] metadata;
      String headerName = getHeaderName(ctx);
      switch (ctx.ruleMode()) {
        case WRITE:
          if (count > 0) {
            metadata = buildMetadata(cryptor.getDekFormat(), dek.getEncryptedDek());
            // Add header
            ctx.headers().add(headerName, metadata);
          }
          break;
        case READ:
          // Remove header
          ctx.headers().remove(headerName);
          break;
        default:
          throw new IllegalArgumentException("Unsupported rule mode " + ctx.ruleMode());
      }
    }

    private byte[] buildMetadata(String dekFormat, byte[] encryptedDek) {
      byte[] kekBytes = getKekId().getBytes(StandardCharsets.UTF_8);
      byte[] dekBytes = dekFormat.getBytes(StandardCharsets.UTF_8);
      return ByteBuffer.allocate(LENGTH_VERSION
              + LENGTH_KEK_ID + kekBytes.length
              + LENGTH_DEK_FORMAT + dekBytes.length
              + LENGTH_ENCRYPTED_DEK + encryptedDek.length)
          .put(VERSION)
          .putInt(kekBytes.length)
          .put(kekBytes)
          .putInt(dekBytes.length)
          .put(dekBytes)
          .putInt(encryptedDek.length)
          .put(encryptedDek)
          .array();
    }
  }

  static class EncryptKey {

    private final String kekId;
    private final String dekFormat;

    public EncryptKey(String kekId, String dekFormat) {
      this.kekId = kekId;
      this.dekFormat = dekFormat;
    }

    public String getKekId() {
      return kekId;
    }

    public String getDekFormat() {
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
      EncryptKey that = (EncryptKey) o;
      return Objects.equals(kekId, that.kekId) && Objects.equals(dekFormat,
          that.dekFormat);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kekId, dekFormat);
    }
  }

  static class DecryptKey {

    private final String kekId;
    private final byte[] encryptedDek;

    public DecryptKey(String kekId, byte[] encryptedDek) {
      this.kekId = kekId;
      this.encryptedDek = encryptedDek;
    }

    public String getKekId() {
      return kekId;
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
      DecryptKey that = (DecryptKey) o;
      return Objects.equals(kekId, that.kekId) && Arrays.equals(encryptedDek,
          that.encryptedDek);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(kekId);
      result = 31 * result + Arrays.hashCode(encryptedDek);
      return result;
    }
  }

  static class Dek {

    private final byte[] rawDek;
    private final byte[] encryptedDek;

    public Dek(byte[] rawDek, byte[] encryptedDek) {
      this.rawDek = rawDek;
      this.encryptedDek = encryptedDek;
    }

    public byte[] getRawDek() {
      return rawDek;
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
      Dek dek1 = (Dek) o;
      return Arrays.equals(rawDek, dek1.rawDek)
          && Arrays.equals(encryptedDek, dek1.encryptedDek);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(rawDek);
      result = 31 * result + Arrays.hashCode(encryptedDek);
      return result;
    }
  }
}

