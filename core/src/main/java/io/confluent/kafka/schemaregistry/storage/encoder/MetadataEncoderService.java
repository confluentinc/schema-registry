/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage.encoder;

import com.google.common.annotations.VisibleForTesting;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeyTemplate;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.KeysetManager;
import com.google.crypto.tink.Registry;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.proto.AesGcmKey;
import com.google.crypto.tink.subtle.Hkdf;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.MD5;
import io.confluent.kafka.schemaregistry.storage.Metadata;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Base64;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MetadataEncoderService implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MetadataEncoderService.class);

  private static final String AES_GCM_KEY = "type.googleapis.com/google.crypto.tink.AesGcmKey";
  private static final byte[] EMPTY_AAD = new byte[0];
  private static final String KEY_TEMPLATE_NAME = "AES128_GCM";

  private final SchemaRegistry schemaRegistry;
  protected final String encoderSecret;
  protected KeyTemplate keyTemplate;

  protected final AtomicBoolean initialized = new AtomicBoolean();
  protected final CountDownLatch initLatch = new CountDownLatch(1);

  static {
    try {
      AeadConfig.register();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public MetadataEncoderService(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
    this.encoderSecret = encoderSecret(schemaRegistry.config());
    if (encoderSecret == null) {
      log.warn("No value specified for {}, sensitive metadata will not be encoded",
          SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG);
      return;
    }
    try {
      this.keyTemplate = KeyTemplates.get(KEY_TEMPLATE_NAME);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not instantiate MetadataEncoderService", e);
    }
  }

  public SchemaRegistry getSchemaRegistry() {
    return schemaRegistry;
  }

  protected static Aead getPrimitive(String secret) throws GeneralSecurityException {
    if (secret == null) {
      throw new IllegalArgumentException("Secret is null");
    }
    byte[] keyBytes = Hkdf.computeHkdf(
        "HmacSha256", secret.getBytes(StandardCharsets.UTF_8), null, null, 16);
    AesGcmKey key = AesGcmKey.newBuilder()
        .setVersion(0)
        .setKeyValue(ByteString.copyFrom(keyBytes))
        .build();
    return Registry.getPrimitive(AES_GCM_KEY, key.toByteString(), Aead.class);
  }

  protected static String encoderSecret(SchemaRegistryConfig config) {
    String secret;
    Password password = config.getPassword(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG);
    if (password != null) {
      secret = password.value();
    } else {
      secret = System.getenv("METADATA_ENCODER_SECRET");
    }
    return secret;
  }

  protected static String encoderOldSecret(SchemaRegistryConfig config) {
    String secret;
    Password password = config.getPassword(SchemaRegistryConfig.METADATA_ENCODER_OLD_SECRET_CONFIG);
    if (password != null) {
      secret = password.value();
    } else {
      secret = System.getenv("METADATA_ENCODER_OLD_SECRET");
    }
    return secret;
  }

  /**
   * Subclass hook to prepare any resources before secret rotation occurs.
   * Default is no-op.
   */
  protected void doInit() {
    // no-op
  }

  /**
   * Initialize the metadata encoder service by rotating the secrets if needed.
   * Subclasses should override {@link #doInit()} to prepare any resources
   * before the secrets are rotated.
   */
  public final void init() {
    if (encoderSecret != null && !initialized.get()) {
      doInit();
      maybeRotateSecrets();
      boolean isInitialized = initialized.compareAndSet(false, true);
      if (!isInitialized) {
        throw new IllegalStateException("Metadata encoder service was already initialized");
      }
      initLatch.countDown();
    }
  }

  /**
   * Get all tenant keys. Used for secret rotation.
   */
  protected abstract Set<String> getAllTenants();

  /**
   * Get the encoder wrapper for a tenant.
   */
  protected abstract KeysetWrapper getEncoderWrapper(String tenant);

  /**
   * Store an encoder wrapper for a tenant.
   */
  protected abstract void putEncoderWrapper(String tenant, KeysetWrapper wrapper);

  protected void maybeRotateSecrets() {
    String oldSecret = encoderOldSecret(schemaRegistry.config());
    if (oldSecret != null) {
      log.info("Rotating encoder secret");
      for (String key : getAllTenants()) {
        KeysetWrapper wrapper = getEncoderWrapper(key);
        if (wrapper != null && wrapper.isRotationNeeded()) {
          try {
            KeysetHandle handle = wrapper.getKeysetHandle();
            KeysetHandle rotatedHandle = KeysetManager
                .withKeysetHandle(handle)
                .add(keyTemplate)
                .getKeysetHandle();
            int keyId = rotatedHandle.getAt(rotatedHandle.size() - 1).getId();
            rotatedHandle = KeysetManager
                .withKeysetHandle(rotatedHandle)
                .setPrimary(keyId)
                .getKeysetHandle();
            // This will cause the new secret to be used
            putEncoderWrapper(key, new KeysetWrapper(rotatedHandle, false));
          } catch (GeneralSecurityException e) {
            log.error("Could not rotate key for {}", key, e);
          }
        }
      }
      log.info("Done rotating encoder secret");
    }
  }

  public void waitForInit() throws InterruptedException {
    initLatch.await();
  }

  @VisibleForTesting
  public abstract KeysetHandle getEncoder(String tenant);

  public void encodeMetadata(SchemaValue schema) throws SchemaRegistryStoreException {
    if (!initialized.get() || schema == null || isEncoded(schema)) {
      return;
    }
    try {
      transformMetadata(schema, true, (aead, value) -> {
        try {
          byte[] ciphertext = aead.encrypt(value.getBytes(StandardCharsets.UTF_8), EMPTY_AAD);
          return Base64.getEncoder().encodeToString(ciphertext);
        } catch (GeneralSecurityException e) {
          throw new IllegalStateException("Could not encrypt sensitive metadata", e);
        }
      });
    } catch (IllegalStateException e) {
      throw new SchemaRegistryStoreException(
          "Could not encrypt metadata for schema id " + schema.getId(), e);
    }
  }

  private boolean isEncoded(SchemaValue schema) {
    if (schema == null
        || schema.getMetadata() == null
        || schema.getMetadata().getProperties() == null) {
      return false;
    }
    return schema.getMetadata().getProperties().containsKey(SchemaValue.ENCODED_PROPERTY);
  }

  public void decodeMetadata(SchemaValue schema) throws SchemaRegistryStoreException {
    if (!initialized.get() || schema == null || !isEncoded(schema)) {
      return;
    }
    try {
      transformMetadata(schema, false, (aead, value) -> {
        try {
          byte[] plaintext = aead.decrypt(Base64.getDecoder().decode(value), EMPTY_AAD);
          return new String(plaintext, StandardCharsets.UTF_8);
        } catch (GeneralSecurityException e) {
          throw new IllegalStateException("Could not decrypt sensitive metadata", e);
        }
      });
    } catch (IllegalStateException e) {
      throw new SchemaRegistryStoreException(
          "Could not decrypt metadata for schema id " + schema.getId(), e);
    }
  }

  private void transformMetadata(
      SchemaValue schema, boolean isEncode, BiFunction<Aead, String, String> func)
      throws SchemaRegistryStoreException {
    Metadata metadata = schema.getMetadata();
    if (metadata == null
        || metadata.getProperties() == null
        || metadata.getProperties().isEmpty()
        || metadata.getSensitive() == null
        || metadata.getSensitive().isEmpty()) {
      return;
    }

    try {
      String subject = schema.getSubject();
      // We pass in the schema registry tenant, but the QualifiedSubject.create method only cares
      // if it is the default tenant; otherwise it will extract the tenant from the subject.
      QualifiedSubject qualifiedSubject = QualifiedSubject.create(schemaRegistry.tenant(), subject);
      String tenant = qualifiedSubject.getTenant();

      // Only create the encoder if we are encoding during writes and not decoding during reads
      KeysetHandle handle = isEncode ? getOrCreateEncoder(tenant) : getEncoder(tenant);
      if (handle == null) {
        throw new SchemaRegistryStoreException("Could not get encoder for tenant " + tenant);
      }
      Aead aead = handle.getPrimitive(Aead.class);

      SortedMap<String, String> newProperties = metadata.getProperties().entrySet().stream()
          .map(e -> new AbstractMap.SimpleEntry<>(
              e.getKey(),
              metadata.getSensitive().contains(e.getKey())
                  ? func.apply(aead, e.getValue())
                  : e.getValue())
          )
          .collect(Collectors.toMap(
              SimpleEntry::getKey,
              SimpleEntry::getValue,
              (e1, e2) -> e2,
              TreeMap::new)
          );
      if (isEncode) {
        schema.setMd5Bytes(MD5.ofSchema(schema.toSchemaEntity()).bytes());
        newProperties.put(SchemaValue.ENCODED_PROPERTY, "true");
      } else {
        newProperties.remove(SchemaValue.ENCODED_PROPERTY);
      }

      schema.setMetadata(
          new Metadata(metadata.getTags(), newProperties, metadata.getSensitive()));
    } catch (GeneralSecurityException e) {
      throw new SchemaRegistryStoreException("Could not transform schema id " + schema.getId(), e);
    }
  }

  protected abstract KeysetHandle getOrCreateEncoder(String tenant);

  @Override
  public abstract void close();
}
