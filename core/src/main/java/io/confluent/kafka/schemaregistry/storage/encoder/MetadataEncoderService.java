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
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.MD5;
import io.confluent.kafka.schemaregistry.storage.Metadata;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.kcache.Cache;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.exceptions.CacheInitializationException;
import io.kcache.utils.Caches;
import io.kcache.utils.InMemoryCache;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Base64;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataEncoderService implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MetadataEncoderService.class);

  private static final String AES_GCM_KEY = "type.googleapis.com/google.crypto.tink.AesGcmKey";
  private static final byte[] EMPTY_AAD = new byte[0];
  private static final String KEY_TEMPLATE_NAME = "AES128_GCM";

  private final KafkaSchemaRegistry schemaRegistry;
  private KeyTemplate keyTemplate;
  // visible for testing
  Cache<String, KeysetWrapper> encoders = null;
  private final AtomicBoolean initialized = new AtomicBoolean();
  private final CountDownLatch initLatch = new CountDownLatch(1);

  static {
    try {
      AeadConfig.register();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public MetadataEncoderService(SchemaRegistry schemaRegistry) {
    try {
      this.schemaRegistry = (KafkaSchemaRegistry) schemaRegistry;
      SchemaRegistryConfig config = schemaRegistry.config();
      String secret = encoderSecret(config);
      if (secret == null) {
        log.warn("No value specified for {}, sensitive metadata will not be encoded",
            SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG);
        return;
      }
      String topic = config.getString(SchemaRegistryConfig.METADATA_ENCODER_TOPIC_CONFIG);
      this.keyTemplate = KeyTemplates.get(KEY_TEMPLATE_NAME);
      this.encoders = createCache(new StringSerde(), new KeysetWrapperSerde(config), topic, null);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not instantiate MetadataEncoderService", e);
    }
  }

  @VisibleForTesting
  protected MetadataEncoderService(
      SchemaRegistry schemaRegistry, Cache<String, KeysetWrapper> encoders) {
    try {
      this.schemaRegistry = (KafkaSchemaRegistry) schemaRegistry;
      SchemaRegistryConfig config = schemaRegistry.config();
      String secret = encoderSecret(config);
      if (secret == null) {
        log.warn("No value specified for {}, sensitive metadata will not be encoded",
            SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG);
        return;
      }
      this.keyTemplate = KeyTemplates.get(KEY_TEMPLATE_NAME);
      this.encoders = encoders;
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not instantiate MetadataEncoderService", e);
    }
  }

  public KafkaSchemaRegistry getSchemaRegistry() {
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

  protected <K, V> Cache<K, V> createCache(
      Serde<K> keySerde,
      Serde<V> valueSerde,
      String topic,
      CacheUpdateHandler<K, V> cacheUpdateHandler) throws CacheInitializationException {
    Properties props = getKafkaCacheProperties(topic);
    KafkaCacheConfig config = new KafkaCacheConfig(props);
    Cache<K, V> kafkaCache = Caches.concurrentCache(
        new KafkaCache<>(config,
            keySerde,
            valueSerde,
            cacheUpdateHandler,
            new InMemoryCache<>()));
    getSchemaRegistry().addLeaderChangeListener(isLeader -> {
      if (isLeader) {
        // Reset the cache to remove any stale data from previous leadership
        kafkaCache.reset();
        // Ensure the new leader catches up with the offsets
        kafkaCache.sync();
      }
    });
    return kafkaCache;
  }

  private Properties getKafkaCacheProperties(String topic) {
    Properties props = new Properties();
    props.putAll(schemaRegistry.config().originalProperties());
    Set<String> keys = props.stringPropertyNames();
    for (String key : keys) {
      if (key.startsWith("kafkastore.")) {
        String newKey = key.replace("kafkastore", "kafkacache");
        if (!keys.contains(newKey)) {
          props.put(newKey, props.get(key));
        }
      }
    }
    props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
    return props;
  }

  public void init() {
    if (encoders != null && !initialized.get()) {
      encoders.init();
      maybeRotateSecrets();
      boolean isInitialized = initialized.compareAndSet(false, true);
      if (!isInitialized) {
        throw new IllegalStateException("Metadata encoder service was already initialized");
      }
      initLatch.countDown();
    }
  }

  private void maybeRotateSecrets() {
    String oldSecret = encoderOldSecret(schemaRegistry.config());
    if (oldSecret != null) {
      log.info("Rotating encoder secret");
      for (String key : encoders.keySet()) {
        KeysetWrapper wrapper = encoders.get(key);
        if (wrapper.isRotationNeeded()) {
          try {
            KeysetHandle handle = wrapper.getKeysetHandle();
            KeysetHandle rotatedHandle = KeysetManager
                .withKeysetHandle(handle)
                .add(keyTemplate)
                .getKeysetHandle();
            int keyId = rotatedHandle.getAt(rotatedHandle.size()).getId();
            rotatedHandle = KeysetManager
                .withKeysetHandle(rotatedHandle)
                .setPrimary(keyId)
                .getKeysetHandle();
            // This will cause the new secret to be used
            encoders.put(key, new KeysetWrapper(rotatedHandle, false));
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

  public KeysetHandle getEncoder(String tenant) {
    if (encoders == null) {
      return null;
    }
    KeysetWrapper wrapper = encoders.get(tenant);
    return wrapper != null ? wrapper.getKeysetHandle() : null;
  }

  public void encodeMetadata(SchemaValue schema) {
    if (!initialized.get() || schema == null || isEncoded(schema)) {
      return;
    }
    transformMetadata(schema, true, (aead, value) -> {
      try {
        byte[] ciphertext = aead.encrypt(value.getBytes(StandardCharsets.UTF_8), EMPTY_AAD);
        return Base64.getEncoder().encodeToString(ciphertext);
      } catch (GeneralSecurityException e) {
        throw new IllegalStateException("Could not encrypt sensitive metadata", e);
      }
    });
  }

  private boolean isEncoded(SchemaValue schema) {
    if (schema == null
        || schema.getMetadata() == null
        || schema.getMetadata().getProperties() == null) {
      return false;
    }
    return schema.getMetadata().getProperties().containsKey(SchemaValue.ENCODED_PROPERTY);
  }

  public void decodeMetadata(SchemaValue schema) {
    if (!initialized.get() || schema == null || !isEncoded(schema)) {
      return;
    }
    transformMetadata(schema, false, (aead, value) -> {
      try {
        byte[] plaintext = aead.decrypt(Base64.getDecoder().decode(value), EMPTY_AAD);
        return new String(plaintext, StandardCharsets.UTF_8);
      } catch (GeneralSecurityException e) {
        throw new IllegalStateException("Could not encrypt sensitive metadata", e);
      }
    });
  }

  private void transformMetadata(
      SchemaValue schema, boolean isEncode, BiFunction<Aead, String, String> func) {
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

      KeysetHandle handle = getOrCreateEncoder(tenant);
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
      throw new IllegalStateException("Could not encrypt sensitive metadata", e);
    }
  }

  private KeysetHandle getOrCreateEncoder(String tenant) {
    KeysetWrapper wrapper = encoders.computeIfAbsent(tenant,
        k -> {
          try {
            KeysetHandle handle = KeysetHandle.generateNew(keyTemplate);
            return new KeysetWrapper(handle, false);
          } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Could not create key template");
          }

        });
    return wrapper.getKeysetHandle();
  }

  @Override
  public void close() {
    log.info("Shutting down MetadataEncoderService");
    if (encoders != null) {
      try {
        encoders.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }
}
