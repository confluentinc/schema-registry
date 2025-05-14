/*
 * Copyright 2025 Confluent Inc.
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

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.ClearSubjectValue;
import io.confluent.kafka.schemaregistry.storage.DeleteSubjectValue;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryKey;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryKeyType;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryValue;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.resourcemanager.api.model.scope.ScopingAttribute;
import io.confluent.resourcemanager.protobuf.apis.meta.v1.ObjectMeta;
import io.confluent.resourcemanager.protobuf.apis.meta.v1.ScopeAttribute;
import io.kcache.CacheUpdateHandler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;


public class ResourceManagerStoreUpdateHandler<K, V> implements CacheUpdateHandler<String, KeysetWrapper> {

  private static final Logger log = LoggerFactory.getLogger(ResourceManagerStoreUpdateHandler.class);
  private final io.confluent.kafka.schemaregistry.storage.encoder.MetadataEncoderRMService rmService;
  private final Aead aead;
  private final String ORG_NAME = "schema-registry-metadata-encoder-service";

  public ResourceManagerStoreUpdateHandler(SchemaRegistryConfig config) {
    try {
      this.rmService = new io.confluent.kafka.schemaregistry.storage.encoder.MetadataEncoderRMService("schema-registry-metadata-encoder-service");
      String secret = MetadataEncoderService.encoderSecret(config);
      aead = MetadataEncoderService.getPrimitive(secret);
    } catch (GeneralSecurityException e) {
      throw new ConfigException("Error while configuring ResourceManagerStoreUpdateHandler", e);
    }
  }

  @Override
  public ValidationStatus validateUpdate(Headers headers, String key, KeysetWrapper value,
                                         TopicPartition tp, long offset, long timestamp,
                                         TimestampType tsType, Optional<Integer> leaderEpoch) {
    // Add any validation logic here
    return ValidationStatus.SUCCESS;
  }

  @Override
  public void handleUpdate(String key, KeysetWrapper value, KeysetWrapper oldValue, TopicPartition tp, long offset, long ts) {
    try {
      log.info("RMStore handling update");
      if (value == null) {
        log.info("Handling deletion for key: {}", key);
        rmService.deleteKeysetWrapper(key).get();
        log.info("Successfully deleted keyset for key: {}", key);
      } else {
        log.info("Handling update for key: {}, rotation needed: {}", key, value.isRotationNeeded());

        // Get KeysetHandle and serialize it
        KeysetHandle keysetHandle = value.getKeysetHandle();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        keysetHandle.write(JsonKeysetWriter.withOutputStream(baos), aead);

        // Create MetadataKeysetWrapper with serialized data
        MetadataKeysetWrapper metadataWrapper = MetadataKeysetWrapper.newBuilder()
                .setMetadata(ObjectMeta.newBuilder()
                        .setName(key)
                        .addScope(ScopeAttribute.newBuilder()
                                .setName(ScopingAttribute.ORG.scopeName())
                                .setValue(ORG_NAME)
                                .build())
                        .build())
                .setSpec(MetadataKeysetWrapperSpec.newBuilder()
                        .setSerializedKeyset(ByteString.copyFrom(baos.toByteArray()))
                        .setRotationNeeded(value.isRotationNeeded())
                        .build())
                .build();

        // First try to get the existing keyset
        log.debug("Checking if keyset exists for key: {}", key);
        Optional<MetadataKeysetWrapper> existingKeyset = rmService.getKeysetWrapper(key).get();

        log.info("RM Write here");
        if (existingKeyset.isPresent()) {
          log.info("Keyset exists for key: {}, performing update", key);
          rmService.updateKeysetWrapper(metadataWrapper).get();
          log.info("Successfully updated keyset for key: {}", key);
        } else {
          log.info("No existing keyset for key: {}, creating new one", key);
          rmService.createKeysetWrapper(metadataWrapper).get();
          log.info("Successfully created keyset for key: {}", key);
        }
      }
    } catch (Exception e) {
      log.error("Failed to update RM storage for key: {}, error: {}", key, e.getMessage(), e);
    }
  }

  public void sendDummyTransaction() {
    try {
      // Create a fake serialized keyset as a byte array
      byte[] fakeSerializedKeyset = "fake-keyset".getBytes(StandardCharsets.UTF_8);

      // Create a MetadataKeysetWrapper with the fake serialized data
      String dummyKey = "dummyKey";
      MetadataKeysetWrapper metadataWrapper = MetadataKeysetWrapper.newBuilder()
              .setMetadata(ObjectMeta.newBuilder()
                      .setName(dummyKey)
                      .addScope(ScopeAttribute.newBuilder()
                              .setName(ScopingAttribute.ORG.scopeName())
                              .setValue(ORG_NAME)
                              .build())
                      .build())
              .setSpec(MetadataKeysetWrapperSpec.newBuilder()
                      .setSerializedKeyset(ByteString.copyFrom(fakeSerializedKeyset))
                      .setRotationNeeded(false) // Assuming no rotation is needed for the dummy
                      .build())
              .build();

      // Create the dummy entry in the Resource Manager
      rmService.createKeysetWrapper(metadataWrapper).get();
      log.info("Successfully created dummy keyset for key: {}", dummyKey);
    } catch (Exception e) {
      log.error("Failed to create dummy transaction", e);
    }
  }

  @Override
  public void startBatch(int count) {
    // Optional: Implement batch start logic
  }

  @Override
  public void endBatch(int count) {
    // Optional: Implement batch end logic
  }

  @Override
  public void failBatch(int count, Throwable t) {
    log.error("Batch failed with count: {}", count, t);
  }

  @Override
  public Map<TopicPartition, Long> checkpoint(int count) {
    // Return current offsets for checkpointing
    return null; // Let KafkaCache handle default checkpointing
  }

  @Override
  public void cacheInitialized(int count, Map<TopicPartition, Long> checkpoints) {
    log.info("Cache initialized with {} records", count);
  }

  @Override
  public void cacheReset() {
    log.info("Cache reset");
  }

  @Override
  public void cacheSynchronized(int count, Map<TopicPartition, Long> checkpoints) {
    log.info("Cache synchronized with {} records", count);
  }

  @Override
  public void cacheFlushed() {
    log.info("Cache flushed");
  }

  @Override
  public void close() {
    // Cleanup if needed
  }

  public List<KeysetHandle> getKeysets(String tenant) {
    log.info("Getting keysets for tenant: {}", tenant);
    List<KeysetHandle> keysetHandles = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String currentTenant = i == 0 ? tenant : tenant + "_" + i;
      try {
        Optional<MetadataKeysetWrapper> keysetWrapperOpt = rmService.getKeysetWrapper(currentTenant).get();
        if (keysetWrapperOpt.isPresent()) {
          MetadataKeysetWrapper metadataWrapper = keysetWrapperOpt.get();
          ByteString serializedKeyset = metadataWrapper.getSpec().getSerializedKeyset();

          // Convert the serialized keyset back to a KeysetHandle
          KeysetHandle keysetHandle = KeysetHandle.read(
                  JsonKeysetReader.withBytes(serializedKeyset.toByteArray()), aead);

          // Add the KeysetHandle to the list
          keysetHandles.add(keysetHandle);
          log.info("Keyset handle found for tenant: {}", currentTenant);
        } else {
          log.warn("No keyset found for tenant: {}", currentTenant);
        }
      } catch (Exception e) {
        log.error("Failed to retrieve keyset for tenant: {}, error: {}", currentTenant, e.getMessage(), e);
      }
    }
    return keysetHandles;
  }

  public void handleGet(String key, KeysetWrapper value) {
    if (value != null) {
      try {
        log.info("Handling get for key: {}, rotation needed: {}", key, value.isRotationNeeded());

        // Get KeysetHandle and serialize it
        KeysetHandle keysetHandle = value.getKeysetHandle();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        keysetHandle.write(JsonKeysetWriter.withOutputStream(baos), aead);

        // Create MetadataKeysetWrapper with serialized data
        MetadataKeysetWrapper metadataWrapper = MetadataKeysetWrapper.newBuilder()
                .setMetadata(ObjectMeta.newBuilder()
                        .setName(key)
                        .addScope(ScopeAttribute.newBuilder()
                                .setName(ScopingAttribute.ORG.scopeName())
                                .setValue(ORG_NAME)
                                .build())
                        .build())
                .setSpec(MetadataKeysetWrapperSpec.newBuilder()
                        .setSerializedKeyset(ByteString.copyFrom(baos.toByteArray()))
                        .setRotationNeeded(value.isRotationNeeded())
                        .build())
                .build();

        // First try to get the existing keyset
        log.debug("Checking if keyset exists for key: {}", key);
        Optional<MetadataKeysetWrapper> existingKeyset = rmService.getKeysetWrapper(key).get();

        if (existingKeyset.isPresent()) {
          log.info("Keyset exists for key: {}, performing update", key);
          rmService.updateKeysetWrapper(metadataWrapper).get();
          log.info("Successfully updated keyset for key: {}", key);
        } else {
          log.info("No existing keyset for key: {}, creating new one", key);
          rmService.createKeysetWrapper(metadataWrapper).get();
          log.info("Successfully created keyset for key: {}", key);
        }
      } catch (Exception e) {
        log.error("Failed to write to RM after Kafka get for key: {}, error: {}", key, e.getMessage(), e);
      }
    } else {
      log.debug("No value found for key: {}, skipping RM write", key);
    }
  }
}
