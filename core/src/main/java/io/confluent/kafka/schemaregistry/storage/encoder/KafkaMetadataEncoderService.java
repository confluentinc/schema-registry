/*
 * Copyright 2026 Confluent Inc.
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
import com.google.crypto.tink.KeysetHandle;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.kcache.Cache;
import io.kcache.CacheUpdateHandler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Set;

public class KafkaMetadataEncoderService extends AbstractMetadataEncoderService {

  private static final Logger log = LoggerFactory.getLogger(KafkaMetadataEncoderService.class);

  protected Cache<String, KeysetWrapper> encoders = null;

  public KafkaMetadataEncoderService(SchemaRegistry schemaRegistry) {
    super(schemaRegistry);
    if (encoderSecret == null) {
      return;
    }
    SchemaRegistryConfig config = schemaRegistry.config();
    String topic = config.getString(SchemaRegistryConfig.METADATA_ENCODER_TOPIC_CONFIG);
    this.encoders = createCache(new Serdes.StringSerde(), new KeysetWrapperSerde(config), topic,
        new TenantCacheUpdateHandler());
  }

  @VisibleForTesting
  protected KafkaMetadataEncoderService(
      SchemaRegistry schemaRegistry, Cache<String, KeysetWrapper> encoders) {
    super(schemaRegistry);
    if (encoderSecret == null) {
      return;
    }
    this.encoders = encoders;
  }

  @Override
  protected void doInit() {
    if (encoders != null) {
      encoders.init();
    }
  }

  protected Set<String> getAllTenants() {
    return encoders != null ? encoders.keySet() : Collections.emptySet();
  }

  protected KeysetWrapper getEncoderWrapper(String tenant) {
    return encoders != null ? encoders.get(tenant) : null;
  }

  protected void putEncoderWrapper(String tenant, KeysetWrapper wrapper) {
    if (encoders != null) {
      encoders.put(tenant, wrapper);
    }
  }

  @VisibleForTesting
  protected KeysetHandle getEncoder(String tenant) {
    if (encoders == null) {
      return null;
    }
    KeysetWrapper wrapper = encoders.get(tenant);
    if (wrapper == null) {
      // Ensure encoders are up to date
      encoders.sync();
      wrapper = encoders.get(tenant);
    }
    return wrapper != null ? wrapper.getKeysetHandle() : null;
  }

  protected KeysetHandle getOrCreateEncoder(String tenant) {
    // Ensure encoders are up to date
    encoders.sync();
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
    log.info("Shutting down KafkaMetadataEncoderService");
    if (encoders != null) {
      try {
        encoders.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  /**
   * Cache update handler that logs tenant (key) updates to the encoder cache.
   */
  private static class TenantCacheUpdateHandler
      implements CacheUpdateHandler<String, KeysetWrapper> {

    @Override
    public void handleUpdate(String tenant, KeysetWrapper newValue, KeysetWrapper oldValue,
                             TopicPartition tp, long offset, long timestamp) {
      if (oldValue == null) {
        log.info("Encoder cache update: new tenant '{}' added (partition={}, offset={}, "
            + "timestamp={})", tenant, tp.partition(), offset, timestamp);
      } else if (newValue == null) {
        log.info("Encoder cache update: tenant '{}' removed (partition={}, offset={}, "
            + "timestamp={})", tenant, tp.partition(), offset, timestamp);
      } else {
        log.info("Encoder cache update: tenant '{}' updated (partition={}, offset={}, "
            + "timestamp={})", tenant, tp.partition(), offset, timestamp);
      }
    }
  }
}
