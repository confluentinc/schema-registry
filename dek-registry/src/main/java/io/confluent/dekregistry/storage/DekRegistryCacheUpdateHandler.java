/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.dekregistry.storage;

import io.confluent.dekregistry.metrics.MetricsManager;
import io.kcache.CacheUpdateHandler;
import java.io.IOException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DekRegistryCacheUpdateHandler
    implements CacheUpdateHandler<EncryptionKeyId, EncryptionKey> {

  private static final Logger log = LoggerFactory.getLogger(DekRegistryCacheUpdateHandler.class);

  private MetricsManager metricsManager;

  public DekRegistryCacheUpdateHandler(MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
  }

  /**
   * Invoked on every new schema written to the Kafka store
   *
   * @param key   Key associated with the schema.
   * @param value Value written to the Kafka lookupCache
   */
  @Override
  public void handleUpdate(
      EncryptionKeyId key,
      EncryptionKey value,
      EncryptionKey oldValue,
      TopicPartition tp,
      long offset,
      long timestamp) {
    String tenant = key.getTenant();
    if (value == null) {
      if (oldValue != null) {
        metricsManager.decrementKeyCount(tenant, key.getType());
      }
    } else if (oldValue == null) {
      metricsManager.incrementKeyCount(tenant, key.getType());
    }
  }

  @Override
  public void close() throws IOException {
  }
}
