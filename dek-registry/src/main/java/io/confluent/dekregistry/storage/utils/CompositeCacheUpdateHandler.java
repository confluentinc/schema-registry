/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.dekregistry.storage.utils;

import io.kcache.CacheUpdateHandler;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

public class CompositeCacheUpdateHandler<K, V> implements CacheUpdateHandler<K, V> {

  private final List<? extends CacheUpdateHandler<K, V>> handlers;

  public CompositeCacheUpdateHandler(List<? extends CacheUpdateHandler<K, V>> handlers) {
    this.handlers = handlers;
  }

  @Override
  public void cacheInitialized(int count, Map<TopicPartition, Long> checkpoints) {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      handler.cacheInitialized(count, checkpoints);
    }
  }

  @Override
  public void cacheReset() {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      handler.cacheReset();
    }
  }

  @Override
  public void cacheSynchronized(int count, Map<TopicPartition, Long> checkpoints) {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      handler.cacheSynchronized(count, checkpoints);
    }
  }

  @Override
  public void startBatch(int count) {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      handler.startBatch(count);
    }
  }

  @Override
  public ValidationStatus validateUpdate(Headers headers, K key, V value, TopicPartition tp,
      long offset, long ts, TimestampType tsType,
      Optional<Integer> leaderEpoch) {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      ValidationStatus status = handler.validateUpdate(key, value, tp, offset, ts);
      if (status != ValidationStatus.SUCCESS) {
        return status;
      }
    }
    return ValidationStatus.SUCCESS;
  }

  @Override
  public void handleUpdate(K key, V value, V oldValue, TopicPartition tp, long offset, long ts) {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      handler.handleUpdate(key, value, oldValue, tp, offset, ts);
    }
  }

  @Override
  public Map<TopicPartition, Long> checkpoint(int count) {
    Map<TopicPartition, Long> result = null;
    for (CacheUpdateHandler<K, V> handler : handlers) {
      Map<TopicPartition, Long> offsets = handler.checkpoint(count);
      if (offsets != null) {
        if (result != null) {
          for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            // When merging, choose the smaller offset
            result.merge(entry.getKey(), entry.getValue(), Long::min);
          }
        } else {
          result = new HashMap<>(offsets);
        }
      }
    }
    return result;
  }

  @Override
  public void endBatch(int count) {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      handler.endBatch(count);
    }
  }

  @Override
  public void failBatch(int count, Throwable t) {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      handler.failBatch(count, t);
    }
  }

  @Override
  public void cacheFlushed() {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      handler.cacheFlushed();
    }
  }

  @Override
  public void close() throws IOException {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      handler.close();
    }
  }

  @Override
  public void cacheDestroyed() {
    for (CacheUpdateHandler<K, V> handler : handlers) {
      handler.cacheDestroyed();
    }
  }
}
