/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CompositeSchemaUpdateHandler implements SchemaUpdateHandler {

  private static final Logger log = LoggerFactory.getLogger(CompositeSchemaUpdateHandler.class);

  private final List<SchemaUpdateHandler> handlers;

  public CompositeSchemaUpdateHandler(List<SchemaUpdateHandler> handlers) {
    this.handlers = handlers;
  }

  /**
   * Invoked after the cache is initialized.
   */
  @Override
  public void cacheInitialized(Map<TopicPartition, Long> checkpoints) {
    for (SchemaUpdateHandler handler : handlers) {
      handler.cacheInitialized(checkpoints);
    }
  }

  @Override
  public void startBatch(int count) {
    for (SchemaUpdateHandler handler : handlers) {
      handler.startBatch(count);
    }
  }

  /**
   * Invoked before every new K,V pair written to the store
   *
   * @param key   Key associated with the data
   * @param value Data written to the store
   */
  @Override
  public ValidationStatus validateUpdate(SchemaRegistryKey key, SchemaRegistryValue value,
                                         TopicPartition tp, long offset, long timestamp) {
    for (SchemaUpdateHandler handler : handlers) {
      ValidationStatus status = handler.validateUpdate(key, value, tp, offset, timestamp);
      if (status != ValidationStatus.SUCCESS) {
        return status;
      }
    }
    return ValidationStatus.SUCCESS;
  }

  /**
   * Invoked on every new schema written to the Kafka store
   *
   * @param key   Key associated with the schema.
   * @param value Value written to the Kafka lookupCache
   */
  @Override
  public void handleUpdate(SchemaRegistryKey key,
                           SchemaRegistryValue value,
                           SchemaRegistryValue oldValue,
                           TopicPartition tp,
                           long offset,
                           long timestamp) {
    for (SchemaUpdateHandler handler : handlers) {
      handler.handleUpdate(key, value, oldValue, tp, offset, timestamp);
    }
  }

  @Override
  public Map<TopicPartition, Long> checkpoint(int count) {
    Map<TopicPartition, Long> result = null;
    for (SchemaUpdateHandler handler : handlers) {
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
    for (SchemaUpdateHandler handler : handlers) {
      handler.endBatch(count);
    }
  }

  @Override
  public void close() throws IOException {
    for (SchemaUpdateHandler handler : handlers) {
      handler.close();
    }
  }
}
