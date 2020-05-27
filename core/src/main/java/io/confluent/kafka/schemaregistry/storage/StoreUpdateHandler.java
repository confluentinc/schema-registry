/*
 * Copyright 2018 Confluent Inc.
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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface StoreUpdateHandler<K, V> extends Configurable {

  String SCHEMA_REGISTRY = "schemaRegistry";

  @Override
  default void configure(Map<String, ?> map) {
  }

  /**
   * Invoked before every new K,V pair written to the store
   *
   * @param key   Key associated with the data
   * @param value Data written to the store
   * @param tp Topic-partition
   * @param offset Offset of record
   * @param timestamp Timestamp of record
   */
  default boolean validateUpdate(K key, V value, TopicPartition tp, long offset, long timestamp) {
    return true;
  }

  /**
   * Invoked on every new K,V pair written to the store
   *
   * @param key   Key associated with the data
   * @param value Data written to the store
   * @param oldValue the previous value associated with key, or null if there was no mapping for key
   * @param tp Topic-partition
   * @param offset Offset of record
   * @param timestamp Timestamp of record
   */
  void handleUpdate(K key, V value, V oldValue, TopicPartition tp, long offset, long timestamp);

  /**
   * Invoked after a batch of updates.
   *
   * @param count batch count
   */
  default void checkpoint(int count) {
  }
}
