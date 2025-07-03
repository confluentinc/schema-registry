/*
 * Copyright 2014-2021 Confluent Inc.
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

import org.apache.kafka.common.TopicPartition;

public class StringMessageHandler implements StoreUpdateHandler<String, String> {

  /**
   * Invoked before every new K,V pair written to the store
   *
   * @param key   Key associated with the data
   * @param value Data written to the store
   * @param timestamp Timestamp of record
   */
  public ValidationStatus validateUpdate(String key, String value,
                                         TopicPartition tp, long offset, long timestamp) {
    return ValidationStatus.SUCCESS;
  }

  /**
   * Invoked on every new K,V pair written to the store
   *
   * @param key   Key associated with the data
   * @param value Data written to the store
   * @param timestamp Timestamp of record
   */
  @Override
  public void handleUpdate(String key, String value, String oldValue,
                           TopicPartition tp, long offset, long timestamp) {

  }
}
