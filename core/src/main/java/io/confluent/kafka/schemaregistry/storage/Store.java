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

import java.util.Map;

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;

public interface Store<K, V> extends AutoCloseable {

  /**
   * Whether the store is persistent.
   *
   * @return whether the store is persistent
   */
  default boolean isPersistent() {
    return false;
  }

  public void init() throws StoreInitializationException;

  public V get(K key) throws StoreException;

  public V put(K key, V value) throws StoreException;

  /**
   * Iterator over keys in the specified range
   *
   * @param key1 If key1 is null, start from the first key in sorted order
   * @param key2 If key2 is null, end at the last key
   * @return Iterator over keys in the half-open interval [key1, key2). If both keys are null,
   *     return an iterator over all keys in the database
   */
  public CloseableIterator<V> getAll(K key1, K key2) throws StoreException;

  public void putAll(Map<K, V> entries) throws StoreException;

  public V delete(K key) throws StoreException;

  public CloseableIterator<K> getAllKeys() throws StoreException;

  public void flush() throws StoreException;

  public void close() throws StoreException;
}
