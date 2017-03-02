/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;


/**
 * In-memory store based on maps
 */
public class InMemoryStore<K, V> implements Store<K, V> {

  private final ConcurrentSkipListMap<K, V> store;

  public InMemoryStore() {
    store = new ConcurrentSkipListMap<K, V>();
  }

  public void init() throws StoreInitializationException {
    // do nothing
  }

  @Override
  public V get(K key) {
    return store.get(key);
  }

  @Override
  public void put(K key, V value) throws StoreException {
    store.put(key, value);
  }

  @Override
  public Iterator<V> getAll(K key1, K key2) {
    ConcurrentNavigableMap<K, V> subMap = (key1 == null && key2 == null)
                                          ? store
                                          : store.subMap(key1, key2);
    return subMap.values().iterator();
  }

  @Override
  public void putAll(Map<K, V> entries) {
    store.putAll(entries);
  }

  @Override
  public void delete(K key) throws StoreException {
    store.remove(key);
  }

  @Override
  public Iterator<K> getAllKeys() throws StoreException {
    return store.keySet().iterator();
  }

  @Override
  public void close() {
    store.clear();
  }
}
