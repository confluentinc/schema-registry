/**
 *
 */
package io.confluent.kafka.schemaregistry.storage;

import java.util.Iterator;
import java.util.Map;

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;

/**
 * @author nnarkhed
 */
public interface Store<K, V> {

  public void init() throws StoreInitializationException;

  public V get(K key) throws StoreException;

  public void put(K key, V value) throws StoreException;

  /**
   * Iterator over keys in the specified range
   *
   * @param key1 If key1 is null, start from the first key in sorted order
   * @param key2 If key2 is null, end at the last key
   * @return Iterator over keys in the half-open interval [key1, key2). If both keys are
   * null, return an iterator over all keys in the database
   */
  public Iterator<V> getAll(K key1, K key2) throws StoreException;

  public void putAll(Map<K, V> entries) throws StoreException;

  public void delete(K key) throws StoreException;

  public void close();
}
