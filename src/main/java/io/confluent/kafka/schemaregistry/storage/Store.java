/**
 * 
 */
package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.utils.Pair;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author nnarkhed
 *
 */
public interface Store<K,V> {
    public void init() throws StoreInitializationException;
    public V get(K key) throws StoreException;
    public void put(K key, V value) throws StoreException;
    public Iterator<V> getAll(K key1, K key2) throws StoreException;
    public void putAll(Map<K, V> entries) throws StoreException;
    public void delete(K key) throws StoreException;
    public void close();
}
