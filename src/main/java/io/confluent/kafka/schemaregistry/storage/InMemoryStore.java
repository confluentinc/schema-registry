/**
 * 
 */
package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 * In-memory store based on maps
 * @author nnarkhed
 *
 */
public class InMemoryStore<K,V> implements Store<K,V> {
    private final ConcurrentSkipListMap<K,V> store;

    public InMemoryStore() {
        store = new ConcurrentSkipListMap<K,V>();
    }

    public void init() throws StoreInitializationException {
        // do nothing
    }

    @Override public V get(K key) {
        return store.get(key);
    }

    @Override public void put(K key, V value) throws StoreException {
        store.put(key, value);
    }

    @Override public Iterator<V> getAll(K key1, K key2) {
        ConcurrentNavigableMap<K, V> subMap = (key1 == null && key2 == null) ?
            store : store.subMap(key1, key2);
        return subMap.values().iterator();
    }

    @Override public void putAll(Map<K, V> entries) {
        store.putAll(entries);
    }

    @Override public void delete(K key) throws StoreException {
        store.remove(key);
    }

    @Override public void close() {
        store.clear();
    }
}
