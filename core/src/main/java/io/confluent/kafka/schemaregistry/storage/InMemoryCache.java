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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;


/**
 * In-memory store based on maps
 */
public class InMemoryCache<K, V> implements LookupCache<K, V> {
  // visible for subclasses
  protected final ConcurrentNavigableMap<K, V> store;
  private final Map<Integer, SchemaKey> guidToSchemaKey;
  private final Map<MD5, SchemaIdAndSubjects> schemaHashToGuid;
  private final Map<Integer, List<SchemaKey>> guidToDeletedSchemaKeys;

  public InMemoryCache() {
    this(new ConcurrentSkipListMap<>());
  }

  public InMemoryCache(ConcurrentNavigableMap<K, V> store) {
    this.store = store;
    this.guidToSchemaKey = new ConcurrentHashMap<>();
    this.schemaHashToGuid = new ConcurrentHashMap<>();
    this.guidToDeletedSchemaKeys = new ConcurrentHashMap<>();
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

  @Override
  public SchemaIdAndSubjects schemaIdAndSubjects(Schema schema) {
    MD5 md5 = MD5.ofString(schema.getSchema());
    return schemaHashToGuid.get(md5);
  }

  @Override
  public boolean containsSchema(Schema schema) {
    MD5 md5 = MD5.ofString(schema.getSchema());
    return this.schemaHashToGuid.containsKey(md5);
  }

  @Override
  public SchemaKey schemaKeyById(Integer id) {
    return guidToSchemaKey.get(id);
  }

  @Override
  public void schemaDeleted(SchemaKey schemaKey, SchemaValue schemaValue) {
    guidToSchemaKey.put(schemaValue.getId(), schemaKey);
    // We ensure the schema is registered by its hash; this is necessary in case of a
    // compaction when the previous non-deleted schemaValue will not get registered
    addToSchemaHashToGuid(schemaKey, schemaValue);
    guidToDeletedSchemaKeys
        .computeIfAbsent(schemaValue.getId(), k -> new CopyOnWriteArrayList<>())
        .add(schemaKey);
  }

  @Override
  public void schemaTombstoned(SchemaKey schemaKey) {
    // Don't need to update guidToSchemaKey since the tombstone action was initiated by
    // a newer schema being registered to guidToSchemaKey
    schemaHashToGuid.values().forEach(v -> v.removeIf(k -> k.equals(schemaKey)));
    guidToDeletedSchemaKeys.values().forEach(v -> v.removeIf(k -> k.equals(schemaKey)));
  }

  @Override
  public void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue) {
    guidToSchemaKey.put(schemaValue.getId(), schemaKey);
    addToSchemaHashToGuid(schemaKey, schemaValue);
  }

  private void addToSchemaHashToGuid(SchemaKey schemaKey, SchemaValue schemaValue) {
    MD5 md5 = MD5.ofString(schemaValue.getSchema());
    SchemaIdAndSubjects schemaIdAndSubjects = schemaHashToGuid.get(md5);
    if (schemaIdAndSubjects == null) {
      schemaIdAndSubjects = new SchemaIdAndSubjects(schemaValue.getId());
    }
    schemaIdAndSubjects.addSubjectAndVersion(schemaKey.getSubject(), schemaKey.getVersion());
    schemaHashToGuid.put(md5, schemaIdAndSubjects);
  }

  @Override
  public List<SchemaKey> deletedSchemaKeys(SchemaValue schemaValue) {
    return guidToDeletedSchemaKeys.getOrDefault(schemaValue.getId(), Collections.emptyList());
  }

  @Override
  @SuppressWarnings("unchecked")
  public AvroCompatibilityLevel compatibilityLevel(String subject,
                                                   boolean returnTopLevelIfNotFound,
                                                   AvroCompatibilityLevel defaultForTopLevel
  ) {
    ConfigKey subjectConfigKey = new ConfigKey(subject);
    ConfigValue config = (ConfigValue) get((K) subjectConfigKey);
    if (config == null && subject == null) {
      return defaultForTopLevel;
    }
    if (config != null) {
      return config.getCompatibilityLevel();
    } else if (returnTopLevelIfNotFound) {
      config = (ConfigValue) get((K) new ConfigKey(null));
      return config != null ? config.getCompatibilityLevel() : defaultForTopLevel;
    } else {
      return null;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Mode mode(String subject,
                   boolean returnTopLevelIfNotFound,
                   Mode defaultForTopLevel
  ) {
    ModeKey modeKey = new ModeKey(subject);
    ModeValue modeValue = (ModeValue) get((K) modeKey);
    if (modeValue == null && subject == null) {
      return defaultForTopLevel;
    }
    if (modeValue != null) {
      return modeValue.getMode();
    } else if (returnTopLevelIfNotFound) {
      modeValue = (ModeValue) get((K) new ModeKey(null));
      return modeValue != null ? modeValue.getMode() : defaultForTopLevel;
    } else {
      return null;
    }
  }

  @Override
  public boolean hasSubjects(String subject) {
    return hasSubjects(matchingPredicate(subject));
  }

  public boolean hasSubjects(Predicate<String> match) {
    return store.entrySet().stream()
        .anyMatch(e -> {
          K k = e.getKey();
          V v = e.getValue();
          if (k instanceof SchemaKey) {
            SchemaKey key = (SchemaKey) k;
            SchemaValue value = (SchemaValue) v;
            if (value != null && !value.isDeleted()) {
              return match.test(key.getSubject());
            }
          }
          return false;
        });
  }

  @Override
  public void clearSubjects(String subject) {
    clearSubjects(matchingPredicate(subject));
  }

  public void clearSubjects(Predicate<String> match) {
    Predicate<SchemaKey> matchDeleted = matchDeleted(match);

    // Try to replace deleted schemas with non-deleted, otherwise remove
    replaceMatchingDeletedWithNonDeletedOrRemove(match);

    // Delete from non-store structures first as they rely on the store
    schemaHashToGuid.values().forEach(v -> v.removeIf(matchDeleted));
    schemaHashToGuid.entrySet().removeIf(e -> e.getValue().isEmpty());
    guidToDeletedSchemaKeys.values().forEach(v -> v.removeIf(matchDeleted));
    guidToDeletedSchemaKeys.entrySet().removeIf(e -> e.getValue().isEmpty());

    // Delete from store later as the previous deletions rely on the store
    store.entrySet().removeIf(e -> {
      if (e.getKey() instanceof SchemaKey) {
        SchemaKey key = (SchemaKey) e.getKey();
        SchemaValue value = (SchemaValue) e.getValue();
        return match.test(key.getSubject()) && value.isDeleted();
      }
      return false;
    });
  }

  private Predicate<String> matchingPredicate(String subject) {
    return s -> subject == null || subject.equals(s);
  }

  // Visible for testing
  protected void replaceMatchingDeletedWithNonDeletedOrRemove(Predicate<String> match) {
    Predicate<SchemaKey> matchDeleted = matchDeleted(match);

    // Iterate through the entries, and for each entry that matches and is soft deleted,
    // see if there is a replacement entry (that is not soft deleted) that has the same
    // schema string.  If so, replace, else remove the entry.
    Iterator<Map.Entry<Integer, SchemaKey>> it = guidToSchemaKey.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Integer, SchemaKey> entry = it.next();
      SchemaKey schemaKey = entry.getValue();
      if (matchDeleted.test(schemaKey)) {
        SchemaValue schemaValue = (SchemaValue) store.get(schemaKey);
        // The value returned from the store should not be null since we clean up caches
        // after tombstoning, but we still check defensively
        SchemaKey newSchemaKey = schemaValue != null
                                 ? getNonDeletedSchemaKey(schemaValue.getSchema())
                                 : null;
        if (newSchemaKey != null) {
          entry.setValue(newSchemaKey);
        } else {
          it.remove();
        }
      }
    }
  }

  private SchemaKey getNonDeletedSchemaKey(String schema) {
    MD5 md5 = MD5.ofString(schema);
    SchemaIdAndSubjects keys = schemaHashToGuid.get(md5);
    return keys == null ? null : keys.findAny(key -> {
      SchemaValue value = (SchemaValue) store.get(key);
      // The value returned from the store should not be null since we clean up caches
      // after tombstoning, but we still check defensively
      return value != null && !value.isDeleted();
    });
  }

  private Predicate<SchemaKey> matchDeleted(Predicate<String> match) {
    return key -> {
      if (match.test(key.getSubject())) {
        SchemaValue value = (SchemaValue) store.get(key);
        // The value returned from the store should not be null since we clean up caches
        // after tombstoning, but we still check defensively
        return value == null || value.isDeleted();
      }
      return false;
    };
  }
}
