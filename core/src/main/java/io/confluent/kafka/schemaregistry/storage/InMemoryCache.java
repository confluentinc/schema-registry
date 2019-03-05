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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;


/**
 * In-memory store based on maps
 */
public class InMemoryCache<K, V> implements LookupCache<K, V> {
  private final ConcurrentSkipListMap<K, V> store;
  private final Map<Integer, SchemaKey> guidToSchemaKey;
  private final Map<MD5, SchemaIdAndSubjects> schemaHashToGuid;
  private final Map<Integer, List<SchemaKey>> guidToDeletedSchemaKeys;

  public InMemoryCache() {
    this.store = new ConcurrentSkipListMap<>();
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
    guidToDeletedSchemaKeys
        .computeIfAbsent(schemaValue.getId(), k -> new ArrayList<>()).add(schemaKey);
  }

  @Override
  public void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue) {
    guidToSchemaKey.put(schemaValue.getId(), schemaKey);

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
  public boolean hasSubjects(String subject) throws StoreException {
    return hasSubjects(matchingPredicate(subject));
  }

  public boolean hasSubjects(Predicate<String> match) throws StoreException {
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
  public void clearSubjects(String subject) throws StoreException {
    clearSubjects(matchingPredicate(subject));
  }

  public void clearSubjects(Predicate<String> match) throws StoreException {
    Predicate<SchemaValue> matchDeleted = value -> {
      if (value != null && value.isDeleted()) {
        return match.test(value.getSubject());
      }
      return false;
    };

    // Try to replace deleted schemas with non-deleted, otherwise remove
    replaceMatchingDeletedWithNonDeletedOrRemove(match);

    // Delete from non-store structures first as they rely on the store
    schemaHashToGuid.values().forEach(
        v -> v.removeIf(k -> matchDeleted.test((SchemaValue) store.get(k))));
    guidToDeletedSchemaKeys.values().forEach(
        v -> v.removeIf(k -> matchDeleted.test((SchemaValue) store.get(k))));

    // Delete from store later as the previous deletions rely on the store
    store.entrySet().removeIf(e -> {
      if (e.getKey() instanceof SchemaKey) {
        return matchDeleted.test((SchemaValue) e.getValue());
      }
      return false;
    });
  }

  // Visible for testing
  protected void replaceMatchingDeletedWithNonDeletedOrRemove(Predicate<String> match) {
    Predicate<SchemaValue> matchDeleted = value -> {
      if (value != null && value.isDeleted()) {
        return match.test(value.getSubject());
      }
      return false;
    };

    Iterator<Map.Entry<Integer, SchemaKey>> it = guidToSchemaKey.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Integer, SchemaKey> entry = it.next();
      SchemaKey schemaKey = entry.getValue();
      SchemaValue schemaValue = (SchemaValue) store.get(schemaKey);
      if (matchDeleted.test(schemaValue)) {
        SchemaKey newSchemaKey = getNonDeletedSchemaKey(schemaValue.getSchema());
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
    return keys.findAny(key -> !((SchemaValue) store.get(key)).isDeleted());
  }

  private Predicate<String> matchingPredicate(String subject) {
    return s -> subject == null || subject.equals(s);
  }

}
