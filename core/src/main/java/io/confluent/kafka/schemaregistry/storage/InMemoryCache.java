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

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * In-memory store based on maps
 */
public class InMemoryCache<K, V> implements LookupCache<K, V> {
  // visible for subclasses
  protected final ConcurrentNavigableMap<K, V> store;
  private final Map<Integer, Map<String, Integer>> guidToSubjectVersions;
  private final Map<MD5, Integer> hashToGuid;
  private final Map<SchemaKey, Set<Integer>> referencedBy;

  public InMemoryCache() {
    this(new ConcurrentSkipListMap<>());
  }

  public InMemoryCache(ConcurrentNavigableMap<K, V> store) {
    this.store = store;
    this.guidToSubjectVersions = new ConcurrentHashMap<>();
    this.hashToGuid = new ConcurrentHashMap<>();
    this.referencedBy = new ConcurrentHashMap<>();
  }

  public void init() throws StoreInitializationException {
    // do nothing
  }

  @Override
  public V get(K key) {
    return store.get(key);
  }

  @Override
  public V put(K key, V value) throws StoreException {
    return store.put(key, value);
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
  public V delete(K key) throws StoreException {
    return store.remove(key);
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
    List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> refs
        = schema.getReferences();
    MD5 md5 = MD5.ofString(schema.getSchema(), refs == null ? null : refs.stream()
        .map(ref -> new SchemaReference(ref.getName(), ref.getSubject(), ref.getVersion()))
        .collect(Collectors.toList()));
    Integer id = hashToGuid.get(md5);
    if (id == null) {
      return null;
    }
    Map<String, Integer> subjectVersions = guidToSubjectVersions.get(id);
    if (subjectVersions == null || subjectVersions.isEmpty()) {
      return null;
    }
    return new SchemaIdAndSubjects(id, subjectVersions);
  }

  @Override
  public boolean containsSchema(Schema schema) {
    return schemaIdAndSubjects(schema) != null;
  }

  @Override
  public Set<Integer> referencesSchema(SchemaKey schema) {
    return referencedBy.getOrDefault(schema, new HashSet<>());
  }

  @Override
  public SchemaKey schemaKeyById(Integer id) {
    Map<String, Integer> subjectVersions = guidToSubjectVersions.get(id);
    if (subjectVersions == null || subjectVersions.isEmpty()) {
      return null;
    }
    Map.Entry<String, Integer> entry = subjectVersions.entrySet().iterator().next();
    return new SchemaKey(entry.getKey(), entry.getValue());
  }

  @Override
  public void schemaDeleted(SchemaKey schemaKey, SchemaValue schemaValue) {
    Map<String, Integer> subjectVersions =
        guidToSubjectVersions.computeIfAbsent(schemaValue.getId(), k -> new HashMap<>());
    subjectVersions.put(schemaKey.getSubject(), schemaKey.getVersion());
    // We ensure the schema is registered by its hash; this is necessary in case of a
    // compaction when the previous non-deleted schemaValue will not get registered
    addToSchemaHashToGuid(schemaKey, schemaValue);
    for (SchemaReference ref : schemaValue.getReferences()) {
      SchemaKey refKey = new SchemaKey(ref.getSubject(), ref.getVersion());
      Set<Integer> refBy = referencedBy.get(refKey);
      if (refBy != null) {
        refBy.remove(schemaValue.getId());
        if (refBy.isEmpty()) {
          referencedBy.remove(refKey);
        }
      }
    }
  }

  @Override
  public void schemaTombstoned(SchemaKey schemaKey, SchemaValue schemaValue) {
    if (schemaValue == null) {
      return;
    }
    Map<String, Integer> subjectVersions = guidToSubjectVersions.get(schemaValue.getId());
    if (subjectVersions == null || subjectVersions.isEmpty()) {
      return;
    }
    subjectVersions.computeIfPresent(schemaKey.getSubject(),
        (k, v) -> schemaKey.getVersion() == v ? null : v);
    if (subjectVersions.isEmpty()) {
      guidToSubjectVersions.remove(schemaValue.getId());
    }
  }

  @Override
  public void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue) {
    Map<String, Integer> subjectVersions =
        guidToSubjectVersions.computeIfAbsent(schemaValue.getId(), k -> new HashMap<>());
    subjectVersions.put(schemaKey.getSubject(), schemaKey.getVersion());
    addToSchemaHashToGuid(schemaKey, schemaValue);
    for (SchemaReference ref : schemaValue.getReferences()) {
      SchemaKey refKey = new SchemaKey(ref.getSubject(), ref.getVersion());
      Set<Integer> refBy = referencedBy.computeIfAbsent(refKey, k -> new HashSet<>());
      refBy.add(schemaValue.getId());
    }
  }

  private void addToSchemaHashToGuid(SchemaKey schemaKey, SchemaValue schemaValue) {
    MD5 md5 = MD5.ofString(schemaValue.getSchema(), schemaValue.getReferences());
    hashToGuid.put(md5, schemaValue.getId());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompatibilityLevel compatibilityLevel(String subject,
                                               boolean returnTopLevelIfNotFound,
                                               CompatibilityLevel defaultForTopLevel
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
  public boolean hasSubjects(String subject, boolean lookupDeletedSubjects) {
    return hasSubjects(matchingPredicate(subject), lookupDeletedSubjects);
  }

  public boolean hasSubjects(Predicate<String> match, boolean lookupDeletedSubjects) {
    return store.entrySet().stream()
        .anyMatch(e -> {
          K k = e.getKey();
          V v = e.getValue();
          if (k instanceof SchemaKey) {
            SchemaKey key = (SchemaKey) k;
            SchemaValue value = (SchemaValue) v;
            if (value != null && (!value.isDeleted() || lookupDeletedSubjects)) {
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
    BiPredicate<String, Integer> matchDeleted = matchDeleted(match);

    Iterator<Map.Entry<Integer, Map<String, Integer>>> it =
        guidToSubjectVersions.entrySet().iterator();
    while (it.hasNext()) {
      Map<String, Integer> subjectVersions = it.next().getValue();
      subjectVersions.entrySet().removeIf(e -> matchDeleted.test(e.getKey(), e.getValue()));
      if (subjectVersions.isEmpty()) {
        it.remove();
      }
    }

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

  private BiPredicate<String, Integer> matchDeleted(Predicate<String> match) {
    return (subject, version) -> {
      if (match.test(subject)) {
        SchemaValue value = (SchemaValue) store.get(new SchemaKey(subject, version));
        // The value returned from the store should not be null since we clean up caches
        // after tombstoning, but we still check defensively
        return value == null || value.isDeleted();
      }
      return false;
    };
  }
}
