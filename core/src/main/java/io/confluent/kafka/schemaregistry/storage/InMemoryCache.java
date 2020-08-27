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

import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import java.util.Collections;
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
import java.util.stream.Stream;


/**
 * In-memory store based on maps
 */
public class InMemoryCache<K, V> implements LookupCache<K, V> {
  private final ConcurrentNavigableMap<K, V> store;
  private final Map<String, Map<Integer, Map<String, Integer>>> guidToSubjectVersions;
  private final Map<String, Map<MD5, Integer>> hashToGuid;
  private final Map<SchemaKey, Set<Integer>> referencedBy;

  public InMemoryCache(Serializer<K, V> serializer) {
    this.store = new ConcurrentSkipListMap<>();
    this.guidToSubjectVersions = new ConcurrentHashMap<>();
    this.hashToGuid = new ConcurrentHashMap<>();
    this.referencedBy = new ConcurrentHashMap<>();
  }

  @Override
  public void init() throws StoreInitializationException {
    // do nothing
  }

  @Override
  public V get(K key) throws StoreException {
    return store.get(key);
  }

  @Override
  public V put(K key, V value) throws StoreException {
    return store.put(key, value);
  }

  @Override
  public CloseableIterator<V> getAll(K key1, K key2) throws StoreException {
    ConcurrentNavigableMap<K, V> subMap = (key1 == null && key2 == null)
                                          ? store
                                          : store.subMap(key1, key2);
    return new DelegatingIterator<>(subMap.values().iterator());
  }

  @Override
  public void putAll(Map<K, V> entries) throws StoreException {
    store.putAll(entries);
  }

  @Override
  public V delete(K key) throws StoreException {
    return store.remove(key);
  }

  @Override
  public CloseableIterator<K> getAllKeys() throws StoreException {
    return new DelegatingIterator<>(store.keySet().iterator());
  }

  @Override
  public void flush() throws StoreException {
  }

  @Override
  public void close() throws StoreException {
    store.clear();
  }

  @Override
  public SchemaIdAndSubjects schemaIdAndSubjects(Schema schema) throws StoreException {
    List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> refs
        = schema.getReferences();
    MD5 md5 = MD5.ofString(schema.getSchema(), refs == null ? null : refs.stream()
        .map(ref -> new SchemaReference(ref.getName(), ref.getSubject(), ref.getVersion()))
        .collect(Collectors.toList()));
    Map<MD5, Integer> hashes = hashToGuid.getOrDefault(tenant(), Collections.emptyMap());
    Integer id = hashes.get(md5);
    if (id == null) {
      return null;
    }
    Map<Integer, Map<String, Integer>> guids =
            guidToSubjectVersions.getOrDefault(tenant(), Collections.emptyMap());
    Map<String, Integer> subjectVersions = guids.get(id);
    if (subjectVersions == null || subjectVersions.isEmpty()) {
      return null;
    }
    return new SchemaIdAndSubjects(id, subjectVersions);
  }

  @Override
  public boolean containsSchema(Schema schema) throws StoreException {
    return schemaIdAndSubjects(schema) != null;
  }

  @Override
  public Set<Integer> referencesSchema(SchemaKey schema) throws StoreException {
    return referencedBy.getOrDefault(schema, Collections.newSetFromMap(new ConcurrentHashMap<>()));
  }

  @Override
  public SchemaKey schemaKeyById(Integer id) throws StoreException {
    Map<Integer, Map<String, Integer>> guids =
            guidToSubjectVersions.getOrDefault(tenant(), Collections.emptyMap());
    Map<String, Integer> subjectVersions = guids.get(id);
    if (subjectVersions == null || subjectVersions.isEmpty()) {
      return null;
    }
    Map.Entry<String, Integer> entry = subjectVersions.entrySet().iterator().next();
    return new SchemaKey(entry.getKey(), entry.getValue());
  }

  @Override
  public void schemaDeleted(SchemaKey schemaKey, SchemaValue schemaValue) {
    Map<Integer, Map<String, Integer>> guids =
            guidToSubjectVersions.computeIfAbsent(tenant(), k -> new ConcurrentHashMap<>());
    Map<String, Integer> subjectVersions =
        guids.computeIfAbsent(schemaValue.getId(), k -> new ConcurrentHashMap<>());
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
    Map<Integer, Map<String, Integer>> guids =
            guidToSubjectVersions.getOrDefault(tenant(), Collections.emptyMap());
    Map<String, Integer> subjectVersions = guids.get(schemaValue.getId());
    if (subjectVersions == null || subjectVersions.isEmpty()) {
      return;
    }
    subjectVersions.computeIfPresent(schemaKey.getSubject(),
        (k, v) -> schemaKey.getVersion() == v ? null : v);
    if (subjectVersions.isEmpty()) {
      guids.remove(schemaValue.getId());
    }
  }

  @Override
  public void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue) {
    Map<Integer, Map<String, Integer>> guids =
            guidToSubjectVersions.computeIfAbsent(tenant(), k -> new ConcurrentHashMap<>());
    Map<String, Integer> subjectVersions =
        guids.computeIfAbsent(schemaValue.getId(), k -> new ConcurrentHashMap<>());
    subjectVersions.put(schemaKey.getSubject(), schemaKey.getVersion());
    addToSchemaHashToGuid(schemaKey, schemaValue);
    for (SchemaReference ref : schemaValue.getReferences()) {
      SchemaKey refKey = new SchemaKey(ref.getSubject(), ref.getVersion());
      Set<Integer> refBy = referencedBy.computeIfAbsent(
              refKey, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
      refBy.add(schemaValue.getId());
    }
  }

  private void addToSchemaHashToGuid(SchemaKey schemaKey, SchemaValue schemaValue) {
    MD5 md5 = MD5.ofString(schemaValue.getSchema(), schemaValue.getReferences());
    Map<MD5, Integer> hashes = hashToGuid.computeIfAbsent(tenant(), k -> new ConcurrentHashMap<>());
    hashes.put(md5, schemaValue.getId());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompatibilityLevel compatibilityLevel(String subject,
                                               boolean returnTopLevelIfNotFound,
                                               CompatibilityLevel defaultForTopLevel
  ) throws StoreException {
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
  ) throws StoreException {
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
  public Set<String> subjects(String subject, boolean lookupDeletedSubjects) throws StoreException {
    return subjects(matchingSubjectPredicate(subject), lookupDeletedSubjects);
  }

  private Set<String> subjects(Predicate<String> match, boolean lookupDeletedSubjects) {
    return store.entrySet().stream()
        .flatMap(e -> {
          K k = e.getKey();
          V v = e.getValue();
          if (k instanceof SchemaKey) {
            SchemaKey key = (SchemaKey) k;
            SchemaValue value = (SchemaValue) v;
            if (value != null && (!value.isDeleted() || lookupDeletedSubjects)) {
              return match.test(key.getSubject()) ? Stream.of(key.getSubject()) : Stream.empty();
            }
          }
          return Stream.empty();
        })
        .collect(Collectors.toSet());
  }

  @Override
  public boolean hasSubjects(String subject, boolean lookupDeletedSubjects) throws StoreException {
    return hasSubjects(matchingSubjectPredicate(subject), lookupDeletedSubjects);
  }

  private boolean hasSubjects(Predicate<String> match, boolean lookupDeletedSubjects) {
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
  public void clearSubjects(String subject) throws StoreException {
    clearSubjects(matchingSubjectPredicate(subject));
  }

  private void clearSubjects(Predicate<String> match) {
    BiPredicate<String, Integer> matchDeleted = matchDeleted(match);

    Map<Integer, Map<String, Integer>> guids =
            guidToSubjectVersions.getOrDefault(tenant(), Collections.emptyMap());
    Iterator<Map.Entry<Integer, Map<String, Integer>>> it = guids.entrySet().iterator();
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

  protected Predicate<String> matchingSubjectPredicate(String subject) {
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

  static class DelegatingIterator<T> implements CloseableIterator<T> {

    private Iterator<T> iterator;

    public DelegatingIterator(Iterator<T> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public T next() {
      return iterator.next();
    }

    @Override
    public void remove() {
      iterator.remove();
    }

    @Override
    public void close() {
    }
  }
}
