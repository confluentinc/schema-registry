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
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;

import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
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

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;


/**
 * In-memory store based on maps
 */
public class InMemoryCache<K, V> implements LookupCache<K, V> {
  private final ConcurrentNavigableMap<K, V> store;
  private final Map<String, Map<String, Map<Integer, Map<String, Integer>>>> guidToSubjectVersions;
  private final Map<String, Map<String, Map<MD5, Integer>>> hashToGuid;
  private final Map<String, Map<String, Map<SchemaKey, Set<Integer>>>> referencedBy;

  public InMemoryCache(Serializer<K, V> serializer) {
    this.store = new ConcurrentSkipListMap<>(new SubjectKeyComparator<>(this));
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
    String ctx = QualifiedSubject.contextFor(tenant(), schema.getSubject());
    List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> refs
        = schema.getReferences();
    MD5 md5 = MD5.ofString(schema.getSchema(), refs == null ? null : refs.stream()
        .map(ref -> new SchemaReference(ref.getName(), ref.getSubject(), ref.getVersion()))
        .collect(Collectors.toList()));
    Map<String, Map<MD5, Integer>> ctxHashes =
        hashToGuid.getOrDefault(tenant(), Collections.emptyMap());
    Map<MD5, Integer> hashes = ctxHashes.getOrDefault(ctx, Collections.emptyMap());
    Integer id = hashes.get(md5);
    if (id == null) {
      return null;
    }
    Map<String, Map<Integer, Map<String, Integer>>> ctxGuids =
        guidToSubjectVersions.getOrDefault(tenant(), Collections.emptyMap());
    Map<Integer, Map<String, Integer>> guids = ctxGuids.getOrDefault(ctx, Collections.emptyMap());
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
    String ctx = QualifiedSubject.contextFor(tenant(), schema.getSubject());
    Map<String, Map<SchemaKey, Set<Integer>>> ctxRefBy =
        referencedBy.getOrDefault(tenant(), Collections.emptyMap());
    Map<SchemaKey, Set<Integer>> refBy = ctxRefBy.getOrDefault(ctx, Collections.emptyMap());
    return refBy.getOrDefault(schema, Collections.emptySet());
  }

  @Override
  public SchemaKey schemaKeyById(Integer id, String subject) throws StoreException {
    QualifiedSubject qs = QualifiedSubject.create(tenant(), subject);
    String ctx = qs != null ? qs.getContext() : DEFAULT_CONTEXT;
    Map<String, Map<Integer, Map<String, Integer>>> ctxGuids =
        guidToSubjectVersions.getOrDefault(tenant(), Collections.emptyMap());
    Map<Integer, Map<String, Integer>> guids = ctxGuids.getOrDefault(ctx, Collections.emptyMap());
    Map<String, Integer> subjectVersions = guids.get(id);
    if (subjectVersions == null || subjectVersions.isEmpty()) {
      return null;
    }
    if (qs == null || qs.getSubject().isEmpty()) {
      Map.Entry<String, Integer> entry = subjectVersions.entrySet().iterator().next();
      return new SchemaKey(entry.getKey(), entry.getValue());
    } else {
      Integer version = subjectVersions.get(subject);
      return version != null ? new SchemaKey(subject, version) : null;
    }
  }

  @Override
  public void schemaDeleted(
      SchemaKey schemaKey, SchemaValue schemaValue, SchemaValue oldSchemaValue) {
    String ctx = QualifiedSubject.contextFor(tenant(), schemaKey.getSubject());
    Map<String, Map<Integer, Map<String, Integer>>> ctxGuids =
        guidToSubjectVersions.computeIfAbsent(tenant(), k -> new ConcurrentHashMap<>());
    Map<Integer, Map<String, Integer>> guids =
        ctxGuids.computeIfAbsent(ctx, k -> new ConcurrentHashMap<>());
    Map<String, Integer> subjectVersions =
        guids.computeIfAbsent(schemaValue.getId(), k -> new ConcurrentHashMap<>());
    subjectVersions.put(schemaKey.getSubject(), schemaKey.getVersion());
    // We ensure the schema is registered by its hash; this is necessary in case of a
    // compaction when the previous non-deleted schemaValue will not get registered
    addToSchemaHashToGuid(schemaKey, schemaValue);
    for (SchemaReference ref : schemaValue.getReferences()) {
      SchemaKey refKey = new SchemaKey(ref.getSubject(), ref.getVersion());
      Map<String, Map<SchemaKey, Set<Integer>>> ctxRefBy =
          referencedBy.getOrDefault(tenant(), Collections.emptyMap());
      Map<SchemaKey, Set<Integer>> refBy = ctxRefBy.getOrDefault(ctx, Collections.emptyMap());
      if (refBy != null) {
        Set<Integer> ids = refBy.get(refKey);
        if (ids != null) {
          ids.remove(schemaValue.getId());
          if (ids.isEmpty()) {
            refBy.remove(refKey);
          }
        }
      }
    }
  }

  @Override
  public void schemaTombstoned(SchemaKey schemaKey, SchemaValue schemaValue) {
    if (schemaValue == null) {
      return;
    }
    String ctx = QualifiedSubject.contextFor(tenant(), schemaKey.getSubject());
    Map<String, Map<Integer, Map<String, Integer>>> ctxGuids =
        guidToSubjectVersions.getOrDefault(tenant(), Collections.emptyMap());
    Map<Integer, Map<String, Integer>> guids = ctxGuids.getOrDefault(ctx, Collections.emptyMap());
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
  public void schemaRegistered(
      SchemaKey schemaKey, SchemaValue schemaValue, SchemaValue oldSchemaValue) {
    String ctx = QualifiedSubject.contextFor(tenant(), schemaKey.getSubject());
    Map<String, Map<Integer, Map<String, Integer>>> ctxGuids =
        guidToSubjectVersions.computeIfAbsent(tenant(), k -> new ConcurrentHashMap<>());
    Map<Integer, Map<String, Integer>> guids =
        ctxGuids.computeIfAbsent(ctx, k -> new ConcurrentHashMap<>());
    Map<String, Integer> subjectVersions =
        guids.computeIfAbsent(schemaValue.getId(), k -> new ConcurrentHashMap<>());
    subjectVersions.put(schemaKey.getSubject(), schemaKey.getVersion());
    addToSchemaHashToGuid(schemaKey, schemaValue);
    for (SchemaReference ref : schemaValue.getReferences()) {
      SchemaKey refKey = new SchemaKey(ref.getSubject(), ref.getVersion());
      Map<String, Map<SchemaKey, Set<Integer>>> ctxRefBy =
          referencedBy.computeIfAbsent(tenant(), k -> new ConcurrentHashMap<>());
      Map<SchemaKey, Set<Integer>> refBy =
          ctxRefBy.computeIfAbsent(ctx, k -> new ConcurrentHashMap<>());
      Set<Integer> ids = refBy.computeIfAbsent(
              refKey, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
      ids.add(schemaValue.getId());
    }
  }

  private void addToSchemaHashToGuid(SchemaKey schemaKey, SchemaValue schemaValue) {
    String ctx = QualifiedSubject.contextFor(tenant(), schemaKey.getSubject());
    MD5 md5 = MD5.ofString(schemaValue.getSchema(), schemaValue.getReferences());
    Map<String, Map<MD5, Integer>> ctxHashes =
        hashToGuid.computeIfAbsent(tenant(), k -> new ConcurrentHashMap<>());
    Map<MD5, Integer> hashes = ctxHashes.computeIfAbsent(ctx, k -> new ConcurrentHashMap<>());
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
      QualifiedSubject qs = QualifiedSubject.create(tenant(), subject);
      if (qs != null && !DEFAULT_CONTEXT.equals(qs.getContext())) {
        config = (ConfigValue) get((K) new ConfigKey(qs.toQualifiedContext()));
      } else {
        config = (ConfigValue) get((K) new ConfigKey(null));
      }
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
      QualifiedSubject qs = QualifiedSubject.create(tenant(), subject);
      if (qs != null && !DEFAULT_CONTEXT.equals(qs.getContext())) {
        modeValue = (ModeValue) get((K) new ModeKey(qs.toQualifiedContext()));
      } else {
        modeValue = (ModeValue) get((K) new ModeKey(null));
      }
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
        .collect(Collectors.toCollection(LinkedHashSet::new));
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
  public Map<String, Integer> clearSubjects(String subject) throws StoreException {
    return clearSubjects(subject, matchingSubjectPredicate(subject));
  }

  private Map<String, Integer> clearSubjects(String subject, Predicate<String> match) {
    String ctx = QualifiedSubject.contextFor(tenant(), subject);
    BiPredicate<String, Integer> matchDeleted = matchDeleted(match);

    Map<String, Map<Integer, Map<String, Integer>>> ctxGuids =
        guidToSubjectVersions.getOrDefault(tenant(), Collections.emptyMap());
    Map<Integer, Map<String, Integer>> guids = ctxGuids.getOrDefault(ctx, Collections.emptyMap());
    Iterator<Map.Entry<Integer, Map<String, Integer>>> it = guids.entrySet().iterator();
    while (it.hasNext()) {
      Map<String, Integer> subjectVersions = it.next().getValue();
      subjectVersions.entrySet().removeIf(e -> matchDeleted.test(e.getKey(), e.getValue()));
      if (subjectVersions.isEmpty()) {
        it.remove();
      }
    }

    // Delete from store later as the previous deletions rely on the store
    Map<String, Integer> counts = new HashMap<>();
    store.entrySet().removeIf(e -> {
      if (e.getKey() instanceof SchemaKey) {
        SchemaKey key = (SchemaKey) e.getKey();
        SchemaValue value = (SchemaValue) e.getValue();
        boolean isMatch = match.test(key.getSubject()) && value.isDeleted();
        if (isMatch) {
          String schemaType = value.getSchemaType();
          if (schemaType == null) {
            schemaType = AvroSchema.TYPE;
          }
          counts.merge(schemaType, 1, Integer::sum);
        }
        return isMatch;
      }
      return false;
    });
    return counts;
  }

  protected Predicate<String> matchingSubjectPredicate(String subject) {
    QualifiedSubject qs = QualifiedSubject.create(tenant(), subject);
    return s -> qs == null
        || subject.equals(s)
        // check context match for a qualified subject with an empty subject
        || (qs.getSubject().isEmpty() && qs.toQualifiedContext().equals(
            QualifiedSubject.qualifiedContextFor(tenant(), s)));
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
