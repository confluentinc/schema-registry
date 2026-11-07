/*
 * Copyright 2026 Confluent Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.junit.Test;

public class InMemoryCacheTest {

  private static final String SUBJECT = "s";
  private static final String SCHEMA = "\"string\"";

  @Test
  public void anIdUnderTwoVersionsResolvesToTheHigherWhateverTheWriteOrder() throws Exception {
    // v2 soft-deleted and kept, v3 its re-registration under the same ID; the older write last.
    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache = cache();
    put(cache, 3, false);
    put(cache, 2, true);

    assertEquals(new SchemaKey(SUBJECT, 3), cache.schemaKeyById(7, SUBJECT));
    assertEquals(3, cache.schemaIdAndSubjects(schema()).getVersion(SUBJECT));
  }

  @Test
  public void tombstoningOneVersionLeavesTheOtherToResolveTheId() throws Exception {
    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache = cache();
    put(cache, 2, true);
    put(cache, 3, false);

    tombstone(cache, 3);
    assertEquals(new SchemaKey(SUBJECT, 2), cache.schemaKeyById(7, SUBJECT));
    assertEquals(2, cache.schemaIdAndSubjects(schema()).getVersion(SUBJECT));

    tombstone(cache, 2);
    assertNull(cache.schemaKeyById(7, SUBJECT));
    assertNull(cache.schemaIdAndSubjects(schema()));
  }

  @Test
  public void anIdListsEveryVersionCarryingIt() throws Exception {
    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache = cache();
    put(cache, 2, true);
    put(cache, 3, false);

    assertEquals(Collections.singletonMap(SUBJECT, new TreeSet<>(Arrays.asList(2, 3))),
        cache.subjectVersionsById(7, SUBJECT));
    assertEquals(Collections.emptyMap(), cache.subjectVersionsById(8, SUBJECT));
  }

  @Test
  public void tombstoningAnUnindexedVersionIsANoOp() throws Exception {
    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache = cache();

    // Nothing indexed for the tenant or context at all; the old value is non-null, as when
    // the reader sees a tombstone for a record whose registration it never indexed
    cache.schemaTombstoned(
        new SchemaKey(SUBJECT, 2), new SchemaValue(SUBJECT, 2, 7, SCHEMA, false));

    // Context indexed, but not this id
    put(cache, 3, false);
    SchemaKey key = new SchemaKey(SUBJECT, 4);
    cache.schemaTombstoned(key, new SchemaValue(SUBJECT, 4, 8, SCHEMA, false));

    assertEquals(new SchemaKey(SUBJECT, 3), cache.schemaKeyById(7, SUBJECT));
  }

  @Test
  public void hasSubjectsAgreesWithAScanOfEverySubject() throws Exception {
    // subjects() still scans the whole store with the predicate hasSubjects used to
    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache = cache();
    put(cache, "s", 1, false);
    put(cache, "s", 2, true);
    put(cache, "sa", 1, false);
    put(cache, "r", 1, true);
    put(cache, ":.ctx:s", 1, false);
    put(cache, ":.ctx:t", 1, true);
    put(cache, "x*", 1, false);

    // ":.:s" names "s" too, but was never matched to it
    for (String query : new String[] {"s", "sa", "s2", "r", "t", ":.ctx:s", ":.ctx:t",
        ":.ctx:u", ":.ctx:", ":.other:", ":.ctx:sa", ":.:s", "x*", "s*", "*"}) {
      for (boolean deleted : new boolean[] {false, true}) {
        assertEquals(query + ", deleted " + deleted,
            !cache.subjects(query, deleted).isEmpty(), cache.hasSubjects(query, deleted));
      }
    }
    assertTrue(cache.hasSubjects("s", false));
    assertFalse(cache.hasSubjects("r", false));
    assertTrue(cache.hasSubjects("r", true));
    assertFalse(cache.hasSubjects("s2", true));
  }

  @Test
  public void hasSubjectsUsesAnOverriddenPredicateForAWildcard() throws Exception {
    // As a multi-tenant subclass does, matching a name ending in "_*" as a prefix
    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache =
        new InMemoryCache<SchemaRegistryKey, SchemaRegistryValue>(new SchemaRegistrySerializer()) {
          @Override
          protected Predicate<String> matchingSubjectPredicate(String subject) {
            return subject.endsWith("_*")
                ? s -> s.startsWith(subject.substring(0, subject.length() - 1))
                : super.matchingSubjectPredicate(subject);
          }
        };
    cache.init();
    put(cache, "t_a", 1, false);
    assertTrue(cache.hasSubjects("t_*", false));
    assertFalse(cache.hasSubjects("u_*", false));
  }

  @Test
  public void anImportOverwriteUnderAnotherIdLeavesTheOldIdItsOtherVersions() throws Exception {
    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache = cache();
    put(cache, 1, false);
    put(cache, 2, false);

    // v2 imported again under ID 9: ID 7 is left with v1 alone, then with nothing.
    overwrite(cache, 2, 9);
    assertEquals(new SchemaKey(SUBJECT, 1), cache.schemaKeyById(7, SUBJECT));
    assertEquals(new SchemaKey(SUBJECT, 2), cache.schemaKeyById(9, SUBJECT));
    overwrite(cache, 1, 9);
    assertNull(cache.schemaKeyById(7, SUBJECT));
  }

  @Test
  public void aSoftDeletedOverwriteUnderAnotherIdLeavesTheOldIdItsOtherVersions()
      throws Exception {
    // A lagging reader whose overwrite record was compacted away sees it as a soft delete.
    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache = cache();
    put(cache, 1, false);
    put(cache, 2, false);

    SchemaKey key = new SchemaKey(SUBJECT, 2);
    SchemaValue value = new SchemaValue(SUBJECT, 2, 9, SCHEMA, true);
    SchemaValue old = (SchemaValue) cache.put(key, value);
    cache.schemaDeleted(key, value, old);

    assertEquals(new SchemaKey(SUBJECT, 1), cache.schemaKeyById(7, SUBJECT));
    assertEquals(new SchemaKey(SUBJECT, 2), cache.schemaKeyById(9, SUBJECT));
  }

  private static InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache() throws Exception {
    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache =
        new InMemoryCache<>(new SchemaRegistrySerializer());
    cache.init();
    return cache;
  }

  // As the store handler does: the value goes in the store, then into the indexes.
  private static void put(InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache,
      int version, boolean deleted) throws Exception {
    put(cache, SUBJECT, version, deleted);
  }

  private static void put(InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache,
      String subject, int version, boolean deleted) throws Exception {
    SchemaKey key = new SchemaKey(subject, version);
    SchemaValue value = new SchemaValue(subject, version, 7, SCHEMA, deleted);
    cache.put(key, value);
    if (deleted) {
      cache.schemaDeleted(key, value, null);
    } else {
      cache.schemaRegistered(key, value, null);
    }
  }

  // As an IMPORT overwrite: the version's record replaced by one under another ID.
  private static void overwrite(InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache,
      int version, int id) throws Exception {
    SchemaKey key = new SchemaKey(SUBJECT, version);
    SchemaValue value = new SchemaValue(SUBJECT, version, id, SCHEMA, false);
    SchemaValue old = (SchemaValue) cache.put(key, value);
    cache.schemaRegistered(key, value, old);
  }

  private static void tombstone(InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache,
      int version) throws Exception {
    SchemaKey key = new SchemaKey(SUBJECT, version);
    SchemaValue old = (SchemaValue) cache.delete(key);
    cache.schemaTombstoned(key, old);
  }

  private static Schema schema() {
    return new Schema(SUBJECT, 0, -1, null, null, SCHEMA);
  }
}
