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

    for (String query : new String[] {"s", "sa", "s2", "r", "t", ":.ctx:s", ":.ctx:t",
        ":.ctx:u", ":.ctx:", ":.other:", ":.ctx:sa"}) {
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
