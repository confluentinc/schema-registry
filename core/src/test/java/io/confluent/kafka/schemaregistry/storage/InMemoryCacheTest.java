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
import static org.junit.Assert.assertNull;

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

  private static InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache() throws Exception {
    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache =
        new InMemoryCache<>(new SchemaRegistrySerializer());
    cache.init();
    return cache;
  }

  // As the store handler does: the value goes in the store, then into the indexes.
  private static void put(InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> cache,
      int version, boolean deleted) throws Exception {
    SchemaKey key = new SchemaKey(SUBJECT, version);
    SchemaValue value = new SchemaValue(SUBJECT, version, 7, SCHEMA, deleted);
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
