/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.schema.backup.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.junit.Test;

public class BackupCachesTest {

  private static final int SMALL_LIMIT = 10;
  private static final int OVERFILL = 5;
  private static final String VALUE = "value";
  private static final String OTHER_VALUE = "other";

  @Test
  public void testDefaultLimitMatchesConverterSchemaCacheDefault() {
    assertEquals(1000, BackupCaches.MAX_ENTRIES);
  }

  @Test
  public void testMapNeverExceedsLimit() {
    Map<Integer, String> map = BackupCaches.newBoundedMap(SMALL_LIMIT);
    for (int i = 0; i < SMALL_LIMIT * OVERFILL; i++) {
      map.put(i, VALUE + i);
      assertTrue("size " + map.size() + " after " + (i + 1) + " puts",
          map.size() <= SMALL_LIMIT);
    }
  }

  @Test
  public void testDefaultMapNeverExceedsDefaultLimit() {
    Map<Integer, String> map = BackupCaches.newBoundedMap();
    for (int i = 0; i < BackupCaches.MAX_ENTRIES * 2; i++) {
      map.put(i, VALUE);
    }
    assertTrue(map.size() <= BackupCaches.MAX_ENTRIES);
  }

  @Test
  public void testLatestEntryIsKeptAfterEviction() {
    Map<Integer, String> map = BackupCaches.newBoundedMap(SMALL_LIMIT);
    int last = SMALL_LIMIT * OVERFILL;
    for (int i = 0; i <= last; i++) {
      map.put(i, VALUE + i);
    }
    assertEquals(VALUE + last, map.get(last));
  }

  @Test
  public void testComputeIfAbsentComputesOnceAndReturnsCachedValue() {
    Map<String, String> map = BackupCaches.newBoundedMap();
    String first = map.computeIfAbsent(VALUE, k -> new String(OTHER_VALUE));
    String second = map.computeIfAbsent(VALUE, k -> {
      throw new AssertionError("must not recompute a cached entry");
    });
    assertSame(first, second);
  }

  @Test
  public void testMissingKeyReturnsNull() {
    Map<String, String> map = BackupCaches.newBoundedMap();
    assertNull(map.get(VALUE));
  }
}
