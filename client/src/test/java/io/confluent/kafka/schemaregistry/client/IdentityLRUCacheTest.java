/**
 * Copyright 2018 Confluent Inc.
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
 **/

package io.confluent.kafka.schemaregistry.client;

import org.apache.kafka.common.cache.Cache;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class IdentityLRUCacheTest {

  private final Integer one = new Integer(1);
  private final Integer two = new Integer(2);
  private final Integer three = new Integer(3);
  private final Integer four = new Integer(4);

  @Test
  public void testPutGetIdentity() {
    Cache<Integer, String> cache = new IdentityLRUCache<>(4);

    cache.put(one, "b");
    cache.put(two, "d");
    cache.put(three, "f");
    cache.put(four, "h");

    assertEquals(4, cache.size());

    assertEquals("b", cache.get(one));
    assertEquals("d", cache.get(two));
    assertEquals("f", cache.get(three));
    assertEquals("h", cache.get(four));

    assertNull(cache.get(new Integer(1)));
    assertNull(cache.get(new Integer(2)));
    assertNull(cache.get(new Integer(3)));
    assertNull(cache.get(new Integer(4)));
  }

  @Test
  public void testRemove() {
    Cache<Integer, String> cache = new IdentityLRUCache<>(4);

    cache.put(one, "b");
    cache.put(two, "d");
    cache.put(three, "f");
    assertEquals(3, cache.size());

    // Identity different than `one`
    assertFalse(cache.remove(new Integer(1)));
    assertEquals(3, cache.size());

    assertEquals(true, cache.remove(one));
    assertEquals(2, cache.size());
    assertNull(cache.get(one));
    assertEquals("d", cache.get(two));
    assertEquals("f", cache.get(three));

    assertEquals(true, cache.remove(two));
    assertEquals(1, cache.size());
    assertNull(cache.get(two));
    assertEquals("f", cache.get(three));

    assertEquals(true, cache.remove(three));
    assertEquals(0, cache.size());
    assertNull(cache.get(three));
  }

  @Test
  public void testEviction() {
    Cache<Integer, String> cache = new IdentityLRUCache<>(2);

    cache.put(one, "b");
    cache.put(two, "d");
    assertEquals(2, cache.size());

    cache.put(three, "f");
    assertEquals(2, cache.size());
    assertNull(cache.get(one));
    assertEquals("d", cache.get(two));
    assertEquals("f", cache.get(three));

    // Validate correct access order eviction
    cache.get(two);
    cache.put(four, "h");
    assertEquals(2, cache.size());
    assertNull(cache.get(three));
    assertEquals("d", cache.get(two));
    assertEquals("h", cache.get(four));
  }

  @Test
  public void testMultipleEqualKeys() {
    Cache<Integer, String> cache = new IdentityLRUCache<>(2);

    Integer anotherOne = new Integer(1);

    cache.put(one, "value");
    cache.put(anotherOne, "other");
    assertEquals(2, cache.size());
    assertEquals("value", cache.get(one));
    assertEquals("other", cache.get(anotherOne));
  }
}
