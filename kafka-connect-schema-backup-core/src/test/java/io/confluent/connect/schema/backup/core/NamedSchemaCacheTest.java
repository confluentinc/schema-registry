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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class NamedSchemaCacheTest {

  private static final String SCHEMA_TEXT = "schema";
  private static final String NAME_A = "a.First";
  private static final String NAME_B = "a.Second";
  private static final String SEPARATOR = "#";

  private final AtomicInteger copies = new AtomicInteger();

  private NamedSchemaCache<String> cache(boolean sameWhenEqual) {
    return new NamedSchemaCache<>(
        (x, y) -> sameWhenEqual && x.equals(y),
        (schema, name) -> {
          copies.incrementAndGet();
          return new String(schema + SEPARATOR + name);
        });
  }

  @Test
  public void testSameSchemaAndNameReusesTheCopy() {
    NamedSchemaCache<String> cache = cache(true);
    String schema = new String(SCHEMA_TEXT);

    String first = cache.named(schema, NAME_A);
    String second = cache.named(schema, NAME_A);

    assertEquals(SCHEMA_TEXT + SEPARATOR + NAME_A, first);
    assertSame(first, second);
    assertEquals(1, copies.get());
  }

  @Test
  public void testDifferentNamesGetDifferentCopies() {
    NamedSchemaCache<String> cache = cache(true);
    String schema = new String(SCHEMA_TEXT);

    assertEquals(SCHEMA_TEXT + SEPARATOR + NAME_A, cache.named(schema, NAME_A));
    assertEquals(SCHEMA_TEXT + SEPARATOR + NAME_B, cache.named(schema, NAME_B));
    assertEquals(2, copies.get());
  }

  @Test
  public void testEqualSchemaInstancesShareTheCopy() {
    NamedSchemaCache<String> cache = cache(true);

    String first = cache.named(new String(SCHEMA_TEXT), NAME_A);
    String second = cache.named(new String(SCHEMA_TEXT), NAME_A);

    assertSame(first, second);
    assertEquals(1, copies.get());
  }

  @Test
  public void testSchemasNotTheSameAreKeptApart() {
    NamedSchemaCache<String> cache = cache(false);

    String first = cache.named(new String(SCHEMA_TEXT), NAME_A);
    String second = cache.named(new String(SCHEMA_TEXT), NAME_A);

    assertNotSame(first, second);
    assertEquals(2, copies.get());
  }

  @Test
  public void testSameInstanceIsReusedEvenWhenNotEqualByPredicate() {
    NamedSchemaCache<String> cache = cache(false);
    String schema = new String(SCHEMA_TEXT);

    assertSame(cache.named(schema, NAME_A), cache.named(schema, NAME_A));
    assertEquals(1, copies.get());
  }

  @Test(expected = NullPointerException.class)
  public void testNullSchemaIsRejected() {
    cache(true).named(null, NAME_A);
  }

  @Test(expected = NullPointerException.class)
  public void testNullNameIsRejected() {
    cache(true).named(SCHEMA_TEXT, null);
  }
}
