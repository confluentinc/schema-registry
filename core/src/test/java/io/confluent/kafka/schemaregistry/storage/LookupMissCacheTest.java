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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public class LookupMissCacheTest {

  private static final String SCHEMA = "{\"type\":\"string\"}";

  private final AtomicLong nanos = new AtomicLong();
  private final LookupMissCache cache = new LookupMissCache(100, 60, nanos::get);

  @Test
  public void testKeyCoversEveryPartOfTheLookup() {
    LookupMissCache.Key base = key("a", "s", schema(s -> { }), false, false);
    assertEquals(base, key("a", "s", schema(s -> { }), false, false));
    assertEquals(base.hashCode(), key("a", "s", schema(s -> { }), false, false).hashCode());

    assertNotEquals(base, key("b", "s", schema(s -> { }), false, false));
    assertNotEquals(base, key("a", "t", schema(s -> { }), false, false));
    assertNotEquals(base, key("a", "s", schema(s -> { }), true, false));
    assertNotEquals(base, key("a", "s", schema(s -> { }), false, true));
    assertNotEquals(base, key("a", "s", schema(s -> s.setSchemaType("JSON")), false, false));
    assertNotEquals(base, key("a", "s", schema(s -> s.setVersion(2)), false, false));
    assertNotEquals(base, key("a", "s", schema(s -> s.setId(5)), false, false));
    assertNotEquals(base, key("a", "s", schema(s -> s.setSchema("\"int\"")), false, false));
    assertNotEquals(base, key("a", "s", schema(s -> s.setReferences(
        List.of(new SchemaReference("r", "r-value", 1)))), false, false));
    assertNotEquals(base, key("a", "s", schema(s -> s.setMetadata(
        new Metadata(null, Collections.singletonMap("k", "v"), null))), false, false));
    assertNotEquals(base, key("a", "s", schema(s -> s.setRuleSet(
        new RuleSet(null, List.of(rule("doc"))))), false, false));
  }

  @Test
  public void testKeySeparatesValuesThatConcatenateAlike() {
    // Schema.updateHash would digest these alike: no separators, and empty strings left out
    assertNotEquals(
        key("a", "s", schema(s -> s.setMetadata(
            new Metadata(null, Collections.singletonMap("ab", "c"), null))), false, false),
        key("a", "s", schema(s -> s.setMetadata(
            new Metadata(null, Collections.singletonMap("a", "bc"), null))), false, false));
    assertNotEquals(
        key("a", "s", schema(s -> s.setRuleSet(new RuleSet(null, List.of(rule(""))))),
            false, false),
        key("a", "s", schema(s -> s.setRuleSet(new RuleSet(null, List.of(rule(null))))),
            false, false));
  }

  @Test
  public void testMissServedUntilAWriteOfItsTenant() {
    LookupMissCache.Key key = key("a", "s", schema(s -> { }), false, false);
    cache.putMiss(key, cache.tag("a"));
    assertTrue(cache.isMiss(key, cache.tag("a")));

    // "a" and "b" fall on different counters
    cache.writeApplied("b");
    assertTrue(cache.isMiss(key, cache.tag("a")));

    cache.writeApplied("a");
    assertFalse(cache.isMiss(key, cache.tag("a")));
  }

  @Test
  public void testWriteWithoutTenantDropsEveryMiss() {
    LookupMissCache.Key key = key("a", "s", schema(s -> { }), false, false);
    cache.putMiss(key, cache.tag("a"));
    cache.writeApplied(null);
    assertFalse(cache.isMiss(key, cache.tag("a")));
  }

  @Test
  public void testMissOfALookupOverlappingAWriteIsNotServed() {
    LookupMissCache.Key key = key("a", "s", schema(s -> { }), false, false);
    LookupMissCache.Tag before = cache.tag("a");
    // The write is applied while the lookup runs, which then stores what it found
    cache.writeApplied("a");
    cache.putMiss(key, before);
    assertFalse(cache.isMiss(key, cache.tag("a")));
  }

  @Test
  public void testInvalidatorDropsMissEvenIfAHandlerThrows() {
    LookupMissCache.Key key = key("a", "s", schema(s -> { }), false, false);
    cache.putMiss(key, cache.tag("a"));
    SchemaUpdateHandler throwing = (k, v, old, tp, offset, ts) -> {
      throw new IllegalStateException("handler failed");
    };
    LookupMissInvalidator invalidator = new LookupMissInvalidator(throwing, cache, () -> "a");

    assertThrows(IllegalStateException.class, () -> invalidator.handleUpdate(
        new SchemaKey("s", 1), null, null, new TopicPartition("_schemas", 0), 0, 0));
    assertFalse(cache.isMiss(key, cache.tag("a")));
  }

  @Test
  public void testInvalidatorUsesTheTenantItIsCalledUnder() {
    LookupMissCache.Key key = key("a", "s", schema(s -> { }), false, false);
    cache.putMiss(key, cache.tag("a"));
    // A handler that leaves another tenant on the thread ("a" and "b" use different counters)
    AtomicReference<String> threadTenant = new AtomicReference<>("a");
    SchemaUpdateHandler switching = (k, v, old, tp, offset, ts) -> threadTenant.set("b");
    new LookupMissInvalidator(switching, cache, threadTenant::get).handleUpdate(
        new SchemaKey("s", 1), null, null, new TopicPartition("_schemas", 0), 0, 0);
    assertFalse(cache.isMiss(key, cache.tag("a")));
  }

  @Test
  public void testMissExpires() {
    LookupMissCache.Key key = key("a", "s", schema(s -> { }), false, false);
    cache.putMiss(key, cache.tag("a"));
    nanos.addAndGet(TimeUnit.SECONDS.toNanos(59));
    assertTrue(cache.isMiss(key, cache.tag("a")));
    nanos.addAndGet(TimeUnit.SECONDS.toNanos(2));
    assertFalse(cache.isMiss(key, cache.tag("a")));
  }

  private static LookupMissCache.Key key(String tenant, String subject, Schema schema,
                                         boolean normalize, boolean deleted) {
    return LookupMissCache.key(tenant, subject, schema, normalize, deleted);
  }

  private static Schema schema(Consumer<Schema> change) {
    Schema schema = new Schema("s", 0, -1, "AVRO", Collections.emptyList(), SCHEMA);
    change.accept(schema);
    return schema;
  }

  private static Rule rule(String doc) {
    return new Rule("r", doc, RuleKind.CONDITION, RuleMode.WRITE, "CEL", null, null,
        "true", null, null, false);
  }
}
