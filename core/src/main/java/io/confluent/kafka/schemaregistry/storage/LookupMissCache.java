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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Remembers schema lookups under a subject that found no match. A miss is tagged with its
 * tenant's write counts as they were when the lookup began, and is served only while they are
 * unchanged; every record read from the schemas topic bumps the count of its tenant.
 */
public class LookupMissCache {

  // Tenants share counters by hash; a shared counter only costs an extra miss
  private static final int STRIPES = 1024;

  // Every property is written, so that requests differing only in null against empty differ
  private static final ObjectMapper KEY_MAPPER = keyMapper();

  private final Cache<Key, Tag> misses;
  private final AtomicLongArray tenantWrites = new AtomicLongArray(STRIPES);
  private final AtomicLong globalWrites = new AtomicLong();

  public LookupMissCache(int maxSize, int expirySecs, Ticker ticker) {
    this.misses = Caffeine.newBuilder()
        .maximumSize(maxSize)
        .expireAfterWrite(expirySecs, TimeUnit.SECONDS)
        .ticker(ticker)
        .build();
  }

  /**
   * Returns the key for a lookup, or null if the request cannot be digested. Only the digest is
   * kept, so a key's size does not depend on the request.
   */
  public static Key key(String tenant, String subject, Schema schema,
                        boolean normalize, boolean lookupDeletedSchema) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      try (OutputStream out = new DigestOutputStream(OutputStream.nullOutputStream(), md)) {
        KEY_MAPPER.writeValue(out, Arrays.asList(
            tenant, subject, normalize, lookupDeletedSchema, schema.getSubject(),
            schema.getSchemaType(), schema.getVersion(), schema.getId(),
            schema.getReferences(), schema.getMetadata(), schema.getRuleSet(),
            schema.getSchema()));
      }
      return new Key(md.digest());
    } catch (IOException | NoSuchAlgorithmException e) {
      return null;
    }
  }

  /**
   * Returns the write counts of the tenant, to be taken before a lookup begins.
   */
  public Tag tag(String tenant) {
    return new Tag(tenantWrites.get(stripe(tenant)), globalWrites.get());
  }

  /**
   * Returns whether the lookup is remembered as a miss under the given current tag.
   */
  public boolean isMiss(Key key, Tag current) {
    return current.equals(misses.getIfPresent(key));
  }

  /**
   * Remembers a miss, tagged with the counts taken before the lookup began.
   */
  public void putMiss(Key key, Tag tag) {
    misses.put(key, tag);
  }

  /**
   * Invalidates the misses of a tenant, or of every tenant if it is null.
   */
  public void writeApplied(String tenant) {
    if (tenant == null) {
      globalWrites.incrementAndGet();
    } else {
      tenantWrites.incrementAndGet(stripe(tenant));
    }
  }

  // visible for testing
  long size() {
    misses.cleanUp();
    return misses.estimatedSize();
  }

  private static int stripe(String tenant) {
    return tenant != null ? Math.floorMod(tenant.hashCode(), STRIPES) : 0;
  }

  private static ObjectMapper keyMapper() {
    ObjectMapper mapper = JacksonMapper.newObjectMapper();
    JsonInclude.Value always =
        JsonInclude.Value.construct(JsonInclude.Include.ALWAYS, JsonInclude.Include.ALWAYS);
    mapper.setDefaultPropertyInclusion(always);
    for (Class<?> type : List.of(
        SchemaReference.class, Metadata.class, RuleSet.class, Rule.class)) {
      mapper.configOverride(type).setInclude(always);
    }
    return mapper;
  }

  /**
   * A lookup, as a digest of its tenant, subject, flags and requested schema.
   */
  public static final class Key {
    private final byte[] digest;

    Key(byte[] digest) {
      this.digest = digest;
    }

    @Override
    public boolean equals(Object o) {
      return this == o || (o instanceof Key && Arrays.equals(digest, ((Key) o).digest));
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(digest);
    }
  }

  /**
   * The write counts of a tenant and of all tenants.
   */
  public static final class Tag {
    private final long tenantWrites;
    private final long globalWrites;

    Tag(long tenantWrites, long globalWrites) {
      this.tenantWrites = tenantWrites;
      this.globalWrites = globalWrites;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Tag)) {
        return false;
      }
      Tag that = (Tag) o;
      return tenantWrites == that.tenantWrites && globalWrites == that.globalWrites;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tenantWrites, globalWrites);
    }
  }
}
