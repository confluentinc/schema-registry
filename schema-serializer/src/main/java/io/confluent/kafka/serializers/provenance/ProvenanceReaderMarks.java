/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.serializers.provenance;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.provenance.ProvenanceProjector.Pin;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * What a deserializer knows of the reader schemas it reads with, by instance: which its caller
 * pinned to a registered version, and which it derived from an application class. These are facts
 * about instances, not results of a configuration, so a deserializer keeps one for its life and
 * hands it to every projector it builds: a reconfigure during a read never unmarks its reader.
 *
 * <p>Internal to Schema Registry's deserializers: not a supported API, and it may change in
 * any release.
 */
public final class ProvenanceReaderMarks {

  // Pinned readers, by instance, to the version the caller named: an equal reader may stand for
  // another version, or for none. Each is a copy of the caller's, made for that pin alone.
  private final Cache<ParsedSchema, Pin> pins = CacheBuilder.newBuilder().weakKeys().build();
  // Copies of a caller's pinned reader, by its instance, then by pin.
  private final Cache<ParsedSchema, Map<Pin, ParsedSchema>> copies =
      CacheBuilder.newBuilder().weakKeys().build();
  // Readers derived from an application class: matched to the latest version they equal.
  private final Cache<ParsedSchema, Boolean> derived =
      CacheBuilder.newBuilder().weakKeys().build();

  /**
   * The copy of {@code schema} kept for {@code pin}, pinned. The instance itself is never marked:
   * handed over later without a pin, or with another, it must not read as pinned to this one.
   */
  ParsedSchema pinnedCopy(ParsedSchema schema, Pin pin) {
    ParsedSchema copy;
    try {
      copy = copies.get(schema, ConcurrentHashMap::new)
          .computeIfAbsent(pin, p -> schema.copy());
    } catch (ExecutionException e) {
      throw new IllegalStateException(e.getCause());
    }
    pins.put(copy, pin);
    return copy;
  }

  /**
   * The registered version {@code reader} was pinned to, or null if none.
   */
  Pin pinOf(ParsedSchema reader) {
    return pins.getIfPresent(reader);
  }

  /**
   * Marks {@code reader} as derived from an application class.
   */
  void markDerived(ParsedSchema reader) {
    derived.put(reader, Boolean.TRUE);
  }

  /**
   * Whether {@code reader} was marked as derived from an application class.
   */
  boolean isDerived(ParsedSchema reader) {
    return derived.getIfPresent(reader) != null;
  }
}
