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

import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

/**
 * Size-bounded cache of schema copies named after a record's type, keyed by schema and name
 * (the same approach as the Protobuf deserializer's copy cache).
 *
 * @param <S> the schema type
 */
public final class NamedSchemaCache<S> {

  private final Map<Key<S>, S> cache = BackupCaches.newBoundedMap();
  private final BiPredicate<S, S> sameSchema;
  private final BiFunction<S, String, S> copy;

  /**
   * Creates a cache.
   *
   * @param sameSchema whether two schemas are interchangeable, for keys that are not the same
   *     instance
   * @param copy creates the copy of a schema with the given name
   */
  public NamedSchemaCache(BiPredicate<S, S> sameSchema, BiFunction<S, String, S> copy) {
    this.sameSchema = Objects.requireNonNull(sameSchema, "sameSchema");
    this.copy = Objects.requireNonNull(copy, "copy");
  }

  /**
   * Returns the copy of {@code schema} named {@code name}, reusing an earlier copy of the
   * same schema and name.
   */
  public S named(S schema, String name) {
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(name, "name");
    return cache.computeIfAbsent(new Key<>(schema, name, sameSchema),
        k -> copy.apply(schema, name));
  }

  private static final class Key<S> {
    private final S schema;
    private final String name;
    private final BiPredicate<S, S> sameSchema;

    private Key(S schema, String name, BiPredicate<S, S> sameSchema) {
      this.schema = schema;
      this.name = name;
      this.sameSchema = sameSchema;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      Key<S> other = (Key<S>) o;
      return name.equals(other.name)
          && (schema == other.schema || sameSchema.test(schema, other.schema));
    }

    @Override
    public int hashCode() {
      return 31 * schema.hashCode() + name.hashCode();
    }
  }
}
