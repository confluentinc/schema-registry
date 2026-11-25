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

package io.confluent.kafka.schemaregistry.type.logical.statements;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * The types the names in a type may be of. A type referred to has names of its own, which
 * mean nothing beside another type's: they are followed among its own.
 */
final class KnownTypes {

  private final Map<String, Schema> types = new HashMap<>();
  // The names of a type referred to, by its root and by each type a name of it is of.
  private final Map<Schema, KnownTypes> own = new IdentityHashMap<>();

  static KnownTypes none() {
    return new KnownTypes();
  }

  static KnownTypes of(Map<String, Schema> types) {
    KnownTypes known = new KnownTypes();
    known.types.putAll(types);
    return known;
  }

  /** The type a name is of, or null where it is not known. */
  Schema get(String name) {
    return types.get(name);
  }

  /** The names to follow those in a type among: its own if it is referred to, else these. */
  KnownTypes within(Schema type) {
    return own.getOrDefault(type, this);
  }

  /** Whether a type is the root of a type referred to. */
  boolean isReferred(Schema type) {
    return own.containsKey(type);
  }

  void put(String name, Schema type) {
    types.put(name, type);
  }

  /** A type referred to, under a name not yet of another, with the names it has. */
  void putReferred(String name, Schema type, KnownTypes names) {
    types.putIfAbsent(name, type);
    own.put(type, names);
  }

  /** Takes the types referred to: under a name these have only where it is one to replace. */
  void adopt(KnownTypes referred, Predicate<String> replaces) {
    referred.types.forEach((name, type) -> {
      if (replaces.test(name) || !types.containsKey(name)) {
        types.put(name, type);
      }
    });
    own.putAll(referred.own);
  }
}
