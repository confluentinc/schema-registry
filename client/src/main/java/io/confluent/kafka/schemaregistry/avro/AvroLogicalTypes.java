/*
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
 */

package io.confluent.kafka.schemaregistry.avro;

import io.confluent.avro.type.LogicalMapFactory;
import io.confluent.avro.type.VariantLogicalTypeFactory;
import org.apache.avro.LogicalTypes;

/**
 * Registers Confluent's Avro logical types. Loaded before any schema is parsed, since a schema
 * parsed before its logical type is registered does not carry it.
 */
final class AvroLogicalTypes {

  static {
    registerIfAbsent(new LogicalMapFactory());
    registerIfAbsent(new VariantLogicalTypeFactory());
  }

  private AvroLogicalTypes() {
  }

  /**
   * Does nothing itself; calling it loads this class, which registers the logical types.
   */
  static void ensureRegistered() {
  }

  // Avro's LogicalTypes registry is a single JVM-global map keyed by name, so an
  // unconditional register() would clobber another library's factory for the same name
  // (e.g. Apache Iceberg also registers "map" and "variant"). Iceberg's own registration is
  // unconditional, so leaving ours conditional lets whichever library claimed the name first
  // keep owning it, while still registering when no other library has.
  private static void registerIfAbsent(LogicalTypes.LogicalTypeFactory factory) {
    if (!LogicalTypes.getCustomRegisteredTypes().containsKey(factory.getTypeName())) {
      LogicalTypes.register(factory);
    }
  }
}
