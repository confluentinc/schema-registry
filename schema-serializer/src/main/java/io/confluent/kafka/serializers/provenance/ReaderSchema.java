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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import java.util.Objects;

/**
 * A reader schema, with the schema id of the registered version it stands for when the caller
 * knows it. A caller that changes its reader before handing it over — Flink merging a writer's
 * rules onto a pinned version — passes that version's id, so provenance never has to find the
 * reader by its structure.
 */
public final class ReaderSchema {

  private final ParsedSchema schema;
  private final Integer registeredId;

  private ReaderSchema(ParsedSchema schema, Integer registeredId) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.registeredId = registeredId;
  }

  /**
   * A reader whose registered version, if any, is to be found by the registry.
   */
  public static ReaderSchema of(ParsedSchema schema) {
    return new ReaderSchema(schema, null);
  }

  /**
   * A reader standing for the registered version with schema id {@code registeredId}.
   */
  public static ReaderSchema of(ParsedSchema schema, int registeredId) {
    return new ReaderSchema(schema, registeredId);
  }

  public ParsedSchema getSchema() {
    return schema;
  }

  /**
   * The schema id of the version this reader stands for, or null if the caller did not say.
   */
  public Integer getRegisteredId() {
    return registeredId;
  }
}
