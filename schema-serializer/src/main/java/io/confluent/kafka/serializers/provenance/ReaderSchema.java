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
 * A reader schema, with the registered version it stands for when the caller knows it: by
 * subject version, or by schema id. A caller that changes its reader before handing it over —
 * Flink merging a writer's rules onto a pinned version — names that version, so provenance never
 * has to find the reader by its structure. A version is the exact form: one schema id may sit
 * under several versions, and then stands for the latest of them.
 */
public final class ReaderSchema {

  private final ParsedSchema schema;
  private final Integer id;
  private final String subject;
  private final Integer version;

  private ReaderSchema(ParsedSchema schema, Integer registeredId, String subject,
      Integer version) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.id = registeredId;
    this.subject = subject;
    this.version = version;
  }

  /**
   * A reader whose registered version, if any, is to be found by the registry.
   */
  public static ReaderSchema of(ParsedSchema schema) {
    return new ReaderSchema(schema, null, null, null);
  }

  /**
   * A reader standing for the registered version with schema id {@code registeredId}: the latest
   * version carrying it.
   */
  public static ReaderSchema of(ParsedSchema schema, int registeredId) {
    return new ReaderSchema(schema, registeredId, null, null);
  }

  /**
   * A reader standing for version {@code version} of {@code subject}, soft-deleted or not. A
   * record whose subject is another fails, rather than be read against another subject's version.
   */
  public static ReaderSchema of(ParsedSchema schema, String subject, int version) {
    Objects.requireNonNull(subject, "subject");
    if (version < 1) {
      throw new IllegalArgumentException("A pinned version is a version number, not " + version);
    }
    return new ReaderSchema(schema, null, subject, version);
  }

  public ParsedSchema getSchema() {
    return schema;
  }

  /**
   * The schema id of the version this reader stands for, or null if the caller did not say.
   */
  public Integer getId() {
    return id;
  }

  /**
   * The subject of the version this reader is pinned to, or null if it is not pinned to one.
   */
  public String getSubject() {
    return subject;
  }

  /**
   * The version this reader is pinned to, or null if it is not pinned to one.
   */
  public Integer getVersion() {
    return version;
  }
}
