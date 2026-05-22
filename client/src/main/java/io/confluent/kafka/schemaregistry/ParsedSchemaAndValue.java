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

package io.confluent.kafka.schemaregistry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A parsed schema with a value, produced by deserialization.
 */
public interface ParsedSchemaAndValue {

  /**
   * The reader schema used to interpret the value. When the consumer has
   * registered a reader schema that differs from the writer schema on the
   * wire, this returns the reader schema and {@link #getWriterSchema()}
   * returns the writer schema; otherwise the two may be equal.
   */
  ParsedSchema getSchema();

  /**
   * The deserialized value, projected into the reader schema. Validating this
   * against {@link #getWriterSchema()} may fail when the reader and writer
   * schemas differ structurally.
   */
  Object getValue();

  /**
   * The writer's parsed schema, as it appeared on the wire payload. May
   * differ from {@link #getSchema()} when schema projection / migration is
   * in play. Returns {@code null} when the deserializer did not resolve a
   * writer schema (unusual).
   */
  default ParsedSchema getWriterSchema() {
    return null;
  }

  /**
   * Identifying info (subject / id / version / guid) for the writer schema,
   * resolved from the wire payload. Returns {@code null} when nothing was
   * resolved. Individual fields on a populated {@code SchemaInfo} may still
   * be null (some deserializer paths know the id but not the guid, for
   * example).
   */
  default SchemaInfo getWriterSchemaInfo() {
    return null;
  }

  /**
   * Results of the rules visited during deserialization, one entry per rule
   * (including rules that were skipped because they were disabled, in the
   * wrong execution environment, or whose mode did not match the current
   * phase). Order matches rule execution order.
   *
   * <p>Returns an empty, unmodifiable list when no rules ran.
   */
  default List<RuleResult> getRuleResults() {
    return Collections.emptyList();
  }

  final class SchemaInfo {

    private final String subject;
    private final Integer id;
    private final Integer version;
    private final String guid;

    public SchemaInfo(String subject, Integer id, Integer version, String guid) {
      this.subject = subject;
      this.id = id;
      this.version = version;
      this.guid = guid;
    }

    public String subject() {
      return subject;
    }

    public Integer id() {
      return id;
    }

    public Integer version() {
      return version;
    }

    public String guid() {
      return guid;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SchemaInfo)) {
        return false;
      }
      SchemaInfo that = (SchemaInfo) o;
      return Objects.equals(subject, that.subject)
          && Objects.equals(id, that.id)
          && Objects.equals(version, that.version)
          && Objects.equals(guid, that.guid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subject, id, version, guid);
    }

    @Override
    public String toString() {
      return "SchemaInfo{subject=" + subject
          + ", id=" + id
          + ", version=" + version
          + ", guid=" + guid
          + '}';
    }
  }
}
