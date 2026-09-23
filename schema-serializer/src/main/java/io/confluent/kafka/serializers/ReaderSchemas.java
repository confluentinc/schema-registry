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

package io.confluent.kafka.serializers;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import java.util.Objects;

/**

 * What a {@link ReaderSchemaResolver} answers: the schemas a record is read with.

 */
public final class ReaderSchemas {

  private final ParsedSchema reader;
  private final ParsedSchema resolveWriterAs;

  private ReaderSchemas(ParsedSchema reader, ParsedSchema resolveWriterAs) {
    this.reader = reader;
    this.resolveWriterAs = resolveWriterAs;
  }

  /**

   * Read into {@code reader}; null reads as the deserializer would without a resolver.

   */
  public static ReaderSchemas of(ParsedSchema reader) {
    return new ReaderSchemas(reader, null);
  }

  /**
   * Read into {@code reader}, decoding as if written under {@code resolveWriterAs}.
   *
   * <p>Avro only: {@code resolveWriterAs} must have the true writer's binary layout, and may differ
   * from it only in names, which is how a caller supplies its own field pairing to the resolver.
   * Protobuf and JSON Schema decode with the reader alone and reject it.
   */
  public static ReaderSchemas of(ParsedSchema reader, ParsedSchema resolveWriterAs) {
    if (resolveWriterAs != null && reader == null) {
      throw new IllegalArgumentException("A writer to resolve as needs a reader to resolve to");
    }
    return new ReaderSchemas(reader, resolveWriterAs);
  }

  /**

   * The schema the value is read into, and that domain rules run against.

   */
  public ParsedSchema getReader() {
    return reader;
  }

  /**

   * The writer schema the resolver decodes with in place of the true writer, or null.

   */
  public ParsedSchema getResolveWriterAs() {
    return resolveWriterAs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReaderSchemas that = (ReaderSchemas) o;
    return Objects.equals(reader, that.reader)
        && Objects.equals(resolveWriterAs, that.resolveWriterAs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(reader, resolveWriterAs);
  }

  @Override
  public String toString() {
    return "ReaderSchemas{reader=" + reader + ", resolveWriterAs=" + resolveWriterAs + "}";
  }
}
