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
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.util.function.Function;

/**
 * Chooses the schemas a record is read with, given the schema it was written with.
 *
 * <p>Unlike a plain writer-to-reader function, it is told which subject and schema id the writer
 * came from, so a caller can look up anything keyed by them, such as provenance.
 */
@FunctionalInterface
public interface ReaderSchemaResolver {

  /**
   * The schemas to read a record written under {@code writer} with, or null to read it as the
   * deserializer would without a resolver.
   */
  ReaderSchemas resolve(String subject, SchemaId writerId, ParsedSchema writer);

  /**

   * A resolver answering with {@code writerToReaderSchemaFunc}; null for a null function.

   */
  static ReaderSchemaResolver of(Function<ParsedSchema, ParsedSchema> writerToReaderSchemaFunc) {
    if (writerToReaderSchemaFunc == null) {
      return null;
    }
    return (subject, writerId, writer) ->
        ReaderSchemas.of(writerToReaderSchemaFunc.apply(writer));
  }
}
