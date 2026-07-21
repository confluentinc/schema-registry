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

package io.confluent.kafka.serializers.schema.id;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;

/**
 * A {@link SchemaIdDeserializer} is used by a deserializer to determine how to deserialize
 * schema identifiers. It is invoked before the payload has been converted from bytes.
 */
public interface SchemaIdDeserializer extends Configurable, Closeable {
  default void configure(Map<String, ?> configs) {
  }

  /**
   * Deserialize the payload and set the schema identifier.
   * @param topic topic associated with the payload
   * @param isKey whether this is a record key or record value
   * @param headers headers associated with the record; may be empty.
   * @param payload serialized payload that may include a schema identifier
   * @param schemaId schema identifier to be modified; either the ID or the GUID should be set
   * @return the serialized payload that does not include a schema identifier
   */
  ByteBuffer deserialize(
      String topic, boolean isKey, Headers headers, byte[] payload, SchemaId schemaId)
      throws SerializationException;

  default void close() throws IOException {
  }
}
