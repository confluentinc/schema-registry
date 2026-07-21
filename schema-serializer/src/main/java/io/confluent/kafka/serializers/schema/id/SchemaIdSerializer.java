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
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;

/**
 * A {@link SchemaIdSerializer} is used by a serializer to determine how to serialize schema
 * identifiers. It is invoked after the payload has been converted to bytes.
 */
public interface SchemaIdSerializer extends Configurable, Closeable {

  default void configure(Map<String, ?> configs) {
  }

  /**
   * Associates a schema identifier with the payload.
   * @param topic topic associated with the payload
   * @param isKey whether this is a record key or record value
   * @param headers headers associated with the record that can be mutated; may be empty.
   * @param payload serialized payload that does not include a schema identifier
   * @param schemaId schema identifier to be used; contains either an ID or GUID
   * @return the serialized payload that may include a schema identifier
   */
  byte[] serialize(String topic, boolean isKey, Headers headers, byte[] payload, SchemaId schemaId)
      throws SerializationException;

  default void close() throws IOException {
  }
}
