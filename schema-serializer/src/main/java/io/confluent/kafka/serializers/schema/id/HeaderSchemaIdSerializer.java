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

import static io.confluent.kafka.serializers.schema.id.SchemaId.KEY_SCHEMA_ID_HEADER;
import static io.confluent.kafka.serializers.schema.id.SchemaId.VALUE_SCHEMA_ID_HEADER;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;

/**
 * A {@link HeaderSchemaIdSerializer} adds a schema GUID to the header.
 */
public class HeaderSchemaIdSerializer implements SchemaIdSerializer {

  @Override
  public byte[] serialize(
      String topic, boolean isKey, Headers headers, byte[] payload, SchemaId schemaId)
      throws SerializationException {
    if (headers == null) {
      throw new SerializationException("Missing headers");
    }
    String headerKey = isKey ? KEY_SCHEMA_ID_HEADER : VALUE_SCHEMA_ID_HEADER;
    headers.add(headerKey, schemaId.guidToBytes());
    return payload;
  }
}
