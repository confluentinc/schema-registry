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

import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;

/**
 * A {@link PrefixSchemaIdDeserializer} looks for a schema ID in the payload prefix.
 */
public class PrefixSchemaIdDeserializer implements SchemaIdDeserializer {

  @Override
  public ByteBuffer deserialize(
      String topic, boolean isKey, Headers headers, byte[] payload, SchemaId schemaId)
      throws SerializationException {
    return schemaId.fromBytes(ByteBuffer.wrap(payload));
  }
}
