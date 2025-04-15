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
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;

/**
 * A {@link ConfigSchemaIdDeserializer} returns a schema ID that is configured using the property
 * "use.schema.id" or "use.schema.guid".
 */
public class ConfigSchemaIdDeserializer implements SchemaIdDeserializer {

  public static final String USE_SCHEMA_ID = "use.schema.id";
  public static final String USE_SCHEMA_GUID = "use.schema.guid";

  private Integer id = null;
  private UUID guid = null;

  @Override
  public void configure(Map<String, ?> configs) {
    Object idConfig = configs.get(USE_SCHEMA_ID);
    if (idConfig != null) {
      try {
        this.id = Integer.parseInt(idConfig.toString());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Cannot parse " + idConfig);
      }
    }
    Object guidConfig = configs.get(USE_SCHEMA_GUID);
    if (guidConfig != null) {
      this.guid = UUID.fromString(guidConfig.toString());
    }
  }

  @Override
  public ByteBuffer deserialize(
      String topic, boolean isKey, Headers headers, byte[] payload, SchemaId schemaId)
      throws SerializationException {
    if (id != null) {
      schemaId.setId(id);
    }
    if (guid != null) {
      schemaId.setGuid(guid);
    }
    return ByteBuffer.wrap(payload);
  }
}
