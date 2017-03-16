/**
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

import io.confluent.kafka.schemaregistry.storage.ConfigValue;
import io.confluent.kafka.schemaregistry.storage.NoopKey;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.kafka.schemaregistry.storage.ConfigKey;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryKey;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryKeyType;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryValue;
import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;

public class SchemaRegistrySerializer
    implements Serializer<SchemaRegistryKey, SchemaRegistryValue> {

  public SchemaRegistrySerializer() {

  }

  /**
   * @param key Typed key
   * @return bytes of the serialized key
   */
  @Override
  public byte[] serializeKey(SchemaRegistryKey key) throws SerializationException {
    try {
      return new ObjectMapper().writeValueAsBytes(key);
    } catch (IOException e) {
      throw new SerializationException("Error while serializing schema key" + key.toString(),
                                       e);
    }
  }

  /**
   * @param value Typed value
   * @return bytes of the serialized value
   */
  @Override
  public byte[] serializeValue(SchemaRegistryValue value) throws SerializationException {
    try {
      return new ObjectMapper().writeValueAsBytes(value);
    } catch (IOException e) {
      throw new SerializationException(
          "Error while serializing value schema value " + value.toString(),
          e);
    }
  }

  @Override
  public SchemaRegistryKey deserializeKey(byte[] key) throws SerializationException {
    SchemaRegistryKey schemaKey = null;
    SchemaRegistryKeyType keyType = null;
    try {
      try {
        Map<Object, Object> keyObj = null;
        keyObj = new ObjectMapper().readValue(key,
                                              new TypeReference<Map<Object, Object>>() {});
        keyType = SchemaRegistryKeyType.forName((String) keyObj.get("keytype"));
        if (keyType == SchemaRegistryKeyType.CONFIG) {
          schemaKey = new ObjectMapper().readValue(key, ConfigKey.class);
        } else if (keyType == SchemaRegistryKeyType.NOOP) {
          schemaKey = new ObjectMapper().readValue(key, NoopKey.class);
        } else {
          schemaKey = new ObjectMapper().readValue(key, SchemaKey.class);
        }
      } catch (JsonProcessingException e) {

        String type = "unknown";
        if (keyType == SchemaRegistryKeyType.CONFIG) {
          type = SchemaRegistryKeyType.CONFIG.name();
        } else if (keyType == SchemaRegistryKeyType.SCHEMA) {
          type = SchemaRegistryKeyType.SCHEMA.name();
        } else if (keyType == SchemaRegistryKeyType.NOOP) {
          type = SchemaRegistryKeyType.NOOP.name();
        }

        throw new SerializationException("Failed to deserialize " + type + " key", e);
      }
    } catch (IOException e) {
      throw new SerializationException("Error while deserializing schema key", e);
    }
    return schemaKey;
  }

  /**
   * @param key   Typed key corresponding to this value
   * @param value Bytes of the serialized value
   * @return Typed deserialized value. Must be one of
   *     {@link io.confluent.kafka.schemaregistry.storage.ConfigValue}
   *     or {@link io.confluent.kafka.schemaregistry.storage.SchemaValue}
   */
  @Override
  public SchemaRegistryValue deserializeValue(SchemaRegistryKey key, byte[] value)
      throws SerializationException {
    SchemaRegistryValue schemaRegistryValue = null;
    if (key.getKeyType().equals(SchemaRegistryKeyType.CONFIG)) {
      try {
        schemaRegistryValue = new ObjectMapper().readValue(value, ConfigValue.class);
      } catch (IOException e) {
        throw new SerializationException("Error while deserializing config", e);
      }
    } else if (key.getKeyType().equals(SchemaRegistryKeyType.SCHEMA)) {
      try {
        schemaRegistryValue = new ObjectMapper().readValue(value, SchemaValue.class);
      } catch (IOException e) {
        throw new SerializationException("Error while deserializing schema", e);
      }
    } else {
      throw new SerializationException("Unrecognized key type. Must be one of schema or config");
    }
    return schemaRegistryValue;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> stringMap) {

  }
}
