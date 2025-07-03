/*
 * Copyright 2014-2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import java.util.Map;

import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;

public class StringSerializer implements Serializer<String, String> {

  public static StringSerializer INSTANCE = new StringSerializer();

  // only a singleton is needed
  private StringSerializer() {
  }

  /**
   * @param key Typed key
   * @return bytes of the serialized key
   */
  @Override
  public byte[] serializeKey(String key) throws SerializationException {
    return key != null ? key.getBytes() : null;
  }

  /**
   * @param value Typed value
   * @return bytes of the serialized value
   */
  @Override
  public byte[] serializeValue(String value) throws SerializationException {
    return value != null ? value.getBytes() : null;
  }

  @Override
  public String deserializeKey(byte[] key) {
    return new String(key);
  }

  /**
   * @param key   Typed key corresponding to this value
   * @param value Bytes of the serialized value
   * @return Typed deserialized value
   */
  @Override
  public String deserializeValue(String key, byte[] value) throws SerializationException {
    return new String(value);
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public void configure(Map<String, ?> stringMap) {
    // do nothing
  }
}
