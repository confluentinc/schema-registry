/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage.serialization;

import java.io.Closeable;
import java.io.Serializable;
import org.apache.kafka.common.Configurable;

import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;

/**
 * @param <K> Key type to be serialized from. <p/> A class that implements this interface is
 *            expected to have a constructor with no parameter.
 */
public interface Serializer<K, V> extends Configurable, Serializable, Closeable {

  /**
   * @param key Typed key
   * @return bytes of the serialized key
   */
  public byte[] serializeKey(K key) throws SerializationException;

  /**
   * @param value Typed value
   * @return bytes of the serialized value
   */
  public byte[] serializeValue(V value) throws SerializationException;

  /**
   * @param key Bytes of the serialized key
   * @return Typed deserialized key
   */
  public K deserializeKey(byte[] key) throws SerializationException;

  /**
   * @param key   Typed key corresponding to this value
   * @param value Bytes of the serialized value
   * @return Typed deserialized value
   */
  public V deserializeValue(K key, byte[] value) throws SerializationException;

  /**
   * Close this serializer
   */
  public void close();
}
