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

import io.confluent.common.Configurable;
import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;

/**
 * @param <T> Type to be serialized from. <p/> A class that implements this interface is expected to
 *            have a constructor with no parameter.
 */
public interface Serializer<T> extends Configurable {

  /**
   * @param data Typed data
   * @return bytes of the serialized data
   */
  public byte[] toBytes(T data) throws SerializationException;

  /**
   * @param data Bytes
   * @return Typed deserialized data
   */
  public T fromBytes(byte[] data) throws SerializationException;

  /**
   * Close this serializer
   */
  public void close();
}
