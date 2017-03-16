/**
 * Copyright 2015 Confluent Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafka.serializers;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

/**
 * Decode JSON data to an Object.
 */
public class KafkaJsonDecoder<T> extends KafkaJsonDeserializer<T> implements Decoder<T> {

  /**
   * This constructor is needed by Kafka. When used directly or through reflection,
   * the type will default to {@link Object} since there is no way to infer a generic type.
   * In particular, it is an error to do the following:
   *
   * <pre>{@code
   *   // WARNING: DON'T DO THIS
   *   KafkaJsonDecoder<String> decoder = new KafkaJsonDecoder<String>(props);
   *   }
   * </pre>
   *
   * <p>Instead you must use {@link KafkaJsonDecoder#KafkaJsonDecoder(VerifiableProperties, Class)}
   * with the expected type.
   *
   * @param props The decoder configuration
   */

  @SuppressWarnings({"rawtypes", "unchecked"})
  public KafkaJsonDecoder(VerifiableProperties props) {
    configure(new KafkaJsonDecoderConfig(props.props()), (Class<T>) Object.class);
  }

  /**
   * Create a new decoder using the expected type to decode to
   *
   * @param props The decoder configuration
   * @param type  The type of objects constructed
   */
  public KafkaJsonDecoder(VerifiableProperties props, Class<T> type) {
    configure(new KafkaJsonDecoderConfig(props.props()), type);
  }

  @Override
  public T fromBytes(byte[] bytes) {
    return super.deserialize(null, bytes);
  }
}
