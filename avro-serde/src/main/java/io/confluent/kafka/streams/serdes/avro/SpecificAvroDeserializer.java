/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.streams.serdes.avro;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * A schema-registry aware deserializer for reading data in "specific Avro" format.
 *
 * <p>This deserializer assumes that the serialized data was written in the wire format defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format.
 * It requires access to a Confluent Schema Registry endpoint, which you must
 * {@link SpecificAvroDeserializer#configure(Map, boolean)} via the parameter
 * "schema.registry.url".</p>
 *
 * <p>See {@link SpecificAvroSerializer} for its serializer counterpart.</p>
 */
@InterfaceStability.Unstable
public class SpecificAvroDeserializer<T extends org.apache.avro.specific.SpecificRecord>
    implements Deserializer<T> {

  private final KafkaAvroDeserializer inner;

  public SpecificAvroDeserializer() {
    inner = new KafkaAvroDeserializer();
  }

  /**
   * For testing purposes only.
   */
  SpecificAvroDeserializer(final SchemaRegistryClient client) {
    inner = new KafkaAvroDeserializer(client);
  }

  @Override
  public void configure(final Map<String, ?> deserializerConfig,
                        final boolean isDeserializerForRecordKeys) {
    inner.configure(
        ConfigurationUtils.withSpecificAvroEnabled(deserializerConfig),
        isDeserializerForRecordKeys);
  }

  @Override
  public T deserialize(final String topic, final byte[] bytes) {
    return deserialize(topic, null, bytes);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T deserialize(final String topic, final Headers headers, final byte[] bytes) {
    return (T) inner.deserialize(topic, headers, bytes);
  }

  @Override
  public void close() {
    inner.close();
  }

}