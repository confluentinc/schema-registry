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

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * A schema-registry aware serializer for writing data in "generic Avro" format.
 *
 * <p>This serializer writes data in the wire format defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format.
 * It requires access to a Confluent Schema Registry endpoint, which you must
 * {@link GenericAvroSerializer#configure(Map, boolean)} via the parameter
 * "schema.registry.url".</p>
 *
 * <p>See {@link GenericAvroDeserializer} for its deserializer counterpart.</p>
 */
@InterfaceStability.Unstable
public class GenericAvroSerializer implements Serializer<GenericRecord> {

  private final KafkaAvroSerializer inner;

  public GenericAvroSerializer() {
    inner = new KafkaAvroSerializer();
  }

  /**
   * For testing purposes only.
   */
  GenericAvroSerializer(final SchemaRegistryClient client) {
    inner = new KafkaAvroSerializer(client);
  }

  @Override
  public void configure(final Map<String, ?> serializerConfig,
                        final boolean isSerializerForRecordKeys) {
    inner.configure(serializerConfig, isSerializerForRecordKeys);
  }

  @Override
  public byte[] serialize(final String topic, final GenericRecord record) {
    return serialize(topic, null, record);
  }

  @Override
  public byte[] serialize(final String topic, final Headers headers, final GenericRecord record) {
    return inner.serialize(topic, headers, record);
  }

  @Override
  public void close() {
    inner.close();
  }

}