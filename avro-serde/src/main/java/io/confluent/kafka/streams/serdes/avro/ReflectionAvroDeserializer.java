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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * A schema-registry aware deserializer for reading data in "reflection Avro" format.
 *
 * <p>This deserializer assumes that the serialized data was written in the wire format defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format. It
 * requires access to a Confluent Schema Registry endpoint, which you must {@link
 * ReflectionAvroDeserializer#configure(Map, boolean)} via the parameter "schema.registry.url".</p>
 *
 * <p>See {@link ReflectionAvroSerializer} for its serializer counterpart.</p>
 */
@InterfaceStability.Unstable
public class ReflectionAvroDeserializer<T> implements Deserializer<T> {

  private final KafkaAvroDeserializer inner;
  private final Schema schema;

  public ReflectionAvroDeserializer() {
    this.schema = null;
    this.inner = new KafkaAvroDeserializer();
  }

  public ReflectionAvroDeserializer(Class<T> type) {
    this.schema = ReflectData.get().getSchema(type);
    this.inner = new KafkaAvroDeserializer();
  }

  /**
   * For testing purposes only.
   */
  ReflectionAvroDeserializer(final SchemaRegistryClient client) {
    this.schema = null;
    this.inner = new KafkaAvroDeserializer(client);
  }

  /**
   * For testing purposes only.
   */
  ReflectionAvroDeserializer(final SchemaRegistryClient client, Class<T> type) {
    this.schema = ReflectData.get().getSchema(type);
    this.inner = new KafkaAvroDeserializer(client);
  }

  @Override
  public void configure(final Map<String, ?> deserializerConfig,
      final boolean isDeserializerForRecordKeys) {
    inner.configure(
        ConfigurationUtils.withReflectionAvroEnabled(deserializerConfig),
        isDeserializerForRecordKeys);
  }

  @Override
  public T deserialize(final String topic, final byte[] bytes) {
    return deserialize(topic, null, bytes);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T deserialize(final String topic, final Headers headers, final byte[] bytes) {
    return (T) inner.deserialize(topic, headers, bytes, schema);
  }

  @Override
  public void close() {
    inner.close();
  }

}
