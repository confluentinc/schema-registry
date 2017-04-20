/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.kafka.streams.serdes;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

@InterfaceStability.Unstable
public class SpecificAvroSerializer<T extends org.apache.avro.specific.SpecificRecord>
    implements Serializer<T> {

  private final KafkaAvroSerializer inner;

  public SpecificAvroSerializer() {
    inner = new KafkaAvroSerializer();
  }

  /**
   * For testing purposes only.
   */
  SpecificAvroSerializer(final SchemaRegistryClient client) {
    inner = new KafkaAvroSerializer(client);
  }

  @Override
  public void configure(final Map<String, ?> serializerConfig,
                        final boolean isSerializerForRecordKeys) {
    inner.configure(
        ConfigurationUtils.withSpecificAvroEnabled(serializerConfig),
        isSerializerForRecordKeys);
  }

  @Override
  public byte[] serialize(final String topic, final T record) {
    return inner.serialize(topic, record);
  }

  @Override
  public void close() {
    inner.close();
  }

}