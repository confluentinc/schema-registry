/**
 * Copyright 2016 Confluent Inc.
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
package io.confluent.kafka.streams.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Map;

public class GenericAvroSerde implements Serde<GenericRecord> {

  private final Serde<GenericRecord> inner;

  /**
   * Constructor used by Kafka Streams.
   */
  public GenericAvroSerde() {
    inner = Serdes.serdeFrom(new GenericAvroSerializer(), new GenericAvroDeserializer());
  }

  public GenericAvroSerde(SchemaRegistryClient client) {
    this(client, Collections.emptyMap());
  }

  public GenericAvroSerde(SchemaRegistryClient client, Map<String, ?> props) {
    inner = Serdes.serdeFrom(new GenericAvroSerializer(client), new GenericAvroDeserializer(client, props));
  }

  @Override
  public Serializer<GenericRecord> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<GenericRecord> deserializer() {
    return inner.deserializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.serializer().configure(configs, isKey);
    inner.deserializer().configure(configs, isKey);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

}