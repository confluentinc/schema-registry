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

package io.confluent.kafka.serializers;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

import org.apache.avro.Schema;

public class KafkaAvroDecoder extends AbstractKafkaAvroDeserializer implements Decoder<Object> {

  public KafkaAvroDecoder(SchemaRegistryClient schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  public KafkaAvroDecoder(SchemaRegistryClient schemaRegistry, VerifiableProperties props) {
    this.schemaRegistry = schemaRegistry;
    configure(deserializerConfig(props));
  }

  /**
   * Constructor used by Kafka consumer.
   */
  public KafkaAvroDecoder(VerifiableProperties props) {
    configure(new KafkaAvroDeserializerConfig(props.props()));
  }

  @Override
  public Object fromBytes(byte[] bytes) {
    return deserialize(bytes);
  }

  /**
   * Pass a reader schema to get an Avro projection
   */
  public Object fromBytes(byte[] bytes, Schema readerSchema) {
    return deserialize(bytes, readerSchema);
  }
}
