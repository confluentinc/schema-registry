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

package io.confluent.kafka.formatter;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.io.PrintStream;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Example
 * To use AvroMessageFormatter, first make sure that Zookeeper, Kafka and schema registry server are
 * all started. Second, make sure the jar for AvroMessageFormatter and its dependencies are included
 * in the classpath of kafka-console-consumer.sh. Then run the following command.
 *
 * <p>1. To read only the value of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 *   --bootstrap-server localhost:9092
 *   --formatter io.confluent.kafka.formatter.AvroMessageFormatter \
 *   --property schema.registry.url=http://localhost:8081
 *
 * <p>2. To read both the key and the value of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 *   --bootstrap-server localhost:9092
 *   --formatter io.confluent.kafka.formatter.AvroMessageFormatter \
 *   --property schema.registry.url=http://localhost:8081 \
 *   --property print.key=true
 *
 * <p>3. To read the key, value, and timestamp of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 *   --bootstrap-server localhost:9092
 *   --formatter io.confluent.kafka.formatter.AvroMessageFormatter \
 *   --property schema.registry.url=http://localhost:8081 \
 *   --property print.key=true \
 *   --property print.timestamp=true
 *
 */
public class AvroMessageFormatter extends SchemaMessageFormatter<Object> {

  private final EncoderFactory encoderFactory = EncoderFactory.get();

  /**
   * Constructor needed by kafka console consumer.
   */
  public AvroMessageFormatter() {
  }

  /**
   * For testing only.
   */
  AvroMessageFormatter(
      SchemaRegistryClient schemaRegistryClient,
      Deserializer keyDeserializer
  ) {
    super(schemaRegistryClient, keyDeserializer);
  }

  @Override
  protected SchemaMessageDeserializer<Object> createDeserializer(
      SchemaRegistryClient schemaRegistryClient,
      Deserializer keyDeserializer
  ) {
    return new AvroMessageDeserializer(schemaRegistryClient, keyDeserializer);
  }

  @Override
  protected void writeTo(String topic, byte[] data, PrintStream output) throws IOException {
    Object object = deserializer.deserialize(topic, data);
    try {
      AvroSchemaUtils.toJson(object, output);
    } catch (AvroRuntimeException e) {
      Schema schema = AvroSchemaUtils.getSchema(object);
      throw new SerializationException(
          String.format("Error serializing Avro data of schema %s to json", schema), e);
    }
  }

  @Override
  protected SchemaProvider getProvider() {
    return new AvroSchemaProvider();
  }

  static class AvroMessageDeserializer extends AbstractKafkaAvroDeserializer
      implements SchemaMessageDeserializer<Object> {

    protected final Deserializer keyDeserializer;

    /**
     * For testing only.
     */
    AvroMessageDeserializer(SchemaRegistryClient schemaRegistryClient,
                            Deserializer keyDeserializer) {
      this.schemaRegistry = schemaRegistryClient;
      this.keyDeserializer = keyDeserializer;
    }

    @Override
    public Deserializer getKeyDeserializer() {
      return keyDeserializer;
    }

    @Override
    public Object deserializeKey(String topic, byte[] payload) {
      return keyDeserializer.deserialize(topic, payload);
    }

    @Override
    public Object deserialize(String topic, byte[] payload) throws SerializationException {
      return super.deserialize(topic, isKey, payload, null);
    }

    @Override
    public void close() throws IOException {
      if (keyDeserializer != null) {
        keyDeserializer.close();
      }
      super.close();
    }
  }
}
