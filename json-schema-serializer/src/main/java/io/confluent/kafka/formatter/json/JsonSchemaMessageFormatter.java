/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.formatter.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.PrintStream;

import io.confluent.kafka.formatter.SchemaMessageDeserializer;
import io.confluent.kafka.formatter.SchemaMessageFormatter;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaDeserializer;

/**
 * Example
 * To use JsonSchemaMessageFormatter, first make sure that Zookeeper, Kafka and schema registry
 * server are
 * all started. Second, make sure the jar for JsonSchemaMessageFormatter and its dependencies are
 * included
 * in the classpath of kafka-console-consumer.sh. Then run the following command.
 *
 * <p>1. To read only the value of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 * --bootstrap-server localhost:9092
 * --formatter io.confluent.kafka.formatter.JsonSchemaMessageFormatter \
 * --property schema.registry.url=http://localhost:8081
 *
 * <p>2. To read both the key and the value of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 * --bootstrap-server localhost:9092
 * --formatter io.confluent.kafka.formatter.JsonSchemaMessageFormatter \
 * --property schema.registry.url=http://localhost:8081 \
 * --property print.key=true
 *
 * <p>3. To read the key, value, and timestamp of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 * --bootstrap-server localhost:9092
 * --formatter io.confluent.kafka.formatter.JsonSchemaMessageFormatter \
 * --property schema.registry.url=http://localhost:8081 \
 * --property print.key=true \
 * --property print.timestamp=true
 */
public class JsonSchemaMessageFormatter extends SchemaMessageFormatter<JsonNode> {

  private static final ObjectMapper objectMapper = Jackson.newObjectMapper();

  /**
   * Constructor needed by kafka console consumer.
   */
  public JsonSchemaMessageFormatter() {
  }

  /**
   * For testing only.
   */
  JsonSchemaMessageFormatter(
      SchemaRegistryClient schemaRegistryClient,
      Deserializer keyDeserializer
  ) {
    super(schemaRegistryClient, keyDeserializer);
  }

  @Override
  protected SchemaMessageDeserializer<JsonNode> createDeserializer(
      SchemaRegistryClient schemaRegistryClient,
      Deserializer keyDeserializer
  ) {
    return new JsonSchemaMessageDeserializer(schemaRegistryClient, keyDeserializer);
  }


  @Override
  protected void writeTo(String topic, byte[] data, PrintStream output) throws IOException {
    JsonNode object = deserializer.deserialize(topic, data);
    output.print(objectMapper.writeValueAsString(object));
  }

  @Override
  protected SchemaProvider getProvider() {
    return new JsonSchemaProvider();
  }

  static class JsonSchemaMessageDeserializer extends AbstractKafkaJsonSchemaDeserializer<JsonNode>
      implements SchemaMessageDeserializer<JsonNode> {

    protected final Deserializer keyDeserializer;

    /**
     * For testing only.
     */
    JsonSchemaMessageDeserializer(SchemaRegistryClient schemaRegistryClient,
                                  Deserializer keyDeserializer) {
      this.schemaRegistry = schemaRegistryClient;
      this.keyDeserializer = keyDeserializer;
      this.validate = true;
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
    public JsonNode deserialize(String topic, byte[] payload) throws SerializationException {
      return (JsonNode) super.deserialize(false, topic, isKey, payload);
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
