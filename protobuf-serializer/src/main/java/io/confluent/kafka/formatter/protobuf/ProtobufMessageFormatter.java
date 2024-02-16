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

package io.confluent.kafka.formatter.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import io.confluent.kafka.formatter.SchemaMessageDeserializer;
import io.confluent.kafka.formatter.SchemaMessageFormatter;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufDeserializer;

/**
 * Example
 * To use ProtobufMessageFormatter, first make sure that Zookeeper, Kafka and schema registry
 * server are
 * all started. Second, make sure the jar for ProtobufMessageFormatter and its dependencies are
 * included
 * in the classpath of kafka-console-consumer.sh. Then run the following command.
 *
 * <p>1. To read only the value of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 * --bootstrap-server localhost:9092
 * --formatter io.confluent.kafka.formatter.ProtobufMessageFormatter \
 * --property schema.registry.url=http://localhost:8081
 *
 * <p>2. To read both the key and the value of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 * --bootstrap-server localhost:9092
 * --formatter io.confluent.kafka.formatter.ProtobufMessageFormatter \
 * --property schema.registry.url=http://localhost:8081 \
 * --property print.key=true
 *
 * <p>3. To read the key, value, and timestamp of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 * --bootstrap-server localhost:9092
 * --formatter io.confluent.kafka.formatter.ProtobufMessageFormatter \
 * --property schema.registry.url=http://localhost:8081 \
 * --property print.key=true \
 * --property print.timestamp=true
 */
public class ProtobufMessageFormatter extends SchemaMessageFormatter<Message> {

  private boolean preserveFieldNames = false;

  /**
   * Constructor needed by kafka console consumer.
   */
  public ProtobufMessageFormatter() {
  }

  /**
   * For testing only.
   */
  ProtobufMessageFormatter(String url, Deserializer keyDeserializer) {
    super(url, keyDeserializer);
  }

  @Override
  protected SchemaMessageDeserializer<Message> createDeserializer(Deserializer keyDeserializer) {
    return new ProtobufMessageDeserializer(keyDeserializer);
  }

  @Override
  public void init(Properties props) {
    super.init(props);

    if (props.containsKey("preserve.json.field.name")) {
      preserveFieldNames = props.getProperty("preserve.json.field.name")
              .trim().toLowerCase().equals("true");
    }
  }

  @Override
  protected void writeTo(String topic, Boolean isKey, Headers headers,
      byte[] data, PrintStream output) throws IOException {
    Message object = deserializer.deserialize(topic, isKey, headers, data);
    try {
      JsonFormat.Printer printer = JsonFormat.printer()
              .includingDefaultValueFields()
              .omittingInsignificantWhitespace();
      output.print(object == null ? null :
              this.preserveFieldNames ? printer.preservingProtoFieldNames().print(object)
                      : printer.print(object));
    } catch (InvalidProtocolBufferException e) {
      throw new SerializationException("Error serializing Protobuf data to json", e);
    }
  }

  @Override
  protected SchemaProvider getProvider() {
    return new ProtobufSchemaProvider();
  }

  static class ProtobufMessageDeserializer extends AbstractKafkaProtobufDeserializer
      implements SchemaMessageDeserializer<Message> {

    protected final Deserializer keyDeserializer;

    /**
     * For testing only.
     */
    ProtobufMessageDeserializer(Deserializer keyDeserializer) {
      this.keyDeserializer = keyDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      configure(deserializerConfig(configs), null);
    }

    @Override
    public Deserializer getKeyDeserializer() {
      return keyDeserializer;
    }

    @Override
    public Object deserializeKey(String topic, Headers headers, byte[] payload) {
      return keyDeserializer.deserialize(topic, headers, payload);
    }

    @Override
    public Message deserialize(String topic, Boolean isKey, Headers headers, byte[] payload)
        throws SerializationException {
      return (Message) super.deserialize(false, topic, isKey, headers, payload);
    }

    @Override
    public SchemaRegistryClient getSchemaRegistryClient() {
      return schemaRegistry;
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
