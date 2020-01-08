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
import kafka.common.MessageFormatter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
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
 * --zookeeper localhost:2181 --formatter io.confluent.kafka.formatter.ProtobufMessageFormatter \
 * --property schema.registry.url=http://localhost:8081
 *
 * <p>2. To read both the key and the value of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 * --zookeeper localhost:2181 --formatter io.confluent.kafka.formatter.ProtobufMessageFormatter \
 * --property schema.registry.url=http://localhost:8081 \
 * --property print.key=true
 *
 * <p>3. To read the key, value, and timestamp of the messages in JSON
 * bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
 * --zookeeper localhost:2181 --formatter io.confluent.kafka.formatter.ProtobufMessageFormatter \
 * --property schema.registry.url=http://localhost:8081 \
 * --property print.key=true \
 * --property print.timestamp=true
 */
public class ProtobufMessageFormatter extends AbstractKafkaProtobufDeserializer
    implements MessageFormatter {

  private static final byte[] NULL_BYTES = "null".getBytes(StandardCharsets.UTF_8);
  private boolean printKey = false;
  private boolean printTimestamp = false;
  private boolean printIds = false;
  private boolean printKeyId = false;
  private boolean printValueId = false;
  private byte[] keySeparator = "\t".getBytes(StandardCharsets.UTF_8);
  private byte[] lineSeparator = "\n".getBytes(StandardCharsets.UTF_8);
  private byte[] idSeparator = "\t".getBytes(StandardCharsets.UTF_8);
  private Deserializer keyDeserializer;

  /**
   * Constructor needed by kafka console consumer.
   */
  public ProtobufMessageFormatter() {
  }

  /**
   * For testing only.
   */
  ProtobufMessageFormatter(
      SchemaRegistryClient schemaRegistryClient, Deserializer keyDeserializer
  ) {
    this.schemaRegistry = schemaRegistryClient;
    this.keyDeserializer = keyDeserializer;
  }

  @Override
  public void init(Properties props) {
    if (props == null) {
      throw new ConfigException("Missing schema registry url!");
    }
    String url = props.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    if (url == null) {
      throw new ConfigException("Missing schema registry url!");
    }

    Map<String, Object> originals = getPropertiesMap(props);
    schemaRegistry = createSchemaRegistry(url, originals);

    if (props.containsKey("print.timestamp")) {
      printTimestamp = props.getProperty("print.timestamp").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("print.key")) {
      printKey = props.getProperty("print.key").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("key.separator")) {
      keySeparator = props.getProperty("key.separator").getBytes(StandardCharsets.UTF_8);
    }
    if (props.containsKey("line.separator")) {
      lineSeparator = props.getProperty("line.separator").getBytes(StandardCharsets.UTF_8);
    }
    if (props.containsKey("key.deserializer")) {
      try {
        keyDeserializer = (Deserializer) Class.forName((String) props.get("key.deserializer"))
            .newInstance();
      } catch (Exception e) {
        throw new ConfigException("Error initializing Key deserializer", e);
      }
    }
    if (props.containsKey("print.schema.ids")) {
      printIds = props.getProperty("print.schema.ids").trim().toLowerCase().equals("true");
      if (printIds) {
        printValueId = true;
        if (keyDeserializer == null
            || keyDeserializer instanceof AbstractKafkaProtobufDeserializer) {
          printKeyId = true;
        }
      }
    }
    if (props.containsKey("schema.id.separator")) {
      idSeparator = props.getProperty("schema.id.separator").getBytes(StandardCharsets.UTF_8);
    }
  }

  private Map<String, Object> getPropertiesMap(Properties props) {
    Map<String, Object> originals = new HashMap<>();
    for (final String name : props.stringPropertyNames()) {
      originals.put(name, props.getProperty(name));
    }
    return originals;
  }

  @Override
  public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
    if (printTimestamp) {
      try {
        TimestampType timestampType = consumerRecord.timestampType();
        if (timestampType != TimestampType.NO_TIMESTAMP_TYPE) {
          output.write(String.format("%s:%d", timestampType, consumerRecord.timestamp())
              .getBytes(StandardCharsets.UTF_8));
        } else {
          output.write("NO_TIMESTAMP".getBytes(StandardCharsets.UTF_8));
        }
        output.write(keySeparator);
      } catch (IOException ioe) {
        throw new SerializationException("Error while formatting the timestamp", ioe);
      }
    }
    if (printKey) {
      try {
        if (keyDeserializer != null) {
          Object deserializedKey = consumerRecord.key() == null
                                   ? null
                                   : keyDeserializer.deserialize(null, consumerRecord.key());
          output.write(deserializedKey != null ? deserializedKey.toString()
              .getBytes(StandardCharsets.UTF_8) : NULL_BYTES);
        } else {
          writeTo(consumerRecord.key(), output);
        }
        if (printKeyId) {
          output.write(idSeparator);
          int schemaId = schemaIdFor(consumerRecord.key());
          output.print(schemaId);
        }
        output.write(keySeparator);
      } catch (IOException ioe) {
        throw new SerializationException("Error while formatting the key", ioe);
      }
    }
    try {
      writeTo(consumerRecord.value(), output);
      if (printValueId) {
        output.write(idSeparator);
        int schemaId = schemaIdFor(consumerRecord.value());
        output.print(schemaId);
      }
      output.write(lineSeparator);
    } catch (IOException ioe) {
      throw new SerializationException("Error while formatting the value", ioe);
    }
  }

  private void writeTo(byte[] data, PrintStream output) throws IOException {
    Message object = (Message) deserialize(data);
    try {
      output.print(JsonFormat.printer().print(object));
    } catch (InvalidProtocolBufferException e) {
      throw new SerializationException("Error serializing Protobuf data to json", e);
    }
  }

  @Override
  public void close() {
    // nothing to do
  }

  private int schemaIdFor(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    return buffer.getInt();
  }

  private SchemaRegistryClient createSchemaRegistry(
      String schemaRegistryUrl, Map<String, Object> originals
  ) {
    return schemaRegistry != null ? schemaRegistry : new CachedSchemaRegistryClient(
        Collections.singletonList(schemaRegistryUrl),
        AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
        Collections.singletonList(new ProtobufSchemaProvider()),
        originals
    );
  }
}
