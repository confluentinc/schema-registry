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

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import kafka.common.KafkaException;
import kafka.common.MessageReader;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufSerializer;

/**
 * Example
 * To use ProtobufMessageReader, first make sure that Zookeeper, Kafka and schema registry server
 * are
 * all started. Second, make sure the jar for ProtobufMessageReader and its dependencies are
 * included
 * in the classpath of kafka-console-producer.sh. Then run the following
 * command.
 *
 * <p>Send Protobuf record as value.
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic t1 \
 * --line-reader io.confluent.kafka.formatter.ProtobufMessageReader \
 * --property schema.registry.url=http://localhost:8081 \
 * --property value.schema='syntax = "proto3"; message MyRecord { string f1 = 1; }'
 *
 * <p>In the shell, type in the following.
 * {"f1": "value1"}
 */
public class ProtobufMessageReader extends AbstractKafkaProtobufSerializer
    implements MessageReader {

  private String topic = null;
  private BufferedReader reader = null;
  private Boolean parseKey = false;
  private String keySeparator = "\t";
  private boolean ignoreError = false;
  private ProtobufSchema keySchema = null;
  private ProtobufSchema valueSchema = null;
  private String keySubject = null;
  private String valueSubject = null;
  private Serializer keySerializer;

  /**
   * Constructor needed by kafka console producer.
   */
  public ProtobufMessageReader() {
  }

  /**
   * For testing only.
   */
  ProtobufMessageReader(
      SchemaRegistryClient schemaRegistryClient,
      ProtobufSchema keySchema,
      ProtobufSchema valueSchema,
      String topic,
      boolean parseKey,
      BufferedReader reader,
      boolean autoRegister
  ) {
    this.schemaRegistry = schemaRegistryClient;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.topic = topic;
    this.keySubject = topic + "-key";
    this.valueSubject = topic + "-value";
    this.parseKey = parseKey;
    this.reader = reader;
    this.autoRegisterSchema = autoRegister;
  }

  @Override
  public void init(java.io.InputStream inputStream, Properties props) {
    topic = props.getProperty("topic");
    if (props.containsKey("parse.key")) {
      parseKey = props.getProperty("parse.key").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("key.separator")) {
      keySeparator = props.getProperty("key.separator");
    }
    if (props.containsKey("ignore.error")) {
      ignoreError = props.getProperty("ignore.error").trim().toLowerCase().equals("true");
    }
    reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    String url = props.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    if (url == null) {
      throw new ConfigException("Missing schema registry url!");
    }

    Map<String, Object> originals = getPropertiesMap(props);

    schemaRegistry = new CachedSchemaRegistryClient(Collections.singletonList(url),
        AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
        Collections.singletonList(new ProtobufSchemaProvider()),
        originals
    );
    if (!props.containsKey("value.schema")) {
      throw new ConfigException("Must provide the Protobuf schema string in value.schema");
    }
    String valueSchemaString = props.getProperty("value.schema");
    valueSchema = new ProtobufSchema(valueSchemaString);

    keySerializer = getKeySerializer(props);

    if (needsKeySchema()) {
      if (!props.containsKey("key.schema")) {
        throw new ConfigException("Must provide the Protobuf schema string in key.schema");
      }
      String keySchemaString = props.getProperty("key.schema");
      keySchema = new ProtobufSchema(keySchemaString);
    }
    keySubject = topic + "-key";
    valueSubject = topic + "-value";
    if (props.containsKey("auto.register")) {
      this.autoRegisterSchema = Boolean.parseBoolean(props.getProperty("auto.register").trim());
    } else {
      this.autoRegisterSchema = true;
    }
  }

  private Serializer getKeySerializer(Properties props) throws ConfigException {
    if (props.containsKey("key.serializer")) {
      try {
        return (Serializer) Class.forName((String) props.get("key.serializer")).newInstance();
      } catch (Exception e) {
        throw new ConfigException("Error initializing Key serializer", e);
      }
    } else {
      return null;
    }
  }

  private boolean needsKeySchema() {
    return parseKey && keySerializer == null;
  }

  private Map<String, Object> getPropertiesMap(Properties props) {
    Map<String, Object> originals = new HashMap<>();
    for (final String name : props.stringPropertyNames()) {
      originals.put(name, props.getProperty(name));
    }
    return originals;
  }

  @Override
  public ProducerRecord<byte[], byte[]> readMessage() {
    try {
      String line = reader.readLine();
      if (line == null) {
        return null;
      }
      if (!parseKey) {
        DynamicMessage value = jsonToProtobuf(line, valueSchema);
        byte[] serializedValue = serializeImpl(valueSubject, topic, false, value, valueSchema);
        return new ProducerRecord<>(topic, serializedValue);
      } else {
        int keyIndex = line.indexOf(keySeparator);
        if (keyIndex < 0) {
          if (ignoreError) {
            DynamicMessage value = jsonToProtobuf(line, valueSchema);
            byte[] serializedValue = serializeImpl(valueSubject, topic, false, value, valueSchema);
            return new ProducerRecord<>(topic, serializedValue);
          } else {
            throw new KafkaException("No key found in line " + line);
          }
        } else {
          String keyString = line.substring(0, keyIndex);
          String valueString = (keyIndex + keySeparator.length() > line.length())
                               ? ""
                               : line.substring(keyIndex + keySeparator.length());
          byte[] serializedKey;
          if (keySerializer != null) {
            serializedKey = keySerializer.serialize(topic, keyString);
          } else {
            DynamicMessage key = jsonToProtobuf(keyString, keySchema);
            serializedKey = serializeImpl(keySubject, topic, true, key, keySchema);
          }
          DynamicMessage value = jsonToProtobuf(valueString, valueSchema);
          byte[] serializedValue = serializeImpl(valueSubject, topic, false, value, valueSchema);
          return new ProducerRecord<>(topic, serializedKey, serializedValue);
        }
      }
    } catch (IOException e) {
      throw new KafkaException("Error reading from input", e);
    }
  }

  private DynamicMessage jsonToProtobuf(String jsonString, ProtobufSchema schema) {
    try {
      DynamicMessage.Builder message = schema.newMessageBuilder();
      JsonFormat.parser().merge(jsonString, message);
      return message.build();
    } catch (InvalidProtocolBufferException e) {
      throw new SerializationException(String.format("Error deserializing json %s to Protobuf of "
              + "schema %s",
          jsonString,
          schema
      ), e);
    }
  }

  @Override
  public void close() {
    // nothing to do
  }
}
