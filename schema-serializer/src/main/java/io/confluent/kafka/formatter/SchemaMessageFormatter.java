/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public abstract class SchemaMessageFormatter<T> implements MessageFormatter {

  private static final byte[] NULL_BYTES = "null".getBytes(StandardCharsets.UTF_8);
  private boolean printKey = false;
  private boolean printTimestamp = false;
  private boolean printIds = false;
  private boolean printKeyId = false;
  private boolean printValueId = false;
  private byte[] keySeparator = "\t".getBytes(StandardCharsets.UTF_8);
  private byte[] lineSeparator = "\n".getBytes(StandardCharsets.UTF_8);
  private byte[] idSeparator = "\t".getBytes(StandardCharsets.UTF_8);
  protected SchemaMessageDeserializer<T> deserializer;

  /**
   * Constructor needed by kafka console consumer.
   */
  public SchemaMessageFormatter() {
  }

  /**
   * For testing only.
   */
  public SchemaMessageFormatter(
      SchemaRegistryClient schemaRegistryClient,
      Deserializer keyDeserializer
  ) {
    this.deserializer = createDeserializer(schemaRegistryClient, keyDeserializer);
  }

  protected abstract SchemaMessageDeserializer<T> createDeserializer(
      SchemaRegistryClient schemaRegistryClient,
      Deserializer keyDeserializer
  );

  @Override
  public void configure(Map<String, ?> configs) {
    Properties properties = new Properties();
    properties.putAll(configs);
    this.init(properties);
  }

  public void init(Properties props) {
    if (props == null) {
      throw new ConfigException("Missing schema registry url!");
    }
    String url = props.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    if (url == null) {
      throw new ConfigException("Missing schema registry url!");
    }

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
    Deserializer keyDeserializer = null;
    if (props.containsKey("key.deserializer")) {
      try {
        keyDeserializer =
            (Deserializer)Class.forName((String) props.get("key.deserializer")).newInstance();
      } catch (Exception e) {
        throw new ConfigException("Error initializing Key deserializer: " + e.getMessage());
      }
    }
    if (props.containsKey("print.schema.ids")) {
      printIds = props.getProperty("print.schema.ids").trim().toLowerCase().equals("true");
      if (printIds) {
        printValueId = true;
        if (keyDeserializer == null || keyDeserializer instanceof AbstractKafkaSchemaSerDe) {
          printKeyId = true;
        }
      }
    }
    if (props.containsKey("schema.id.separator")) {
      idSeparator = props.getProperty("schema.id.separator").getBytes(StandardCharsets.UTF_8);
    }

    if (this.deserializer == null) {
      Map<String, Object> originals = getPropertiesMap(props);
      SchemaRegistryClient schemaRegistry = createSchemaRegistry(url, originals);
      this.deserializer = createDeserializer(schemaRegistry, keyDeserializer);
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
          output.write(String.format("%s:%d",
                  timestampType, consumerRecord.timestamp()).getBytes(StandardCharsets.UTF_8));
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
        if (deserializer.getKeyDeserializer() != null) {
          Object deserializedKey = consumerRecord.key() == null
              ? null
              : deserializer.deserializeKey(consumerRecord.topic(), consumerRecord.key());
          output.write(
              deserializedKey != null ? deserializedKey.toString().getBytes(StandardCharsets.UTF_8)
                                      : NULL_BYTES);
        } else {
          writeTo(consumerRecord.topic(), consumerRecord.key(), output);
        }
        if (printKeyId) {
          output.write(idSeparator);
          if (consumerRecord.key() != null) {
            int schemaId = schemaIdFor(consumerRecord.key());
            output.print(schemaId);
          } else {
            output.write(NULL_BYTES);
          }
        }
        output.write(keySeparator);
      } catch (IOException ioe) {
        throw new SerializationException("Error while formatting the key", ioe);
      }
    }
    try {
      writeTo(consumerRecord.topic(), consumerRecord.value(), output);
      if (printValueId) {
        output.write(idSeparator);
        if (consumerRecord.value() != null) {
          int schemaId = schemaIdFor(consumerRecord.value());
          output.print(schemaId);
        } else {
          output.write(NULL_BYTES);
        }
      }
      output.write(lineSeparator);
    } catch (IOException ioe) {
      throw new SerializationException("Error while formatting the value", ioe);
    }
  }

  protected abstract void writeTo(String topic, byte[] data, PrintStream output) throws IOException;

  @Override
  public void close() {
    if (deserializer != null) {
      try {
        deserializer.close();
      } catch (IOException e) {
        throw new RuntimeException("Exception while closing deserializer", e);
      }
    }
  }

  private static final int MAGIC_BYTE = 0x0;

  private int schemaIdFor(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    return buffer.getInt();
  }

  private SchemaRegistryClient createSchemaRegistry(
      String schemaRegistryUrl,
      Map<String, Object> originals
  ) {
    final String maybeMockScope = MockSchemaRegistry.validateAndMaybeGetMockScope(
            Collections.singletonList(schemaRegistryUrl));
    final List<SchemaProvider> providers = Collections.singletonList(getProvider());
    if (maybeMockScope == null) {
      return new CachedSchemaRegistryClient(
              schemaRegistryUrl,
              AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
              providers,
              originals
      );
    } else {
      return MockSchemaRegistry.getClientForScope(maybeMockScope, providers);
    }
  }

  protected abstract SchemaProvider getProvider();
}
