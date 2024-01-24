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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public abstract class SchemaMessageFormatter<T> implements MessageFormatter {

  private static final byte[] NULL_BYTES = "null".getBytes(StandardCharsets.UTF_8);
  private boolean printTimestamp = false;
  private boolean printKey = false;
  private boolean printPartition = false;
  private boolean printOffset = false;
  private boolean printHeaders = false;
  private boolean printIds = false;
  private boolean printKeyId = false;
  private boolean printValueId = false;
  private byte[] keySeparator = "\t".getBytes(StandardCharsets.UTF_8);
  private byte[] lineSeparator = "\n".getBytes(StandardCharsets.UTF_8);
  private byte[] headersSeparator = ",".getBytes(StandardCharsets.UTF_8);
  private byte[] idSeparator = "\t".getBytes(StandardCharsets.UTF_8);
  private byte[] nullLiteral = NULL_BYTES;
  private Deserializer<?> headersDeserializer;
  protected SchemaMessageDeserializer<T> deserializer;

  /**
   * Constructor needed by kafka console consumer.
   */
  public SchemaMessageFormatter() {
  }

  /**
   * For testing only.
   */
  public SchemaMessageFormatter(String url, Deserializer keyDeserializer) {
    this.deserializer = createDeserializer(keyDeserializer);
    Map<String, Object> configs = new HashMap<>();
    configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url);
    this.deserializer.configure(configs, false);
  }

  protected abstract SchemaMessageDeserializer<T> createDeserializer(Deserializer keyDeserializer);

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

    if (props.containsKey("print.timestamp")) {
      printTimestamp = props.getProperty("print.timestamp").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("print.key")) {
      printKey = props.getProperty("print.key").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("print.partition")) {
      printPartition = props.getProperty("print.partition").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("print.offset")) {
      printOffset = props.getProperty("print.offset").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("print.headers")) {
      printHeaders = props.getProperty("print.headers").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("key.separator")) {
      keySeparator = props.getProperty("key.separator").getBytes(StandardCharsets.UTF_8);
    }
    if (props.containsKey("line.separator")) {
      lineSeparator = props.getProperty("line.separator").getBytes(StandardCharsets.UTF_8);
    }
    if (props.containsKey("headers.separator")) {
      headersSeparator = props.getProperty("headers.separator").getBytes(StandardCharsets.UTF_8);
    }
    if (props.containsKey("null.literal")) {
      nullLiteral = props.getProperty("null.literal").getBytes(StandardCharsets.UTF_8);
    }
    Deserializer<?> keyDeserializer = null;
    if (props.containsKey("key.deserializer")) {
      keyDeserializer = getDeserializerProperty(true, props, "key.deserializer");
    }
    if (props.containsKey("headers.deserializer")) {
      headersDeserializer = getDeserializerProperty(false, props, "headers.deserializer");
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
      this.deserializer = createDeserializer(keyDeserializer);
      this.deserializer.configure(originals, false);
    }
  }

  private Deserializer<?> getDeserializerProperty(
      boolean isKey, Properties props, String propertyName) {
    try {
      String serializerName = (String) props.get(propertyName);
      Deserializer<?> deserializer = (Deserializer<?>)
          Class.forName(serializerName).getDeclaredConstructor().newInstance();
      Map<String, ?> deserializerConfig =
          propertiesWithKeyPrefixStripped(propertyName + ".", props);
      deserializer.configure(deserializerConfig, isKey);
      return deserializer;
    } catch (Exception e) {
      throw new ConfigException("Error initializing " + propertyName + ": " + e.getMessage());
    }
  }

  private Map<String, ?> propertiesWithKeyPrefixStripped(String prefix, Properties props) {
    return props.entrySet().stream()
        .filter(e -> ((String) e.getKey()).startsWith(prefix))
        .collect(Collectors.toMap(
            e -> ((String) e.getKey()).substring(prefix.length()), Entry::getValue));
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
    if (printPartition) {
      try {
        output.write("Partition:".getBytes(StandardCharsets.UTF_8));
        output.write(String.valueOf(consumerRecord.partition()).getBytes(StandardCharsets.UTF_8));
        output.write(keySeparator);
      } catch (IOException ioe) {
        throw new SerializationException("Error while formatting the partition", ioe);
      }
    }
    if (printOffset) {
      try {
        output.write("Offset:".getBytes(StandardCharsets.UTF_8));
        output.write(String.valueOf(consumerRecord.offset()).getBytes(StandardCharsets.UTF_8));
        output.write(keySeparator);
      } catch (IOException ioe) {
        throw new SerializationException("Error while formatting the offset", ioe);
      }
    }
    if (printHeaders) {
      try {
        Iterator<Header> headersIt = consumerRecord.headers().iterator();
        if (headersIt.hasNext()) {
          headersIt.forEachRemaining(header -> {
            try {
              output.write((header.key() + ":").getBytes(StandardCharsets.UTF_8));
              output.write(deserialize(headersDeserializer, consumerRecord, header.value()));
              if (headersIt.hasNext()) {
                output.write(headersSeparator);
              }
            } catch (IOException ioe) {
              throw new SerializationException("Error while formatting the headers", ioe);
            }
          });
        } else {
          output.write("NO_HEADERS".getBytes(StandardCharsets.UTF_8));
        }
        output.write(keySeparator);
      } catch (IOException ioe) {
        throw new SerializationException("Error while formatting the headers", ioe);
      }
    }
    if (printKey) {
      try {
        if (deserializer.getKeyDeserializer() != null) {
          Object deserializedKey = consumerRecord.key() == null
              ? null
              : deserializer.deserializeKey(
                  consumerRecord.topic(), consumerRecord.headers(), consumerRecord.key());
          output.write(
              deserializedKey != null ? deserializedKey.toString().getBytes(StandardCharsets.UTF_8)
                                      : nullLiteral);
        } else {
          if (consumerRecord.key() != null) {
            writeTo(consumerRecord.topic(), true, consumerRecord.headers(),
                consumerRecord.key(), output);
          } else {
            output.write(nullLiteral);
          }
        }
        if (printKeyId) {
          output.write(idSeparator);
          if (consumerRecord.key() != null) {
            int schemaId = schemaIdFor(consumerRecord.key());
            output.print(schemaId);
          } else {
            output.write(nullLiteral);
          }
        }
        output.write(keySeparator);
      } catch (IOException ioe) {
        throw new SerializationException("Error while formatting the key", ioe);
      }
    }
    try {
      if (consumerRecord.value() != null) {
        writeTo(consumerRecord.topic(), false, consumerRecord.headers(),
            consumerRecord.value(), output);
      } else {
        output.write(nullLiteral);
      }
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

  protected abstract void writeTo(String topic, Boolean isKey, Headers headers,
      byte[] data, PrintStream output) throws IOException;

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

  private byte[] deserialize(Deserializer<?> deserializer,
      ConsumerRecord<byte[], byte[]> consumerRecord, byte[] sourceBytes) {
    if (deserializer == null || sourceBytes == null) {
      return nullLiteral;
    }
    Object deserializedHeader =
        deserializer.deserialize(consumerRecord.topic(), consumerRecord.headers(), sourceBytes);
    return deserializedHeader != null
        ? deserializedHeader.toString().getBytes(StandardCharsets.UTF_8)
        : nullLiteral;
  }

  private int schemaIdFor(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    return buffer.getInt();
  }

  protected abstract SchemaProvider getProvider();
}
