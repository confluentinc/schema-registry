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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.common.KafkaException;
import kafka.common.MessageReader;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public abstract class SchemaMessageReader<T> implements MessageReader {

  private String topic = null;
  private BufferedReader reader = null;
  private Boolean parseKey = false;
  private String keySeparator = "\t";
  private boolean ignoreError = false;
  private ParsedSchema keySchema = null;
  private ParsedSchema valueSchema = null;
  private String keySubject = null;
  private String valueSubject = null;
  private SchemaMessageSerializer<T> serializer;

  /**
   * Constructor needed by kafka console producer.
   */
  public SchemaMessageReader() {
  }

  /**
   * For testing only.
   */
  public SchemaMessageReader(
      SchemaRegistryClient schemaRegistryClient, ParsedSchema keySchema, ParsedSchema valueSchema,
      String topic, boolean parseKey, BufferedReader reader, boolean autoRegister
  ) {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.topic = topic;
    this.keySubject = topic + "-key";
    this.valueSubject = topic + "-value";
    this.parseKey = parseKey;
    this.reader = reader;
    this.serializer = createSerializer(schemaRegistryClient, autoRegister, null);
  }

  protected abstract SchemaMessageSerializer<T> createSerializer(
      SchemaRegistryClient schemaRegistryClient,
      boolean autoRegister,
      Serializer keySerializer
  );

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

    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
        url, AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT, originals);
    if (!props.containsKey("value.schema")) {
      throw new ConfigException("Must provide the schema string in value.schema");
    }
    String valueSchemaString = props.getProperty("value.schema");
    List<SchemaReference> valueRefs = Collections.emptyList();
    if (props.containsKey("value.refs")) {
      try {
        valueRefs = new ObjectMapper().readValue(
            props.getProperty("value.refs"),
            new TypeReference<List<SchemaReference>>() {}
        );
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    valueSchema = parseSchema(schemaRegistry, valueSchemaString, valueRefs);

    Serializer keySerializer = getKeySerializer(props);

    if (needsKeySchema()) {
      if (!props.containsKey("key.schema")) {
        throw new ConfigException("Must provide the schema string in key.schema");
      }
      String keySchemaString = props.getProperty("key.schema");
      List<SchemaReference> keyRefs = Collections.emptyList();
      if (props.containsKey("key.refs")) {
        try {
          keyRefs = new ObjectMapper().readValue(
              props.getProperty("key.refs"),
              new TypeReference<List<SchemaReference>>() {}
          );
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      keySchema = parseSchema(schemaRegistry, keySchemaString, keyRefs);
    }
    keySubject = topic + "-key";
    valueSubject = topic + "-value";
    boolean autoRegisterSchema;
    if (props.containsKey("auto.register")) {
      autoRegisterSchema = Boolean.parseBoolean(props.getProperty("auto.register").trim());
    } else {
      autoRegisterSchema = true;
    }

    if (this.serializer == null) {
      this.serializer = createSerializer(schemaRegistry, autoRegisterSchema, keySerializer);
    }
  }

  protected abstract ParsedSchema parseSchema(
      SchemaRegistryClient schemaRegistry,
      String schema,
      List<SchemaReference> references
  );

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
    return parseKey && serializer.getKeySerializer() == null;
  }

  private Map<String, Object> getPropertiesMap(Properties props) {
    Map<String, Object> originals = new HashMap<>();
    for (final String name: props.stringPropertyNames()) {
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
        T value = readFrom(line, valueSchema);
        byte[] serializedValue = serializer.serialize(valueSubject, topic, false, value,
            valueSchema);
        return new ProducerRecord<>(topic, serializedValue);
      } else {
        int keyIndex = line.indexOf(keySeparator);
        if (keyIndex < 0) {
          if (ignoreError) {
            T value = readFrom(line, valueSchema);
            byte[] serializedValue = serializer.serialize(valueSubject, topic, false, value,
                valueSchema);
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
          if (serializer.getKeySerializer() != null) {
            serializedKey = serializeNonSchemaKey(keyString);
          } else {
            T key = readFrom(keyString, keySchema);
            serializedKey = serializer.serialize(keySubject, topic, true, key, keySchema);
          }
          T value = readFrom(valueString, valueSchema);
          byte[] serializedValue = serializer.serialize(
              valueSubject, topic, false, value, valueSchema);
          return new ProducerRecord<>(topic, serializedKey, serializedValue);
        }
      }
    } catch (IOException e) {
      throw new KafkaException("Error reading from input", e);
    }
  }

  private byte[] serializeNonSchemaKey(String keyString) {
    Class serializerClass = serializer.getKeySerializer().getClass();
    if (serializerClass == LongSerializer.class) {
      Long longKey = Long.parseLong(keyString);
      return serializer.serializeKey(topic, longKey);
    }
    if (serializerClass == IntegerSerializer.class) {
      Integer intKey = Integer.parseInt(keyString);
      return serializer.serializeKey(topic, intKey);
    }
    if (serializerClass == ShortSerializer.class) {
      Short shortKey = Short.parseShort(keyString);
      return serializer.serializeKey(topic, shortKey);
    }
    return serializer.serializeKey(topic, keyString);
  }

  protected abstract T readFrom(String jsonString, ParsedSchema schema);

  @Override
  public void close() {
    // nothing to do
  }
}
