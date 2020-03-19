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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
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
    String valueSchemaString = getSchemaString(props, false);
    List<SchemaReference> valueRefs = getSchemaReferences(props, false);
    valueSchema = parseSchema(schemaRegistry, valueSchemaString, valueRefs);

    Serializer keySerializer = getKeySerializer(props);

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

    if (needsKeySchema()) {
      String keySchemaString = getSchemaString(props, true);
      List<SchemaReference> keyRefs = getSchemaReferences(props, true);
      keySchema = parseSchema(schemaRegistry, keySchemaString, keyRefs);
    }
  }

  protected abstract ParsedSchema parseSchema(
      SchemaRegistryClient schemaRegistry,
      String schema,
      List<SchemaReference> references
  );

  private String getSchemaString(Properties props, boolean isKey) {
    String propKeyRaw = isKey ? "key.schema" : "value.schema";
    String propKeyFile = isKey ? "key.schema.file" : "value.schema.file";
    if (props.containsKey(propKeyRaw)) {
      return props.getProperty(propKeyRaw);
    } else if (props.containsKey(propKeyFile)) {
      try {
        return new String(Files.readAllBytes(Paths.get(props.getProperty(propKeyFile))),
                          StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new ConfigException("Error reading schema from " + props.getProperty(propKeyFile));
      }
    } else {
      throw new ConfigException("Must provide the " + (isKey ? "key" : "value")
                                + " schema in either " + propKeyRaw + " or " + propKeyFile);
    }
  }

  private List<SchemaReference> getSchemaReferences(Properties props, boolean isKey) {
    String propKey = isKey ? "key.refs" : "value.refs";
    if (props.containsKey(propKey)) {
      try {
        return JacksonMapper.INSTANCE.readValue(
                props.getProperty(propKey),
                new TypeReference<List<SchemaReference>>() {}
        );
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return Collections.emptyList();
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
