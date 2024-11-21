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
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import kafka.common.MessageReader;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public abstract class SchemaMessageReader<T> implements MessageReader {

  public static final String VALUE_SCHEMA = "value.schema";
  public static final String KEY_SCHEMA = "key.schema";

  private String topic = null;
  private BufferedReader reader = null;
  private Boolean parseKey = false;
  private String keySeparator = "\t";
  private boolean parseHeaders = false;
  private String headersDelimiter = "\t";
  private String headersSeparator = ",";
  private Pattern headersSeparatorPattern;
  private String headersKeySeparator = ":";
  private boolean ignoreError = false;
  private String nullMarker = null;
  protected ParsedSchema keySchema = null;
  protected ParsedSchema valueSchema = null;
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
      String url, ParsedSchema keySchema, ParsedSchema valueSchema,
      String topic, boolean parseKey, BufferedReader reader,
      boolean normalizeSchema, boolean autoRegister, boolean useLatest
  ) {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.topic = topic;
    this.keySubject = topic != null ? topic + "-key" : null;
    this.valueSubject = topic != null ? topic + "-value" : null;
    this.parseKey = parseKey;
    this.reader = reader;
    this.serializer = createSerializer(null);
    Map<String, Object> configs = new HashMap<>();
    configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url);
    configs.put(AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, normalizeSchema);
    configs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, autoRegister);
    configs.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, useLatest);
    this.serializer.configure(configs, false);
  }

  protected abstract SchemaMessageSerializer<T> createSerializer(Serializer keySerializer);

  @Override
  public void init(java.io.InputStream inputStream, Properties props) {
    topic = props.getProperty("topic");
    if (topic == null) {
      throw new ConfigException("Missing topic!");
    }
    if (props.containsKey("parse.key")) {
      parseKey = props.getProperty("parse.key").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("key.separator")) {
      keySeparator = props.getProperty("key.separator");
    }
    if (props.containsKey("parse.headers")) {
      parseHeaders = Boolean.parseBoolean(props.getProperty("parse.headers").trim());
    }
    if (props.containsKey("headers.delimiter")) {
      headersDelimiter = props.getProperty("headers.delimiter");
    }
    if (props.containsKey("headers.separator")) {
      headersSeparator = props.getProperty("headers.separator");
    }
    headersSeparatorPattern = Pattern.compile(headersSeparator);
    if (props.containsKey("headers.key.separator")) {
      headersKeySeparator = props.getProperty("headers.key.separator");
    }
    if (Objects.equals(headersDelimiter, headersSeparator)) {
      throw new KafkaException("headers.delimiter and headers.separator may not be equal");
    }
    if (Objects.equals(headersDelimiter, headersKeySeparator)) {
      throw new KafkaException("headers.delimiter and headers.key.separator may not be equal");
    }
    if (Objects.equals(headersSeparator, headersKeySeparator)) {
      throw new KafkaException("headers.separator and headers.key.separator may not be equal");
    }
    if (props.containsKey("ignore.error")) {
      ignoreError = props.getProperty("ignore.error").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("null.marker")) {
      nullMarker = props.getProperty("null.marker");
    }
    if (Objects.equals(nullMarker, keySeparator)) {
      throw new KafkaException("null.marker and key.separator may not be equal");
    }
    if (Objects.equals(nullMarker, headersSeparator)) {
      throw new KafkaException("null.marker and headers.separator may not be equal");
    }
    if (Objects.equals(nullMarker, headersDelimiter)) {
      throw new KafkaException("null.marker and headers.delimiter may not be equal");
    }
    if (Objects.equals(nullMarker, headersKeySeparator)) {
      throw new KafkaException("null.marker and headers.key.separator may not be equal");
    }

    reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    String url = props.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    if (url == null) {
      throw new ConfigException("Missing schema registry url!");
    }

    Serializer<?> keySerializer = null;
    if (props.containsKey("key.serializer")) {
      keySerializer = getSerializerProperty(true, props, "key.serializer");
    }

    if (this.serializer == null) {
      Map<String, Object> originals = getPropertiesMap(props);
      Object autoRegister = props.get("auto.register");
      if (autoRegister != null) {
        // for backward compatibility
        originals.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, autoRegister);
      }
      this.serializer = createSerializer(keySerializer);
      this.serializer.configure(originals, false);
    }

    // This class is only used in a scenario where a single schema is used. It does not support
    // writing data which has a different schema for each record. Therefore, we can calculate the
    // subject names and schemas once rather than per-message in
    // AbstractKafkaSchemaSerDe#getSubjectName(...) as would otherwise happen.
    final AbstractKafkaSchemaSerDeConfig config =
            new AbstractKafkaSchemaSerDeConfig(AbstractKafkaSchemaSerDeConfig.baseConfigDef(),
                    props, false);

    valueSchema = getSchema(serializer.getSchemaRegistryClient(), props, false);
    final Object valueSubjectNameStrategy = config.valueSubjectNameStrategy();
    valueSubject = getSubjectName(valueSubjectNameStrategy, topic, false, valueSchema);

    if (needsKeySchema()) {
      keySchema = getSchema(serializer.getSchemaRegistryClient(), props, true);
      final Object keySubjectNameStrategy = config.keySubjectNameStrategy();
      keySubject = getSubjectName(keySubjectNameStrategy, topic, true, keySchema);
    }
  }

  private Serializer<?> getSerializerProperty(
      boolean isKey, Properties props, String propertyName) {
    try {
      String serializerName = (String) props.get(propertyName);
      Serializer<?> serializer = (Serializer<?>)
          Class.forName(serializerName).getDeclaredConstructor().newInstance();
      Map<String, ?> serializerConfig =
          propertiesWithKeyPrefixStripped(propertyName + ".", props);
      serializer.configure(serializerConfig, isKey);
      return serializer;
    } catch (Exception e) {
      throw new ConfigException("Error initializing " + propertyName + ": " + e.getMessage());
    }
  }

  private Map<String, ?> propertiesWithKeyPrefixStripped(String prefix, Properties props) {
    return props.entrySet().stream()
        .filter(e -> ((String) e.getKey()).startsWith(prefix))
        .collect(Collectors.toMap(
            e -> ((String) e.getKey()).substring(prefix.length()), Map.Entry::getValue));
  }

  private Map<String, Object> getPropertiesMap(Properties props) {
    Map<String, Object> originals = new HashMap<>();
    for (final String name: props.stringPropertyNames()) {
      originals.put(name, props.getProperty(name));
    }
    return originals;
  }

  /**
   * @see AbstractKafkaSchemaSerDe#getSubjectName(String, boolean, Object, ParsedSchema)
   */
  private String getSubjectName(Object subjectNameStrategy, String topic, boolean isKey,
                                ParsedSchema schema) {
    if (subjectNameStrategy instanceof SubjectNameStrategy) {
      return ((SubjectNameStrategy) subjectNameStrategy).subjectName(topic, isKey, schema);
    } else {
      // We don't have an instance of an object, only a schema, so we can't provide the necessary
      // params to the deprecated strategy.
      throw new RuntimeException("Classes extending deprecated "
              + io.confluent.kafka.serializers.subject.SubjectNameStrategy.class.getCanonicalName()
              + " are not supported. Use classes extending "
              + SubjectNameStrategy.class.getCanonicalName() + " instead.");
    }
  }

  protected ParsedSchema parseSchema(
      SchemaRegistryClient schemaRegistry,
      String schema,
      List<SchemaReference> references
  ) {
    SchemaProvider provider = getProvider();
    provider.configure(Collections.singletonMap(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG,
        schemaRegistry));
    return provider.parseSchema(schema, references).get();
  }

  private ParsedSchema getSchema(SchemaRegistryClient schemaRegistry,
                                 Properties props,
                                 boolean isKey) {
    ParsedSchema schema = getSchemaById(schemaRegistry, props, isKey);
    if (schema != null) {
      return schema;
    }
    String schemaString = getSchemaString(props, isKey);
    List<SchemaReference> refs = getSchemaReferences(props, isKey);
    Metadata metadata = getMetadata(props, isKey);
    RuleSet ruleSet = getRuleSet(props, isKey);
    return parseSchema(schemaRegistry, schemaString, refs).copy(metadata, ruleSet);
  }

  /**
   * @return schema, if props identifies one by ID, otherwise null
   */
  private ParsedSchema getSchemaById(SchemaRegistryClient schemaRegistry,
                                     Properties props,
                                     boolean isKey) {
    String propKeyId = isKey ? "key.schema.id" : "value.schema.id";
    int schemaId = 0;
    try {
      if (props.containsKey(propKeyId)) {
        schemaId = Integer.parseInt(props.getProperty(propKeyId));
        return schemaRegistry.getSchemaById(schemaId);
      }
      return null;
    } catch (NumberFormatException e) {
      throw new SerializationException(
          String.format("Error parsing %s as int", propKeyId), e);
    } catch (RestClientException | IOException e) {
      throw new SerializationException(
          String.format("Error retrieving schema for id %d", schemaId), e);
    }
  }

  private String getSchemaString(Properties props, boolean isKey) {
    String propKeyRaw = isKey ? KEY_SCHEMA : VALUE_SCHEMA;
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
                                + " schema in either " + propKeyRaw
                                + ", " + propKeyRaw + ".id, or " + propKeyFile);
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

  private Metadata getMetadata(Properties props, boolean isKey) {
    String propKey = isKey ? "key.metadata" : "value.metadata";
    if (props.containsKey(propKey)) {
      try {
        return JacksonMapper.INSTANCE.readValue(
            props.getProperty(propKey),
            Metadata.class
        );
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  private RuleSet getRuleSet(Properties props, boolean isKey) {
    String propKey = isKey ? "key.rule.set" : "value.rule.set";
    if (props.containsKey(propKey)) {
      try {
        return JacksonMapper.INSTANCE.readValue(
            props.getProperty(propKey),
            RuleSet.class
        );
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  private boolean needsKeySchema() {
    return parseKey && serializer.getKeySerializer() == null;
  }

  @Override
  public ProducerRecord<byte[], byte[]> readMessage() {
    try {
      String line = reader.readLine();
      if (line == null) {
        return null;
      }

      String headersString = parse(parseHeaders, line, 0, headersDelimiter, "headers delimiter");
      int headersOffset = headersString == null
          ? 0
          : headersString.length() + headersDelimiter.length();
      Headers headers = new RecordHeaders();
      if (headersString != null && !headersString.equals(nullMarker)) {
        splitHeaders(headersString, line).forEach(
            header -> headers.add(header.getKey(), header.getValue()));
      }

      String keyString = parse(parseKey, line, headersOffset, keySeparator, "key separator");
      int keyOffset = keyString == null ? 0 : keyString.length() + keySeparator.length();
      byte[] serializedKey = null;
      if (keyString != null && !keyString.equals(nullMarker)) {
        if (serializer.getKeySerializer() != null) {
          serializedKey = serializeNonSchemaKey(headers, keyString);
        } else {
          T key = readFrom(keyString, keySchema);
          serializedKey = serializer.serialize(keySubject, topic, true, headers, key, keySchema);
        }
      }

      String valueString = line.substring(headersOffset + keyOffset);
      byte[] serializedValue = null;
      if (valueString != null && !valueString.equals(nullMarker)) {
        T value = readFrom(valueString, valueSchema);
        serializedValue = serializer.serialize(valueSubject, topic, false, headers,
            value, valueSchema);
      }

      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
          topic,
          null,
          serializedKey,
          serializedValue,
          headers);

      return record;
    } catch (IOException e) {
      throw new KafkaException("Error reading from input", e);
    }
  }

  private String parse(
      boolean enabled, String line, int startIndex, String demarcation, String demarcationName) {
    if (!enabled) {
      return null;
    }
    int index = line.indexOf(demarcation, startIndex);
    if (index < 0) {
      if (ignoreError) {
        return null;
      } else {
        throw new KafkaException("No " + demarcationName + " found in line " + line);
      }
    }
    return line.substring(startIndex, index);
  }

  private List<Map.Entry<String, byte[]>> splitHeaders(String headers, String line) {
    return Arrays.stream(headersSeparatorPattern.split(headers))
        .map(pair -> {
          int index = pair.indexOf(headersKeySeparator);
          if (index < 0) {
            if (ignoreError) {
              return new AbstractMap.SimpleEntry<>(pair, (byte[]) null);
            } else {
              throw new KafkaException(
                  "No header key separator found in pair '" + pair + "' in line " + line);
            }
          }
          String headerKey = pair.substring(0, index);
          if (Objects.equals(headerKey, nullMarker)) {
            throw new KafkaException("Header keys should not be equal to the null marker '"
                + nullMarker + "' as they can't be null");
          }
          String headerValueString = pair.substring(index + headersKeySeparator.length());
          byte[] headerValue = null;
          if (!Objects.equals(headerValueString, nullMarker)) {
            headerValue = headerValueString.getBytes(StandardCharsets.UTF_8);
          }
          return new AbstractMap.SimpleEntry<>(headerKey, headerValue);
        })
        .collect(Collectors.toList());
  }

  private byte[] serializeNonSchemaKey(Headers headers, String keyString) {
    Class<?> serializerClass = serializer.getKeySerializer().getClass();
    if (serializerClass == LongSerializer.class) {
      Long longKey = Long.parseLong(keyString);
      return serializer.serializeKey(topic, headers, longKey);
    }
    if (serializerClass == IntegerSerializer.class) {
      Integer intKey = Integer.parseInt(keyString);
      return serializer.serializeKey(topic, headers, intKey);
    }
    if (serializerClass == ShortSerializer.class) {
      Short shortKey = Short.parseShort(keyString);
      return serializer.serializeKey(topic, headers, shortKey);
    }
    return serializer.serializeKey(topic, headers, keyString);
  }

  protected abstract T readFrom(String jsonString, ParsedSchema schema);

  @Override
  public void close() {
    if (serializer != null) {
      try {
        serializer.close();
      } catch (IOException e) {
        throw new RuntimeException("Exception while closing serializer", e);
      }
    }
  }

  protected abstract SchemaProvider getProvider();
}
