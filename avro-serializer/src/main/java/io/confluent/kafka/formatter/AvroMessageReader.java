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

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.BufferedReader;
import java.io.IOException;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;

/**
 * Example
 * To use AvroMessageReader, first make sure that Zookeeper, Kafka and schema registry server are
 * all started. Second, make sure the jar for AvroMessageReader and its dependencies are included
 * in the classpath of kafka-console-producer.sh. Then run the following
 * command.
 *
 * <p>1. Send Avro string as value. (make sure there is no space in the schema string)
 * bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic t1 \
 *   --line-reader io.confluent.kafka.formatter.AvroMessageReader \
 *   --property schema.registry.url=http://localhost:8081 \
 *   --property value.schema='{"type":"string"}'
 *
 * <p>In the shell, type in the following.
 * "a"
 * "b"
 *
 * <p>2. Send Avro record as value.
 * bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic t1 \
 *   --line-reader io.confluent.kafka.formatter.AvroMessageReader \
 *   --property schema.registry.url=http://localhost:8081 \
 *   --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
 *
 * <p>In the shell, type in the following.
 * {"f1": "value1"}
 *
 * <p>3. Send Avro string as key and Avro record as value.
 * bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic t1 \
 *   --line-reader io.confluent.kafka.formatter.AvroMessageReader \
 *   --property schema.registry.url=http://localhost:8081 \
 *   --property parse.key=true \
 *   --property key.schema='{"type":"string"}' \
 *   --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
 *
 * <p>In the shell, type in the following.
 * "key1" \t {"f1": "value1"}
 *
 */
public class AvroMessageReader extends SchemaMessageReader<Object> {

  private final DecoderFactory decoderFactory = DecoderFactory.get();

  /**
   * Constructor needed by kafka console producer.
   */
  public AvroMessageReader() {
  }

  /**
   * For testing only.
   */
  AvroMessageReader(
      String url,
      Schema keySchema,
      Schema valueSchema,
      String topic,
      boolean parseKey,
      BufferedReader reader,
      boolean normalizeSchema,
      boolean autoRegister,
      boolean useLatest
  ) {
    super(url, new AvroSchema(keySchema), new AvroSchema(valueSchema), topic,
        parseKey, reader, normalizeSchema, autoRegister, useLatest);
  }

  @Override
  protected SchemaMessageSerializer<Object> createSerializer(Serializer keySerializer) {
    return new AvroMessageSerializer(keySerializer);
  }

  @Override
  protected SchemaProvider getProvider() {
    return new AvroSchemaProvider();
  }

  @Override
  protected Object readFrom(String jsonString, ParsedSchema parsedSchema) {
    Schema schema = ((AvroSchema) parsedSchema).rawSchema();
    try {
      Object object = AvroSchemaUtils.toObject(jsonString, (AvroSchema) parsedSchema);
      if (schema.getType().equals(Schema.Type.STRING)) {
        object = ((Utf8) object).toString();
      }
      return object;
    } catch (IOException e) {
      throw new SerializationException(
          String.format("Error deserializing json %s to Avro of schema %s", jsonString, schema), e);
    } catch (AvroRuntimeException e) {
      throw new SerializationException(
          String.format("Error deserializing json %s to Avro of schema %s", jsonString, schema), e);
    }
  }

  static class AvroMessageSerializer extends AbstractKafkaAvroSerializer
      implements SchemaMessageSerializer<Object> {

    protected final Serializer keySerializer;

    AvroMessageSerializer(Serializer keySerializer) {
      this.keySerializer = keySerializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      configure(new KafkaAvroSerializerConfig(configs));
    }

    @Override
    public Serializer getKeySerializer() {
      return keySerializer;
    }

    @Override
    public byte[] serializeKey(String topic, Headers headers, Object payload) {
      return keySerializer.serialize(topic, headers, payload);
    }

    @Override
    public byte[] serialize(
        String subject,
        String topic,
        boolean isKey,
        Headers headers,
        Object object,
        ParsedSchema schema
    ) {
      return super.serializeImpl(subject, topic, headers, object, (AvroSchema) schema);
    }

    @Override
    public SchemaRegistryClient getSchemaRegistryClient() {
      return schemaRegistry;
    }

    @Override
    public void close() throws IOException {
      if (keySerializer != null) {
        keySerializer.close();
      }
      super.close();
    }
  }
}
