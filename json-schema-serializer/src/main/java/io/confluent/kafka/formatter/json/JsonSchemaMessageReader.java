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
import kafka.common.MessageReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.everit.json.schema.ValidationException;

import java.io.BufferedReader;
import java.io.IOException;

import io.confluent.kafka.formatter.SchemaMessageReader;
import io.confluent.kafka.formatter.SchemaMessageSerializer;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaSerializer;

/**
 * Example
 * To use JsonSchemaMessageReader, first make sure that Zookeeper, Kafka and schema registry
 * server are
 * all started. Second, make sure the jar for JsonSchemaMessageReader and its dependencies are
 * included
 * in the classpath of kafka-console-producer.sh. Then run the following
 * command.
 *
 * <p>1. Send JSON Schema string as value. (make sure there is no space in the schema string)
 * bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic t1 \
 * --line-reader io.confluent.kafka.formatter.JsonSchemaMessageReader \
 * --property schema.registry.url=http://localhost:8081 \
 * --property value.schema='{"type":"string"}'
 *
 * <p>In the shell, type in the following.
 * "a"
 * "b"
 *
 * <p>2. Send JSON Schema record as value.
 * bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic t1 \
 * --line-reader io.confluent.kafka.formatter.JsonSchemaMessageReader \
 * --property schema.registry.url=http://localhost:8081 \
 * --property value.schema='{"type":"object","properties":{"f1":{"type":"string"}}}'
 *
 * <p>In the shell, type in the following.
 * {"f1": "value1"}
 *
 * <p>3. Send JSON Schema string as key and JSON Schema record as value.
 * bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic t1 \
 * --line-reader io.confluent.kafka.formatter.JsonSchemaMessageReader \
 * --property schema.registry.url=http://localhost:8081 \
 * --property parse.key=true \
 * --property key.schema='{"type":"string"}' \
 * --property value.schema='{"type":"object","properties":{"f1":{"type":"string"}}}'
 * "type":"string"}]}'
 *
 * <p>In the shell, type in the following.
 * "key1" \t {"f1": "value1"}
 */
public class JsonSchemaMessageReader extends SchemaMessageReader<JsonNode>
    implements MessageReader {

  private static final ObjectMapper objectMapper = Jackson.newObjectMapper();

  /**
   * Constructor needed by kafka console producer.
   */
  public JsonSchemaMessageReader() {
  }

  /**
   * For testing only.
   */
  JsonSchemaMessageReader(
      SchemaRegistryClient schemaRegistryClient,
      JsonSchema keySchema,
      JsonSchema valueSchema,
      String topic,
      boolean parseKey,
      BufferedReader reader,
      boolean normalizeSchema,
      boolean autoRegister,
      boolean useLatest
  ) {
    super(schemaRegistryClient, keySchema, valueSchema, topic,
        parseKey, reader, normalizeSchema, autoRegister, useLatest);
  }

  @Override
  protected SchemaMessageSerializer<JsonNode> createSerializer(
      SchemaRegistryClient schemaRegistryClient,
      boolean normalizeSchema,
      boolean autoRegister,
      boolean useLatest,
      Serializer keySerializer
  ) {
    return new JsonSchemaMessageSerializer(
        schemaRegistryClient, normalizeSchema, autoRegister, useLatest, keySerializer);
  }

  @Override
  protected SchemaProvider getProvider() {
    return new JsonSchemaProvider();
  }

  @Override
  protected JsonNode readFrom(String jsonString, ParsedSchema schema) {
    try {
      return objectMapper.readTree(jsonString);
    } catch (IOException | ValidationException e) {
      throw new SerializationException(String.format("Error serializing json %s", jsonString), e);
    }
  }

  static class JsonSchemaMessageSerializer extends AbstractKafkaJsonSchemaSerializer<JsonNode>
      implements SchemaMessageSerializer<JsonNode> {

    protected final Serializer keySerializer;

    JsonSchemaMessageSerializer(
        SchemaRegistryClient schemaRegistryClient,
        boolean normalizeSchema, boolean autoRegister, boolean useLatest, Serializer keySerializer
    ) {
      this.schemaRegistry = schemaRegistryClient;
      this.normalizeSchema = normalizeSchema;
      this.autoRegisterSchema = autoRegister;
      this.useLatestVersion = useLatest;
      this.keySerializer = keySerializer;
      this.validate = true;
    }

    @Override
    public Serializer getKeySerializer() {
      return keySerializer;
    }

    @Override
    public byte[] serializeKey(String topic, Object payload) {
      return keySerializer.serialize(topic, payload);
    }

    @Override
    public byte[] serialize(
        String subject,
        String topic,
        boolean isKey,
        JsonNode object,
        ParsedSchema schema
    ) {
      return super.serializeImpl(subject, object, (JsonSchema) schema);
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
