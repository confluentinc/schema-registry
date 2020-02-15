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
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.BufferedReader;
import java.util.Collections;
import java.util.List;

import io.confluent.kafka.formatter.SchemaMessageReader;
import io.confluent.kafka.formatter.SchemaMessageSerializer;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
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
public class ProtobufMessageReader extends SchemaMessageReader<Message> {

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
    super(schemaRegistryClient, keySchema, valueSchema, topic,
        parseKey, reader, autoRegister);
  }

  @Override
  protected SchemaMessageSerializer<Message> createSerializer(
      SchemaRegistryClient schemaRegistryClient,
      boolean autoRegister,
      Serializer keySerializer
  ) {
    return new ProtobufMessageSerializer(schemaRegistryClient, autoRegister, keySerializer);
  }

  @Override
  protected ParsedSchema parseSchema(
      SchemaRegistryClient schemaRegistry,
      String schema,
      List<SchemaReference> references
  ) {
    SchemaProvider provider = new ProtobufSchemaProvider();
    provider.configure(Collections.singletonMap(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG,
        schemaRegistry));
    return provider.parseSchema(schema, references).get();
  }

  @Override
  protected Message readFrom(String jsonString, ParsedSchema schema) {
    try {
      return (Message) ProtobufSchemaUtils.toObject(jsonString, (ProtobufSchema) schema);
    } catch (InvalidProtocolBufferException e) {
      throw new SerializationException(String.format("Error deserializing json %s to Protobuf of "
              + "schema %s",
          jsonString,
          schema
      ), e);
    }
  }

  static class ProtobufMessageSerializer extends AbstractKafkaProtobufSerializer
      implements SchemaMessageSerializer<Message> {

    protected final Serializer keySerializer;

    ProtobufMessageSerializer(
        SchemaRegistryClient schemaRegistryClient, boolean autoRegister, Serializer keySerializer
    ) {
      this.schemaRegistry = schemaRegistryClient;
      this.autoRegisterSchema = autoRegister;
      this.keySerializer = keySerializer;
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
        Message object,
        ParsedSchema schema
    ) {
      return super.serializeImpl(subject, topic, isKey, object, (ProtobufSchema) schema);
    }
  }
}
