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
 *
 */

package io.confluent.connect.protobuf;

import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Collections;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.kafka.serializers.protobuf.ProtobufSchemaAndValue;


/**
 * Implementation of Converter that uses Protobuf schemas and objects.
 */
public class ProtobufConverter implements Converter {

  private SchemaRegistryClient schemaRegistry;
  private Serializer serializer;
  private Deserializer deserializer;

  private boolean isKey;
  private ProtobufData protobufData;

  public ProtobufConverter() {
  }

  // Public only for testing
  public ProtobufConverter(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    ProtobufConverterConfig protobufConverterConfig = new ProtobufConverterConfig(configs);

    if (schemaRegistry == null) {
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          protobufConverterConfig.getSchemaRegistryUrls(),
          protobufConverterConfig.getMaxSchemasPerSubject(),
          Collections.singletonList(new ProtobufSchemaProvider()),
          configs,
          protobufConverterConfig.requestHeaders()
      );
    }

    serializer = new Serializer(configs, schemaRegistry);
    deserializer = new Deserializer(configs, schemaRegistry);
    protobufData = new ProtobufData(new ProtobufDataConfig(configs));
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    return fromConnectData(topic, null, schema, value);
  }

  @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    try {
      ProtobufSchemaAndValue schemaAndValue = protobufData.fromConnectData(schema, value);
      Object v = schemaAndValue.getValue();
      if (v == null) {
        return null;
      } else if (v instanceof Message) {
        return serializer.serialize(topic,
            isKey,
            headers,
            (Message) v,
            schemaAndValue.getSchema()
        );
      } else {
        throw new DataException("Unsupported object of class " + v.getClass().getName());
      }
    } catch (TimeoutException e) {
      throw new RetriableException(String.format(
          "Failed to serialize Protobuf data from topic %s :",
          topic
      ), e);
    } catch (SerializationException e) {
      throw new DataException(String.format(
          "Failed to serialize Protobuf data from topic %s :",
          topic
      ), e);
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Protobuf data from topic %s : %s", topic, e.getMessage())
      );
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic, null, value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    try {
      ProtobufSchemaAndValue deserialized = deserializer.deserialize(topic, isKey, headers, value);

      if (deserialized == null || deserialized.getValue() == null) {
        return SchemaAndValue.NULL;
      } else {
        Object object = deserialized.getValue();
        if (object instanceof Message) {
          Message message = (Message) object;
          return protobufData.toConnectData(deserialized.getSchema(), message);
        }
        throw new DataException(String.format(
            "Unsupported type %s returned during deserialization of topic %s ",
            object.getClass().getName(),
            topic
        ));
      }
    } catch (TimeoutException e) {
      throw new RetriableException(String.format(
          "Failed to deserialize data for topic %s to Protobuf: ",
          topic
      ), e);
    } catch (SerializationException e) {
      throw new DataException(String.format(
          "Failed to deserialize data for topic %s to Protobuf: ",
          topic
      ), e);
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Protobuf data from topic %s : %s", topic, e.getMessage())
      );
    }
  }

  private static class Serializer extends AbstractKafkaProtobufSerializer {

    public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
      schemaRegistry = client;
      this.autoRegisterSchema = autoRegisterSchema;
    }

    public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client, false);
      configure(new KafkaProtobufSerializerConfig(configs));
    }

    public byte[] serialize(
        String topic, boolean isKey, Headers headers, Message value, ProtobufSchema schema) {
      if (value == null) {
        return null;
      }
      return serializeImpl(getSubjectName(topic, isKey, value, schema),
          topic, isKey, headers, value, schema);
    }
  }

  private static class Deserializer extends AbstractKafkaProtobufDeserializer {

    public Deserializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client);
      configure(new KafkaProtobufDeserializerConfig(configs), null);
    }

    public ProtobufSchemaAndValue deserialize(
        String topic, boolean isKey, Headers headers, byte[] payload) {
      return deserializeWithSchemaAndVersion(topic, isKey, headers, payload);
    }
  }
}
