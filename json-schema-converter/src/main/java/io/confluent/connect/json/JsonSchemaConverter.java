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

package io.confluent.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Collections;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.JsonSchemaAndValue;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;

/**
 * Implementation of Converter that supports JSON with JSON Schema.
 */
public class JsonSchemaConverter extends AbstractKafkaSchemaSerDe implements Converter {

  private SchemaRegistryClient schemaRegistry;
  private Serializer serializer;
  private Deserializer deserializer;

  private boolean isKey;
  private JsonSchemaData jsonSchemaData;

  public JsonSchemaConverter() {
  }

  @VisibleForTesting
  public JsonSchemaConverter(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    JsonSchemaConverterConfig jsonSchemaConverterConfig = new JsonSchemaConverterConfig(configs);

    if (schemaRegistry == null) {
      schemaRegistry = new CachedSchemaRegistryClient(
          jsonSchemaConverterConfig.getSchemaRegistryUrls(),
          jsonSchemaConverterConfig.getMaxSchemasPerSubject(),
          Collections.singletonList(new JsonSchemaProvider()),
          configs,
          jsonSchemaConverterConfig.requestHeaders()
      );
    }

    serializer = new Serializer(configs, schemaRegistry);
    deserializer = new Deserializer(configs, schemaRegistry);
    jsonSchemaData = new JsonSchemaData(new JsonSchemaDataConfig(configs));
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (schema == null && value == null) {
      return null;
    }
    JsonSchema jsonSchema = jsonSchemaData.fromConnectSchema(schema);
    JsonNode jsonValue = jsonSchemaData.fromConnectData(schema, value);
    try {
      return serializer.serialize(topic, isKey, jsonValue, jsonSchema);
    } catch (SerializationException e) {
      throw new DataException("Converting Kafka Connect data to byte[] failed due to "
          + "serialization error: ",
          e
      );
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    try {
      JsonSchemaAndValue deserialized = deserializer.deserialize(topic, isKey, value);

      if (deserialized == null || deserialized.getValue() == null) {
        return SchemaAndValue.NULL;
      }

      JsonSchema jsonSchema = deserialized.getSchema();
      Schema schema = jsonSchemaData.toConnectSchema(jsonSchema);
      return new SchemaAndValue(schema, jsonSchemaData.toConnectData(schema,
          (JsonNode) deserialized.getValue()));
    } catch (SerializationException e) {
      throw new DataException("Converting byte[] to Kafka Connect data failed due to "
          + "serialization error: ",
          e
      );
    }
  }

  private static class Serializer extends AbstractKafkaJsonSchemaSerializer {

    public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
      schemaRegistry = client;
      this.autoRegisterSchema = autoRegisterSchema;
    }

    public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {

      this(client, false);
      configure(new KafkaJsonSchemaSerializerConfig(configs));
    }

    public byte[] serialize(String topic, boolean isKey, Object value, JsonSchema schema) {
      if (value == null) {
        return null;
      }
      return serializeImpl(getSubjectName(topic, isKey, value, schema), value, schema);
    }
  }

  private static class Deserializer extends AbstractKafkaJsonSchemaDeserializer {

    public Deserializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client);
      configure(new KafkaJsonSchemaDeserializerConfig(configs), null);
    }

    public JsonSchemaAndValue deserialize(String topic, boolean isKey, byte[] payload) {
      return deserializeWithSchemaAndVersion(topic, isKey, payload);
    }
  }
}
