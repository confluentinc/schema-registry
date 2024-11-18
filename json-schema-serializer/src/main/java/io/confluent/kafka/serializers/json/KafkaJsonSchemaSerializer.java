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


package io.confluent.kafka.serializers.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;

public class KafkaJsonSchemaSerializer<T> extends AbstractKafkaJsonSchemaSerializer<T>
    implements Serializer<T> {

  private static int DEFAULT_CACHE_CAPACITY = 1000;

  private boolean isKey;
  private Map<ObjectNode, JsonSchema> nodeToSchemaCache;
  private Map<Class<?>, JsonSchema> classToSchemaCache;

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaJsonSchemaSerializer() {
    this.nodeToSchemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
    this.classToSchemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  }

  public KafkaJsonSchemaSerializer(SchemaRegistryClient client) {
    this.schemaRegistry = client;
    this.ticker = ticker(client);
    this.nodeToSchemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
    this.classToSchemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  }

  public KafkaJsonSchemaSerializer(SchemaRegistryClient client, Map<String, ?> props) {
    this(client, props, DEFAULT_CACHE_CAPACITY);
  }

  public KafkaJsonSchemaSerializer(SchemaRegistryClient client, Map<String, ?> props,
                                   int cacheCapacity) {
    this.schemaRegistry = client;
    this.ticker = ticker(client);
    configure(serializerConfig(props));
    this.nodeToSchemaCache = new BoundedConcurrentHashMap<>(cacheCapacity);
    this.classToSchemaCache = new BoundedConcurrentHashMap<>(cacheCapacity);
  }

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaJsonSchemaSerializerConfig(config));
  }


  @Override
  public byte[] serialize(String topic, T record) {
    return serialize(topic, null, record);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T record) {
    if (record == null) {
      return null;
    }
    JsonSchema schema;
    if (JsonSchemaUtils.isEnvelope(record)) {
      schema = nodeToSchemaCache.computeIfAbsent(
          JsonSchemaUtils.copyEnvelopeWithoutPayload((ObjectNode) record),
          k -> getSchema(record));
    } else {
      schema = classToSchemaCache.computeIfAbsent(record.getClass(), k -> getSchema(record));
    }
    Object value = JsonSchemaUtils.getValue(record);
    return serializeImpl(
        getSubjectName(topic, isKey, value, schema), topic, headers, (T) value, schema);
  }

  private JsonSchema getSchema(T record) {
    try {
      return JsonSchemaUtils.getSchema(record, specVersion, oneofForNullables,
          failUnknownProperties, objectMapper, schemaRegistry);
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {
    try {
      super.close();
    } catch (IOException e) {
      throw new RuntimeException("Exception while closing serializer", e);
    }
  }
}
