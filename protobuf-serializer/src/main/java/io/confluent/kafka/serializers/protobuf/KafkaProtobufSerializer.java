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

package io.confluent.kafka.serializers.protobuf;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.io.InterruptedIOException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;

public class KafkaProtobufSerializer<T extends Message>
    extends AbstractKafkaProtobufSerializer<T> implements Serializer<T> {

  private static int DEFAULT_CACHE_CAPACITY = 1000;

  private Cache<Descriptor, ProtobufSchema> schemaCache;

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaProtobufSerializer() {
    this.schemaCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_CAPACITY)
        .build();
  }

  public KafkaProtobufSerializer(SchemaRegistryClient client) {
    this.schemaRegistry = client;
    this.ticker = ticker(client);
    this.schemaCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_CAPACITY)
        .build();
  }

  public KafkaProtobufSerializer(SchemaRegistryClient client, Map<String, ?> props) {
    this(client, props, DEFAULT_CACHE_CAPACITY);
  }

  public KafkaProtobufSerializer(SchemaRegistryClient client, Map<String, ?> props,
                                 int cacheCapacity) {
    this.schemaRegistry = client;
    this.ticker = ticker(client);
    configure(serializerConfig(props));
    this.schemaCache = CacheBuilder.newBuilder()
        .maximumSize(cacheCapacity)
        .build();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaProtobufSerializerConfig(configs));
  }

  @Override
  public byte[] serialize(String topic, T data) {
    return serialize(topic, null, data);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T record) {
    if (schemaRegistry == null) {
      throw new InvalidConfigurationException(
          "SchemaRegistryClient not found. You need to configure the serializer "
              + "or use serializer constructor with SchemaRegistryClient.");
    }
    if (record == null) {
      return null;
    }
    ProtobufSchema schema = schemaCache.getIfPresent(record.getDescriptorForType());
    if (schema == null) {
      schema = ProtobufSchemaUtils.getSchema(record);
      try {
        // Ensure dependencies are resolved before caching
        boolean autoRegisterForDeps = autoRegisterSchema && !onlyLookupReferencesBySchema;
        boolean useLatestForDeps = useLatestVersion && !onlyLookupReferencesBySchema;
        schema = resolveDependencies(schemaRegistry, normalizeSchema, autoRegisterForDeps,
            useLatestForDeps, latestCompatStrict, latestVersionsCache(),
            skipKnownTypes, referenceSubjectNameStrategy, topic, isKey, schema);
      } catch (InterruptedIOException e) {
        throw new TimeoutException("Error serializing Protobuf message", e);
      } catch (IOException | RestClientException e) {
        throw new SerializationException("Error serializing Protobuf message", e);
      }
      schemaCache.put(record.getDescriptorForType(), schema);
    }
    return serializeImpl(getSubjectName(topic, isKey, record, schema),
        topic, isKey, headers, record, schema);
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
