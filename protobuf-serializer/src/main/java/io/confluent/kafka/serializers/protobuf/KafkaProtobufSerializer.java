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

package io.confluent.kafka.serializers.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;

public class KafkaProtobufSerializer<T extends Message>
    extends AbstractKafkaProtobufSerializer<T> implements Serializer<T> {

  private static int DEFAULT_CACHE_CAPACITY = 1000;

  private boolean isKey;
  private Cache<Descriptor, ProtobufSchema> schemaCache;

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaProtobufSerializer() {
    schemaCache = new SynchronizedCache<>(new LRUCache<>(DEFAULT_CACHE_CAPACITY));
  }

  public KafkaProtobufSerializer(SchemaRegistryClient client) {
    schemaRegistry = client;
    schemaCache = new SynchronizedCache<>(new LRUCache<>(DEFAULT_CACHE_CAPACITY));
  }

  public KafkaProtobufSerializer(SchemaRegistryClient client, Map<String, ?> props) {
    this(client, props, DEFAULT_CACHE_CAPACITY);
  }

  public KafkaProtobufSerializer(SchemaRegistryClient client, Map<String, ?> props,
                                 int cacheCapacity) {
    schemaRegistry = client;
    configure(serializerConfig(props));
    schemaCache = new SynchronizedCache<>(new LRUCache<>(cacheCapacity));
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaProtobufSerializerConfig(configs));
  }

  @Override
  public byte[] serialize(String topic, T record) {
    if (record == null) {
      return null;
    }
    ProtobufSchema schema = schemaCache.get(record.getDescriptorForType());
    if (schema == null) {
      schema = ProtobufSchemaUtils.getSchema(record);
      schemaCache.put(record.getDescriptorForType(), schema);
    }
    return serializeImpl(getSubjectName(topic, isKey, record, schema),
        topic, isKey, record, schema);
  }

  @Override
  public void close() {
  }
}
