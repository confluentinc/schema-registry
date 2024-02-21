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

package io.confluent.kafka.serializers;

import org.apache.avro.Schema;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_KEY_TYPE_CONFIG;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_VALUE_TYPE_CONFIG;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAvroDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<Object> {

  private static final Logger log = LoggerFactory.getLogger(KafkaAvroDeserializer.class);

  /**
   * Constructor used by Kafka consumer.
   */
  public KafkaAvroDeserializer() {

  }

  public KafkaAvroDeserializer(SchemaRegistryClient client) {
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  public KafkaAvroDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    this(client, props, false);
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    this.isKey = isKey;
    configure(deserializerConfig(props), null);
  }

  public KafkaAvroDeserializer(SchemaRegistryClient client, Map<String, ?> props, boolean isKey) {
    this.schemaRegistry = client;
    this.ticker = ticker(client);
    this.isKey = isKey;

    final String specificAvroClassLookupKey = isKey
         ? SPECIFIC_AVRO_KEY_TYPE_CONFIG :
           SPECIFIC_AVRO_VALUE_TYPE_CONFIG;

    final KafkaAvroDeserializerConfig config = deserializerConfig(props);

    final Class<?> type = config.getClass(specificAvroClassLookupKey);

    if (type != null && !config.getBoolean(SPECIFIC_AVRO_READER_CONFIG)) {
      if (log.isWarnEnabled()) {
        log.warn(
            String.format(
              "'%s' value of '%s' is ignored because '%s' is false",
              specificAvroClassLookupKey,
              type.getName(),
              SPECIFIC_AVRO_READER_CONFIG
            )
        );
      }
    }

    if (type != null && !SpecificRecord.class.isAssignableFrom(type)) {
      throw new ConfigException(
        String.format("Value '%s' specified for '%s' is not a '%s'",
          type.getName(),
          specificAvroClassLookupKey,
          SpecificRecord.class.getName()
        )
      );
    }

    configure(deserializerConfig(props), type);
  }

  @Override
  public Object deserialize(String topic, byte[] bytes) {
    return deserialize(topic, null, bytes);
  }

  @Override
  public Object deserialize(String topic, Headers headers, byte[] bytes) {
    return deserialize(topic, isKey, headers, bytes, specificAvroReaderSchema);
  }

  /**
   * Pass a reader schema to get an Avro projection
   */
  public Object deserialize(String topic, byte[] bytes, Schema readerSchema) {
    return deserialize(topic, isKey, bytes, readerSchema);
  }

  public Object deserialize(String topic, Headers headers, byte[] bytes, Schema readerSchema) {
    return deserialize(topic, isKey, headers, bytes, readerSchema);
  }

  @Override
  public void close() {
    try {
      super.close();
    } catch (IOException e) {
      throw new RuntimeException("Exception while closing deserializer", e);
    }
  }
}
