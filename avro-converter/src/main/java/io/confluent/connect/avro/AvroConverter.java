/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.connect.avro;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.NonRecordContainer;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

/**
 * Implementation of Converter that uses Avro schemas and objects.
 */
public class AvroConverter implements Converter {

  private SchemaRegistryClient schemaRegistry;
  private Serializer serializer;
  private Deserializer deserializer;

  private boolean isKey;
  private AvroData avroData;

  public AvroConverter() {
  }

  // Public only for testing
  public AvroConverter(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;

    AvroConverterConfig avroConverterConfig = new AvroConverterConfig(configs);

    if (schemaRegistry == null) {
      schemaRegistry =
          new CachedSchemaRegistryClient(avroConverterConfig.getSchemaRegistryUrls(),
                                         avroConverterConfig.getMaxSchemasPerSubject());
    }

    serializer = new Serializer(schemaRegistry);
    deserializer = new Deserializer(schemaRegistry);
    avroData = new AvroData(new AvroDataConfig(configs));
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    try {
      return serializer.serialize(topic, isKey, avroData.fromConnectData(schema, value));
    } catch (SerializationException e) {
      throw new DataException("Failed to serialize Avro data from topic %s :".format(topic), e);
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    try {
      GenericContainer deserialized = deserializer.deserialize(topic, isKey, value);
      if (deserialized == null) {
        return SchemaAndValue.NULL;
      } else if (deserialized instanceof IndexedRecord) {
        return avroData.toConnectData(deserialized.getSchema(), deserialized);
      } else if (deserialized instanceof NonRecordContainer) {
        return avroData.toConnectData(deserialized.getSchema(), ((NonRecordContainer) deserialized)
            .getValue());
      }
      throw new DataException("Unsupported type returned during deserialization of topic %s "
                                  .format(topic));
    } catch (SerializationException e) {
      throw new DataException("Failed to deserialize data for topic %s to Avro: "
                                  .format(topic), e);
    }
  }


  private static class Serializer extends AbstractKafkaAvroSerializer {

    public Serializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public byte[] serialize(String topic, boolean isKey, Object value) {
      return serializeImpl(getSubjectName(topic, isKey), value);
    }
  }

  private static class Deserializer extends AbstractKafkaAvroDeserializer {

    public Deserializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public GenericContainer deserialize(String topic, boolean isKey, byte[] payload) {
      return deserializeWithSchemaAndVersion(topic, isKey, payload);
    }
  }
}
