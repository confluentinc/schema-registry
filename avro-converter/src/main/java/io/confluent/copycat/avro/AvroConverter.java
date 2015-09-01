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

package io.confluent.copycat.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaAndValue;
import org.apache.kafka.copycat.errors.DataException;
import org.apache.kafka.copycat.storage.Converter;

import java.util.Map;

/**
 * Implementation of Converter that uses Avro schemas and objects.
 */
public class AvroConverter implements Converter {
  private Serializer serializer;
  private Deserializer deserializer;

  private boolean isKey;

  public AvroConverter() {
    serializer = new Serializer();
    deserializer = new Deserializer();
  }

  public AvroConverter(SchemaRegistryClient client) {
    serializer = new Serializer(client);
    deserializer = new Deserializer(client);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
  }

  @Override
  public byte[] fromCopycatData(String topic, Schema schema, Object value) {
    try {
      return serializer.serialize(topic, isKey, AvroData.fromCopycatData(schema, value));
    } catch (SerializationException e) {
      throw new DataException("Failed to serialize Avro data: ", e);
    }
  }

  @Override
  public SchemaAndValue toCopycatData(String topic, byte[] value) {
    try {
      GenericContainer deserialized = deserializer.deserialize(topic, isKey, value);
      if (deserialized instanceof IndexedRecord) {
        return AvroData.toCopycatData(deserialized.getSchema(), deserialized);
      } else if (deserialized instanceof NonRecordContainer) {
        return AvroData.toCopycatData(deserialized.getSchema(), ((NonRecordContainer) deserialized).getValue());
      }
      throw new DataException("Unsupported type returned by deserialization");
    } catch (SerializationException e) {
      throw new DataException("Failed to deserialize data to Avro: ", e);
    }
  }


  private static class Serializer extends AbstractKafkaAvroSerializer {
    public Serializer() {
    }

    public Serializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public byte[] serialize(String topic, boolean isKey, Object value) {
      return serializeImpl(getSubjectName(topic, isKey), value);
    }
  }

  private static class Deserializer extends AbstractKafkaAvroDeserializer {
    public Deserializer() {
    }

    public Deserializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public GenericContainer deserialize(String topic, boolean isKey, byte[] payload) {
      return deserializeWithSchemaAndVersion(topic, isKey, payload);
    }
  }
}
