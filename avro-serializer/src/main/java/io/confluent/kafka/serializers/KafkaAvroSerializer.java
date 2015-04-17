/**
 * Copyright 2014 Confluent Inc.
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
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class KafkaAvroSerializer extends AbstractKafkaAvroSerializer implements Serializer<Object> {

  private boolean isKey;
  private Class genericAvroDatumWriter;

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaAvroSerializer() {

  }

  public KafkaAvroSerializer(SchemaRegistryClient client,
                             Class datumWriter)
  {
    schemaRegistry = client;
    genericAvroDatumWriter = datumWriter;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    Object url = configs.get(SCHEMA_REGISTRY_URL);
    if (url == null) {
      throw new ConfigException("Missing Schema registry url!");
    }
    Object maxSchemaObject = configs.get(MAX_SCHEMAS_PER_SUBJECT);
    if (maxSchemaObject == null) {
      schemaRegistry = new CachedSchemaRegistryClient(
          (String) url, DEFAULT_MAX_SCHEMAS_PER_SUBJECT);
    } else {
      schemaRegistry = new CachedSchemaRegistryClient(
          (String) url, (Integer) maxSchemaObject);
    }
    Object datumWriterClassName = configs.get(GENERIC_AVRO_DATUM_WRITER_CLASS);
    if (datumWriterClassName == null) {
      genericAvroDatumWriter = GenericDatumWriter.class;
    } else {
      try {
        Class datumWriterImpl = Class.forName((String) datumWriterClassName);
        if(!datumWriterImpl.isInstance(DatumWriter.class)) {
          throw new ConfigException("Avro DatumWriter implementation " + datumWriterImpl.getName() + " is not an instance of " + DatumWriter.class.getName());
        } else {
          genericAvroDatumWriter = datumWriterImpl;
        }
      } catch (ClassNotFoundException e) {
        throw new ConfigException("Avro DatumWriter implementation " + datumWriterClassName + " could not be found");
      }
    }
  }

  @Override
  public byte[] serialize(String topic, Object record) {
    String subject;
    if (isKey) {
      subject = topic + "-key";
    } else {
      subject = topic + "-value";
    }
    return serializeImpl(subject, record);
  }

  @Override
  protected DatumWriter<Object> getDatumWriter(Schema schema, Object object) {
    if (object instanceof SpecificRecord) {
      return new SpecificDatumWriter<Object>(schema);
    } else {
      try {
        DatumWriter<Object> datumWriter = (DatumWriter<Object>) genericAvroDatumWriter.newInstance();
        datumWriter.setSchema(schema);
        return datumWriter;
      } catch (InstantiationException e) {
        throw new SerializationException("Unable to instantiate DataWriter of type " + genericAvroDatumWriter.getName(), e);
      } catch (IllegalAccessException e) {
        throw new SerializationException("Unable to instantiate DataWriter of type " + genericAvroDatumWriter.getName(), e);
      }
    }
  }

  @Override
  public void close() {

  }
}
