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

package com.cogent.kafka.connect;

import io.confluent.connect.avro.AvroConverterConfig;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.storage.Converter;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * Implementation of Converter that uses Avro schemas and header-based subject routing.
 * Uses the 'cogent_extraction_avro_subject_name' header to determine schema subject names.
 */
public class HeaderBasedAvroConverter implements Converter {

    private static final String SUBJECT_HEADER_NAME = "cogent_extraction_avro_subject_name";
    private static final String DLQ_SUBJECT_NAME = "__cogent_extraction_avro_subject_name_unavailable__";

    private SchemaRegistryClient schemaRegistry;
    private Serializer serializer;
    private Deserializer deserializer;

    private boolean isKey;
    private AvroData avroData;

    public HeaderBasedAvroConverter() {
    }

    // Public only for testing
    public HeaderBasedAvroConverter(SchemaRegistryClient client) {
        schemaRegistry = client;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        AvroConverterConfig avroConverterConfig = new AvroConverterConfig(configs);

        if (schemaRegistry == null) {
            schemaRegistry = SchemaRegistryClientFactory.newClient(
                    avroConverterConfig.getSchemaRegistryUrls(),
                    avroConverterConfig.getMaxSchemasPerSubject(),
                    Collections.singletonList(new AvroSchemaProvider()),
                    configs,
                    avroConverterConfig.requestHeaders()
            );
        }

        serializer = new Serializer(configs, schemaRegistry);
        deserializer = new Deserializer(configs, schemaRegistry);
        avroData = new AvroData(new AvroDataConfig(configs));
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return fromConnectData(topic, null, schema, value);
    }

      @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    try {
      org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
      return serializer.serialize(
          topic,
          isKey,
          headers,
          avroData.fromConnectData(schema, value),
          new AvroSchema(avroSchema));
    } catch (TimeoutException e) {
      throw new RetriableException(
          String.format("Failed to serialize Avro data from topic %s :", topic),
          e
      );
    } catch (SerializationException e) {
      throw new DataException(
          String.format("Failed to serialize Avro data from topic %s :", topic),
          e
      );
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Avro data from topic %s : %s", topic, e.getMessage())
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
            GenericContainerWithVersion containerWithVersion =
                    deserializer.deserialize(topic, isKey, headers, value);
            if (containerWithVersion == null) {
                return SchemaAndValue.NULL;
            }
            GenericContainer deserialized = containerWithVersion.container();
            Integer version = containerWithVersion.version();
            if (deserialized instanceof IndexedRecord) {
                return avroData.toConnectData(deserialized.getSchema(), deserialized, version);
            } else if (deserialized instanceof NonRecordContainer) {
                return avroData.toConnectData(
                        deserialized.getSchema(), ((NonRecordContainer) deserialized).getValue(), version);
            }
            throw new DataException(
                    String.format("Unsupported type returned during deserialization of topic %s ", topic)
            );
        } catch (TimeoutException e) {
            throw new RetriableException(
                    String.format("Failed to deserialize data for topic %s to Avro: ", topic),
                    e
            );
        } catch (SerializationException e) {
            throw new DataException(
                    String.format("Failed to deserialize data for topic %s to Avro: ", topic),
                    e
            );
        } catch (InvalidConfigurationException e) {
            throw new ConfigException(
                    String.format("Failed to access Avro data from topic %s : %s", topic, e.getMessage())
            );
        }
    }

    private static class Serializer extends AbstractKafkaAvroSerializer {

        public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
            schemaRegistry = client;
            this.autoRegisterSchema = autoRegisterSchema;
        }

        public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {
            this(client, false);
            configure(new KafkaAvroSerializerConfig(configs));
        }

        public byte[] serialize(
                String topic, boolean isKey, Headers headers, Object value, AvroSchema schema) {
            if (value == null) {
                return null;
            }
            return serializeImpl(
                    getSubjectName(topic, isKey, value, schema),
                    topic,
                    headers,
                    value,
                    schema);
        }
    }

    private static class Deserializer extends AbstractKafkaAvroDeserializer {

        private static final ThreadLocal<Headers> currentHeaders = new ThreadLocal<>();

        public Deserializer(SchemaRegistryClient client) {
            schemaRegistry = client;
        }

        public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
            this(client);
            configure(new KafkaAvroDeserializerConfig(configs));
        }

        public GenericContainerWithVersion deserialize(
                String topic, boolean isKey, Headers headers, byte[] payload) {
            currentHeaders.set(headers);
            try {
                return deserializeWithSchemaAndVersion(topic, isKey, headers, payload);
            } finally {
                currentHeaders.remove();
            }
        }

        @Override
        protected String getSubjectName(String topic, boolean isKey, Object value, ParsedSchema schema) {
            // Only check headers for value deserialization (not keys)
            if (!isKey) {
                Headers headers = currentHeaders.get();
                if (headers != null) {
                    Header subjectHeader = headers.lastHeader(SUBJECT_HEADER_NAME);
                    if (subjectHeader != null) {
                        String customSubject = new String(subjectHeader.value(), StandardCharsets.UTF_8).trim();
                        if (!customSubject.isEmpty()) {
                            return customSubject;
                        }
                    }
                }
                
                // If header is not present or empty, return DLQ subject name
                return DLQ_SUBJECT_NAME;
            }

            // For keys, use the default subject name strategy
            return super.getSubjectName(topic, isKey, value, schema);
        }
    }
} 