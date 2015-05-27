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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kafka.utils.VerifiableProperties;

public abstract class AbstractKafkaAvroDeserializer extends AbstractKafkaAvroSerDe {
  private final DecoderFactory decoderFactory = DecoderFactory.get();
  protected boolean useSpecificAvroReader = false;
  private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<String, Schema>();

  protected void configure(KafkaAvroDeserializerConfig config) {
    try {
      String url = config.getString(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG);
      int  maxSchemaObject = config
          .getInt(KafkaAvroDeserializerConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG);
      schemaRegistry = CachedSchemaRegistryClient.newInstance(
          url, maxSchemaObject);
      configureNonClientProperties(config);
    } catch (io.confluent.common.config.ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  /**
   * Sets properties for this deserializer without overriding the schema registry client itself.
   * Useful for testing, where a mock client is injected.
   */
  protected void configureNonClientProperties(KafkaAvroDeserializerConfig config) {
    useSpecificAvroReader = config
        .getBoolean(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG);
  }

  protected KafkaAvroDeserializerConfig deserializerConfig(Map<String, ?> props) {
    try {
      return new KafkaAvroDeserializerConfig(props);
    } catch (io.confluent.common.config.ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  protected KafkaAvroDeserializerConfig deserializerConfig(VerifiableProperties props) {
    try {
      return new KafkaAvroDeserializerConfig(props.props());
    } catch (io.confluent.common.config.ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  private ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    return buffer;
  }

  protected Object deserialize(byte[] payload) throws SerializationException {
    int id = -1;
    if (payload == null) {
      return null;
    }
    try {
      ByteBuffer buffer = getByteBuffer(payload);
      id = buffer.getInt();
      Schema schema = schemaRegistry.getByID(id);
      int length = buffer.limit() - 1 - idSize;
      if (schema.getType().equals(Schema.Type.BYTES)) {
        byte[] bytes = new byte[length];
        buffer.get(bytes, 0, length);
        return bytes;
      }
      int start = buffer.position() + buffer.arrayOffset();
      DatumReader reader = getDatumReader(schema);
      Object object =
          reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));

      if (schema.getType().equals(Schema.Type.STRING)) {
        object = ((Utf8) object).toString();
      }
      return object;
    } catch (IOException e) {
      throw new SerializationException("Error deserializing Avro message for id " + id, e);
    } catch (RestClientException e) {
      throw new SerializationException("Error retrieving Avro schema for id " + id, e);
    } catch (RuntimeException e) {
      // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
      throw new SerializationException("Error deserializing Avro message for id " + id, e);
    }
  }

  private DatumReader getDatumReader(Schema writerSchema) {
    if (useSpecificAvroReader) {
      return new SpecificDatumReader(writerSchema, getReaderSchema(writerSchema));
    } else {
      return new GenericDatumReader(writerSchema);
    }
  }

  private Schema getReaderSchema(Schema writerSchema) {
    Schema readerSchema = readerSchemaCache.get(writerSchema.getFullName());
    if (readerSchema == null) {
      Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);
      if (readerClass != null) {
        try {
          readerSchema = readerClass.newInstance().getSchema();
        } catch (InstantiationException e) {
          throw new SerializationException(writerSchema.getFullName() + " specified by the " +
            "writers schema could not be instantiated to find the readers schema.");
        } catch (IllegalAccessException e) {
          throw new SerializationException(writerSchema.getFullName() + " specified by the " +
            "writers schema is not allowed to be instantiated to find the readers schema.");
        }
        readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
      } else {
        throw new SerializationException("Could not find class " +  writerSchema.getFullName() +
          " specified in writer's schema whilst finding reader's schema for a SpecificRecord.");
      }
    }
    return readerSchema;
  }
}
