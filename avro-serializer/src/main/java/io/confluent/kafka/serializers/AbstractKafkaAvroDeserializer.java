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
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;

import org.apache.avro.reflect.ReflectDatumReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kafka.utils.VerifiableProperties;

public abstract class AbstractKafkaAvroDeserializer extends AbstractKafkaSchemaSerDe {
  private final DecoderFactory decoderFactory = DecoderFactory.get();
  protected boolean useSpecificAvroReader = false;
  private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<>();

  /**
   * Sets properties for this deserializer without overriding the schema registry client itself.
   * Useful for testing, where a mock client is injected.
   */
  protected void configure(KafkaAvroDeserializerConfig config) {
    configureClientProperties(config, new AvroSchemaProvider());
    useSpecificAvroReader = config
        .getBoolean(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG);
  }

  protected KafkaAvroDeserializerConfig deserializerConfig(Map<String, ?> props) {
    return new KafkaAvroDeserializerConfig(props);
  }

  protected KafkaAvroDeserializerConfig deserializerConfig(VerifiableProperties props) {
    return new KafkaAvroDeserializerConfig(props.props());
  }

  /**
   * Deserializes the payload without including schema information for primitive types, maps, and
   * arrays. Just the resulting deserialized object is returned.
   *
   * <p>This behavior is the norm for Decoders/Deserializers.
   *
   * @param payload serialized data
   * @return the deserialized object
   */
  protected Object deserialize(byte[] payload) throws SerializationException {
    return deserialize(null, null, payload, null);
  }

  /**
   * Just like single-parameter version but accepts an Avro schema to use for reading
   *
   * @param payload      serialized data
   * @param readerSchema schema to use for Avro read (optional, enables Avro projection)
   * @return the deserialized object
   */
  protected Object deserialize(byte[] payload, Schema readerSchema) throws SerializationException {
    return deserialize(null, null, payload, readerSchema);
  }

  protected Object deserialize(String topic, Boolean isKey, byte[] payload, Schema readerSchema)
          throws SerializationException {
    if (payload == null) {
      return null;
    }

    DeserializationContext context = new DeserializationContext(topic, isKey, payload);
    return context.read(context.schemaFromRegistry().rawSchema(), readerSchema);
  }

  private Integer schemaVersion(String topic,
                                Boolean isKey,
                                int id,
                                String subject,
                                AvroSchema schema,
                                Object result) throws IOException, RestClientException {
    Integer version;
    if (isDeprecatedSubjectNameStrategy(isKey)) {
      subject = getSubjectName(topic, isKey, result, schema);
      AvroSchema subjectSchema = (AvroSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
      version = schemaRegistry.getVersion(subject, subjectSchema);
    } else {
      //we already got the subject name
      version = schemaRegistry.getVersion(subject, schema);
    }
    return version;
  }

  private String subjectName(String topic, Boolean isKey, AvroSchema schemaFromRegistry) {
    return isDeprecatedSubjectNameStrategy(isKey)
        ? null
        : getSubjectName(topic, isKey, null, schemaFromRegistry);
  }

  /**
   * Deserializes the payload and includes schema information, with version information from the
   * schema registry embedded in the schema.
   *
   * @param payload the serialized data
   * @return a GenericContainer with the schema and data, either as a {@link NonRecordContainer},
   * {@link org.apache.avro.generic.GenericRecord}, or {@link SpecificRecord}
   */
  protected GenericContainerWithVersion deserializeWithSchemaAndVersion(
      String topic, boolean isKey, byte[] payload)
      throws SerializationException {
    // Even if the caller requests schema & version, if the payload is null we cannot include it.
    // The caller must handle this case.
    if (payload == null) {
      return null;
    }

    // Annotate the schema with the version. Note that we only do this if the schema +
    // version are requested, i.e. in Kafka Connect converters. This is critical because that
    // code *will not* rely on exact schema equality. Regular deserializers *must not* include
    // this information because it would return schemas which are not equivalent.
    //
    // Note, however, that we also do not fill in the connect.version field. This allows the
    // Converter to let a version provided by a Kafka Connect source take priority over the
    // schema registry's ordering (which is implicit by auto-registration time rather than
    // explicit from the Connector).
    DeserializationContext context = new DeserializationContext(topic, isKey, payload);
    AvroSchema schema = context.schemaForDeserialize();
    Object result = context.read(schema.rawSchema(), null);

    try {
      Integer version = schemaVersion(topic, isKey, context.getSchemaId(),
          context.getSubject(), schema, result);
      if (schema.rawSchema().getType().equals(Schema.Type.RECORD)) {
        return new GenericContainerWithVersion((GenericContainer) result, version);
      } else {
        return new GenericContainerWithVersion(new NonRecordContainer(schema.rawSchema(), result),
            version);
      }
    } catch (RestClientException | IOException e) {
      throw new SerializationException("Error retrieving Avro "
                                      + getSchemaType(isKey)
                                      + " schema version for id "
                                      + context.getSchemaId(), e);
    }
  }

  protected DatumReader<?> getDatumReader(Schema writerSchema, Schema readerSchema) {
    boolean writerSchemaIsPrimitive =
        AvroSchemaUtils.getPrimitiveSchemas().values().contains(writerSchema);
    // do not use SpecificDatumReader if writerSchema is a primitive
    if (useSchemaReflection && !writerSchemaIsPrimitive) {
      if (readerSchema == null) {
        throw new SerializationException(
            "Reader schema cannot be null when using Avro schema reflection");
      }
      return new ReflectDatumReader<>(writerSchema, readerSchema);
    } else if (useSpecificAvroReader && !writerSchemaIsPrimitive) {
      if (readerSchema == null) {
        readerSchema = getReaderSchema(writerSchema);
      }
      return new SpecificDatumReader<>(writerSchema, readerSchema);
    } else {
      if (readerSchema == null) {
        return new GenericDatumReader<>(writerSchema);
      }
      return new GenericDatumReader<>(writerSchema, readerSchema);
    }
  }

  @SuppressWarnings("unchecked")
  private Schema getReaderSchema(Schema writerSchema) {
    Schema readerSchema = readerSchemaCache.get(writerSchema.getFullName());
    if (readerSchema == null) {
      Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);
      if (readerClass != null) {
        try {
          readerSchema = readerClass.newInstance().getSchema();
        } catch (InstantiationException e) {
          throw new SerializationException(writerSchema.getFullName()
                                           + " specified by the "
                                           + "writers schema could not be instantiated to "
                                           + "find the readers schema.");
        } catch (IllegalAccessException e) {
          throw new SerializationException(writerSchema.getFullName()
                                           + " specified by the "
                                           + "writers schema is not allowed to be instantiated "
                                           + "to find the readers schema.");
        }
        readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
      } else {
        throw new SerializationException("Could not find class "
                                         + writerSchema.getFullName()
                                         + " specified in writer's schema whilst finding reader's "
                                         + "schema for a SpecificRecord.");
      }
    }
    return readerSchema;
  }

  class DeserializationContext {
    private final String topic;
    private final Boolean isKey;
    private final ByteBuffer buffer;
    private final int schemaId;

    DeserializationContext(final String topic, final Boolean isKey, final byte[] payload) {
      this.topic = topic;
      this.isKey = isKey;
      this.buffer = getByteBuffer(payload);
      this.schemaId = buffer.getInt();
    }

    AvroSchema schemaFromRegistry() {
      try {
        return (AvroSchema) schemaRegistry.getSchemaById(schemaId);
      } catch (RestClientException | IOException e) {
        throw new SerializationException("Error retrieving Avro "
                                         + getSchemaType(isKey)
                                         + " schema for id "
                                         + schemaId, e);
      }
    }

    AvroSchema schemaForDeserialize() {
      try {
        return isDeprecatedSubjectNameStrategy(isKey)
            ? AvroSchemaUtils.copyOf(schemaFromRegistry())
            : (AvroSchema) schemaRegistry.getSchemaBySubjectAndId(getSubject(), schemaId);
      } catch (RestClientException | IOException e) {
        throw new SerializationException("Error retrieving Avro "
                                         + getSchemaType(isKey)
                                         + " schema for id "
                                         + schemaId, e);

      }
    }

    String getSubject() {
      return subjectName(topic, isKey, schemaFromRegistry());
    }

    String getTopic() {
      return topic;
    }

    boolean isKey() {
      return isKey;
    }


    int getSchemaId() {
      return schemaId;
    }

    Object read(Schema writerSchema) {
      return read(writerSchema, null);
    }

    Object read(Schema writerSchema, Schema readerSchema) {
      DatumReader<?> reader = getDatumReader(writerSchema, readerSchema);
      int length = buffer.limit() - 1 - idSize;
      if (writerSchema.getType().equals(Schema.Type.BYTES)) {
        byte[] bytes = new byte[length];
        buffer.get(bytes, 0, length);
        return bytes;
      } else {
        int start = buffer.position() + buffer.arrayOffset();
        try {
          Object result = reader.read(null, decoderFactory.binaryDecoder(buffer.array(),
              start, length, null));
          if (writerSchema.getType().equals(Schema.Type.STRING)) {
            return result.toString();
          } else {
            return result;
          }
        } catch (IOException | RuntimeException e) {
          // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
          throw new SerializationException("Error deserializing Avro message for id "
              + schemaId, e);
        }
      }
    }
  }

  private static String getSchemaType(Boolean isKey) {
    if (isKey == null) {
      return "unknown";
    } else if (isKey) {
      return "key";
    } else {
      return "value";
    }
  }
}
