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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.InterruptedIOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.errors.TimeoutException;

public abstract class AbstractKafkaAvroDeserializer extends AbstractKafkaSchemaSerDe {
  private final DecoderFactory decoderFactory = DecoderFactory.get();
  protected boolean isKey;
  protected boolean useSpecificAvroReader = false;
  protected boolean avroReflectionAllowNull = false;
  protected boolean avroUseLogicalTypeConverters = false;
  private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<>();
  private final LoadingCache<IdentityPair<Schema, Schema>, DatumReader<?>> datumReaderCache;

  public AbstractKafkaAvroDeserializer() {
    CacheLoader<IdentityPair<Schema, Schema>, DatumReader<?>> cacheLoader =
        new CacheLoader<IdentityPair<Schema, Schema>, DatumReader<?>>() {
          @Override
          public DatumReader<?> load(IdentityPair<Schema, Schema> key) {
            Schema writerSchema = key.getKey();
            Schema readerSchema = key.getValue();
            Schema finalReaderSchema = getReaderSchema(writerSchema, readerSchema);
            boolean writerSchemaIsPrimitive =
                AvroSchemaUtils.getPrimitiveSchemas().containsValue(writerSchema);
            if (writerSchemaIsPrimitive) {
              if (avroUseLogicalTypeConverters) {
                return new GenericDatumReader<>(writerSchema, finalReaderSchema,
                    AvroData.getGenericData());
              } else {
                return new GenericDatumReader<>(writerSchema, finalReaderSchema);
              }
            } else if (useSchemaReflection) {
              return new ReflectDatumReader<>(writerSchema, finalReaderSchema);
            } else if (useSpecificAvroReader) {
              return new SpecificDatumReader<>(writerSchema, finalReaderSchema);
            } else {
              if (avroUseLogicalTypeConverters) {
                return new GenericDatumReader<>(writerSchema, finalReaderSchema,
                    AvroData.getGenericData());
              } else {
                return new GenericDatumReader<>(writerSchema, finalReaderSchema);
              }
            }
          }
        };
    datumReaderCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_CAPACITY)
        .build(cacheLoader);
  }

  /**
   * Sets properties for this deserializer without overriding the schema registry client itself.
   * Useful for testing, where a mock client is injected.
   */
  protected void configure(KafkaAvroDeserializerConfig config) {
    configureClientProperties(config, new AvroSchemaProvider());
    useSpecificAvroReader = config
        .getBoolean(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG);
    avroReflectionAllowNull = config
        .getBoolean(KafkaAvroDeserializerConfig.AVRO_REFLECTION_ALLOW_NULL_CONFIG);
    avroUseLogicalTypeConverters = config
            .getBoolean(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG);
  }

  protected KafkaAvroDeserializerConfig deserializerConfig(Map<String, ?> props) {
    return new KafkaAvroDeserializerConfig(props);
  }

  protected KafkaAvroDeserializerConfig deserializerConfig(Properties props) {
    return new KafkaAvroDeserializerConfig(props);
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
    return deserialize(null, isKey, payload, null);
  }

  /**
   * Just like single-parameter version but accepts an Avro schema to use for reading
   *
   * @param payload      serialized data
   * @param readerSchema schema to use for Avro read (optional, enables Avro projection)
   * @return the deserialized object
   */
  protected Object deserialize(byte[] payload, Schema readerSchema) throws SerializationException {
    return deserialize(null, isKey, payload, readerSchema);
  }

  protected Object deserialize(String topic, Boolean isKey, byte[] payload, Schema readerSchema)
          throws SerializationException {
    if (schemaRegistry == null) {
      throw new InvalidConfigurationException(
          "SchemaRegistryClient not found. You need to configure the deserializer "
              + "or use deserializer constructor with SchemaRegistryClient.");
    }
    if (payload == null) {
      return null;
    }

    DeserializationContext context = new DeserializationContext(topic, isKey, payload);
    return context.read(context.schemaFromRegistry().rawSchema(), readerSchema);
  }

  private Integer schemaVersion(String topic,
                                boolean isKey,
                                int id,
                                String subject,
                                AvroSchema schema,
                                Object result) throws IOException, RestClientException {
    Integer version;
    if (isDeprecatedSubjectNameStrategy(isKey)) {
      subject = getSubjectName(topic, isKey, result, schema);
    }
    AvroSchema subjectSchema = (AvroSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
    version = schemaRegistry.getVersion(subject, subjectSchema);
    return version;
  }

  private String subjectName(String topic, boolean isKey, AvroSchema schemaFromRegistry) {
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
   *     {@link org.apache.avro.generic.GenericRecord}, or {@link SpecificRecord}
   */
  protected GenericContainerWithVersion deserializeWithSchemaAndVersion(
      String topic, boolean isKey, byte[] payload)
      throws SerializationException, InvalidConfigurationException {
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
    } catch (InterruptedIOException e) {
      String errorMessage = "Error retrieving Avro "
          + getSchemaType(isKey)
          + " schema version for id "
          + context.getSchemaId();
      throw new TimeoutException(errorMessage, e);
    } catch (IOException e) {
      throw new SerializationException("Error retrieving Avro "
                                      + getSchemaType(isKey)
                                      + " schema version for id "
                                      + context.getSchemaId(), e);
    } catch (RestClientException e) {
      String errorMessage = "Error retrieving Avro "
           + getSchemaType(isKey)
           + " schema version for id "
           + context.getSchemaId();
      throw toKafkaException(e, errorMessage);
    }
  }

  protected DatumReader<?> getDatumReader(Schema writerSchema, Schema readerSchema)
      throws ExecutionException {
    return datumReaderCache.get(new IdentityPair<>(writerSchema, readerSchema));
  }

  /**
   * Normalizes the reader schema, puts the resolved schema into the cache. 
   * <li>
   * <ul>if the reader schema is provided, use the provided one</ul>
   * <ul>if the reader schema is cached for the writer schema full name, use the cached value</ul>
   * <ul>if the writer schema is primitive, use the writer one</ul>
   * <ul>if schema reflection is used, generate one from the class referred by writer schema</ul>
   * <ul>if generated classes are used, query the class referred by writer schema</ul>
   * <ul>otherwise use the writer schema</ul>
   * </li>
   */
  private Schema getReaderSchema(Schema writerSchema, Schema readerSchema) {
    if (readerSchema != null) {
      return readerSchema;
    }
    final boolean shouldSkipReaderSchemaCacheUsage = shouldSkipReaderSchemaCacheUsage(writerSchema);
    if (!shouldSkipReaderSchemaCacheUsage) {
      readerSchema = readerSchemaCache.get(writerSchema.getFullName());
    }
    if (readerSchema != null) {
      return readerSchema;
    }
    boolean writerSchemaIsPrimitive =
        AvroSchemaUtils.getPrimitiveSchemas().containsValue(writerSchema);
    if (writerSchemaIsPrimitive) {
      readerSchema = writerSchema;
    } else if (useSchemaReflection) {
      readerSchema = getReflectionReaderSchema(writerSchema);
      readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
    } else if (useSpecificAvroReader) {
      readerSchema = getSpecificReaderSchema(writerSchema);
      if (!shouldSkipReaderSchemaCacheUsage) {
        readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
      }
    } else {
      readerSchema = writerSchema;
    }
    return readerSchema;
  }

  private boolean shouldSkipReaderSchemaCacheUsage(Schema schema) {
    return useSpecificAvroReader
      && (
        schema.getType() == Type.ARRAY
        || schema.getType() == Type.MAP
        || schema.getType() == Type.UNION
      );
  }

  @SuppressWarnings("unchecked")
  private Schema getSpecificReaderSchema(Schema writerSchema) {
    if (writerSchema.getType() == Type.ARRAY
        || writerSchema.getType() == Type.MAP
        || writerSchema.getType() == Type.UNION) {
      return writerSchema;
    }
    Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);
    if (readerClass == null) {
      throw new SerializationException("Could not find class "
          + writerSchema.getFullName()
          + " specified in writer's schema whilst finding reader's "
          + "schema for a SpecificRecord.");
    }
    try {
      return readerClass.newInstance().getSchema();
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
  }

  private Schema getReflectionReaderSchema(Schema writerSchema) {
    ReflectData reflectData = avroReflectionAllowNull ? ReflectData.AllowNull.get()
        : ReflectData.get();
    Class<?> readerClass = reflectData.getClass(writerSchema);
    if (readerClass == null) {
      throw new SerializationException("Could not find class "
          + writerSchema.getFullName()
          + " specified in writer's schema whilst finding reader's "
          + "schema for a reflected class.");
    }
    return reflectData.getSchema(readerClass);
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
        String subjectName = isKey == null || strategyUsesSchema(isKey)
            ? getContext() : getSubject();
        return (AvroSchema) schemaRegistry.getSchemaBySubjectAndId(subjectName, schemaId);
      } catch (InterruptedIOException e) {
        String errorMessage = "Error retrieving Avro "
            + getSchemaType(isKey)
            + " schema for id "
            + schemaId;
        throw new TimeoutException(errorMessage, e);
      } catch (IOException e) {
        throw new SerializationException("Error retrieving Avro "
                                         + getSchemaType(isKey)
                                         + " schema for id "
                                         + schemaId, e);
      } catch (RestClientException e) {
        String errorMessage = "Error retrieving Avro "
            + getSchemaType(isKey)
            + " schema for id "
            + schemaId;
        throw toKafkaException(e, errorMessage);
      }
    }

    AvroSchema schemaForDeserialize() {
      try {
        return isDeprecatedSubjectNameStrategy(isKey)
            ? AvroSchemaUtils.copyOf(schemaFromRegistry())
            : (AvroSchema) schemaRegistry.getSchemaBySubjectAndId(getSubject(), schemaId);
      } catch (InterruptedIOException e) {
        String errorMessage = "Error retrieving Avro "
            + getSchemaType(isKey)
            + " schema for id "
            + schemaId;
        throw new TimeoutException(errorMessage, e);
      } catch (IOException e) {
        throw new SerializationException("Error retrieving Avro "
                                         + getSchemaType(isKey)
                                         + " schema for id "
                                         + schemaId, e);
      } catch (RestClientException e) {
        String errorMessage =  "Error retrieving Avro "
            + getSchemaType(isKey)
            + " schema for id "
            + schemaId;
        throw toKafkaException(e, errorMessage);
      }
    }

    String getSubject() {
      boolean usesSchema = strategyUsesSchema(isKey);
      return subjectName(topic, isKey, usesSchema ? schemaFromRegistry() : null);
    }

    String getContext() {
      return getContextName(topic);
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
      try {
        DatumReader<?> reader = getDatumReader(writerSchema, readerSchema);
        int length = buffer.limit() - 1 - idSize;
        if (writerSchema.getType().equals(Schema.Type.BYTES)) {
          byte[] bytes = new byte[length];
          buffer.get(bytes, 0, length);
          return bytes;
        } else {
          int start = buffer.position() + buffer.arrayOffset();
          Object result = reader.read(null, decoderFactory.binaryDecoder(buffer.array(),
              start, length, null));
          if (writerSchema.getType().equals(Schema.Type.STRING)) {
            return result.toString();
          } else {
            return result;
          }
        }
      } catch (ExecutionException ex) {
        throw new SerializationException("Error deserializing Avro message for id "
            + schemaId, ex.getCause());
      } catch (IOException | RuntimeException e) {
        // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
        throw new SerializationException("Error deserializing Avro message for id "
            + schemaId, e);
      }
    }
  }

  static class IdentityPair<K, V> {
    private final K key;
    private final V value;

    public IdentityPair(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IdentityPair<?, ?> pair = (IdentityPair<?, ?>) o;
      // Only perform identity check
      return key == pair.key && value == pair.value;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(key) + System.identityHashCode(value);
    }

    @Override
    public String toString() {
      return "IdentityPair{"
          + "key=" + key
          + ", value=" + value
          + '}';
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
