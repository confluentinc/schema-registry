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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;

public abstract class AbstractKafkaAvroSerializer extends AbstractKafkaSchemaSerDe {

  private final EncoderFactory encoderFactory = EncoderFactory.get();
  protected boolean normalizeSchema;
  protected boolean autoRegisterSchema;
  protected boolean removeJavaProperties;
  protected int useSchemaId = -1;
  protected boolean idCompatStrict;
  protected boolean useLatestVersion;
  protected boolean latestCompatStrict;
  protected boolean avroReflectionAllowNull = false;
  protected boolean avroUseLogicalTypeConverters = false;
  private final Cache<Schema, DatumWriter<Object>> datumWriterCache;

  public AbstractKafkaAvroSerializer() {
    // use identity (==) comparison for keys
    datumWriterCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_CAPACITY)
        .weakKeys()
        .build();

  }

  protected void configure(KafkaAvroSerializerConfig config) {
    configureClientProperties(config, new AvroSchemaProvider());
    normalizeSchema = config.normalizeSchema();
    autoRegisterSchema = config.autoRegisterSchema();
    removeJavaProperties =
        config.getBoolean(KafkaAvroSerializerConfig.AVRO_REMOVE_JAVA_PROPS_CONFIG);
    useSchemaId = config.useSchemaId();
    idCompatStrict = config.getIdCompatibilityStrict();
    useLatestVersion = config.useLatestVersion();
    latestCompatStrict = config.getLatestCompatibilityStrict();
    avroReflectionAllowNull = config
        .getBoolean(KafkaAvroSerializerConfig.AVRO_REFLECTION_ALLOW_NULL_CONFIG);
    avroUseLogicalTypeConverters = config
            .getBoolean(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG);
  }

  protected KafkaAvroSerializerConfig serializerConfig(Map<String, ?> props) {
    return new KafkaAvroSerializerConfig(props);
  }

  protected KafkaAvroSerializerConfig serializerConfig(Properties props) {
    return new KafkaAvroSerializerConfig(props);
  }

  protected DatumWriter<?> getDatumWriter(Object value, Schema schema) {
    if (value instanceof SpecificRecord) {
      return new SpecificDatumWriter<>(schema);
    } else if (useSchemaReflection) {
      return new ReflectDatumWriter<>(schema);
    } else {
      if (avroUseLogicalTypeConverters) {
        return new GenericDatumWriter<>(schema, AvroData.getGenericData());
      } else {
        return new GenericDatumWriter<>(schema);
      }
    }
  }

  protected byte[] serializeImpl(
      String subject, Object object, AvroSchema schema)
      throws SerializationException, InvalidConfigurationException {
    if (schemaRegistry == null) {
      StringBuilder userFriendlyMsgBuilder = new StringBuilder();
      userFriendlyMsgBuilder.append("You must configure() before serialize()");
      userFriendlyMsgBuilder.append(" or use serializer constructor with SchemaRegistryClient");
      throw new InvalidConfigurationException(userFriendlyMsgBuilder.toString());
    }
    // null needs to treated specially since the client most likely just wants to send
    // an individual null value instead of making the subject a null type. Also, null in
    // Kafka has a special meaning for deletion in a topic with the compact retention policy.
    // Therefore, we will bypass schema registration and return a null value in Kafka, instead
    // of an Avro encoded null.
    if (object == null) {
      return null;
    }
    String restClientErrorMsg = "";
    try {
      int id;
      if (autoRegisterSchema) {
        restClientErrorMsg = "Error registering Avro schema";
        id = schemaRegistry.register(subject, schema, normalizeSchema);
      } else if (useSchemaId >= 0) {
        restClientErrorMsg = "Error retrieving schema ID";
        schema = (AvroSchema)
            lookupSchemaBySubjectAndId(subject, useSchemaId, schema, idCompatStrict);
        id = schemaRegistry.getId(subject, schema);
      } else if (useLatestVersion) {
        restClientErrorMsg = "Error retrieving latest version of Avro schema";
        schema = (AvroSchema) lookupLatestVersion(subject, schema, latestCompatStrict);
        id = schemaRegistry.getId(subject, schema);
      } else {
        restClientErrorMsg = "Error retrieving Avro schema";
        id = schemaRegistry.getId(subject, schema, normalizeSchema);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE);
      out.write(ByteBuffer.allocate(idSize).putInt(id).array());
      Object value = object instanceof NonRecordContainer
          ? ((NonRecordContainer) object).getValue()
          : object;
      Schema rawSchema = schema.rawSchema();
      if (rawSchema.getType().equals(Type.BYTES)) {
        if (value instanceof byte[]) {
          out.write((byte[]) value);
        } else if (value instanceof ByteBuffer) {
          out.write(((ByteBuffer) value).array());
        } else {
          throw new SerializationException(
              "Unrecognized bytes object of type: " + value.getClass().getName());
        }
      } else {
        writeDatum(out, value, rawSchema);
      }
      byte[] bytes = out.toByteArray();
      out.close();
      return bytes;
    } catch (ExecutionException ex) {
      throw new SerializationException("Error serializing Avro message", ex.getCause());
    } catch (InterruptedIOException e) {
      throw new TimeoutException("Error serializing Avro message", e);
    } catch (IOException | RuntimeException e) {
      // avro serialization can throw AvroRuntimeException, NullPointerException,
      // ClassCastException, etc
      throw new SerializationException("Error serializing Avro message", e);
    } catch (RestClientException e) {
      throw toKafkaException(e, restClientErrorMsg + schema);
    }
  }

  private void writeDatum(ByteArrayOutputStream out, Object value, Schema rawSchema)
          throws ExecutionException, IOException {
    BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);

    DatumWriter<Object> writer;
    writer = datumWriterCache.get(rawSchema,
        () -> (DatumWriter<Object>) getDatumWriter(value, rawSchema)
    );
    writer.write(value, encoder);
    encoder.flush();
  }
}
