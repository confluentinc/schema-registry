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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kafka.utils.VerifiableProperties;

public abstract class AbstractKafkaAvroSerializer extends AbstractKafkaSchemaSerDe {

  private final EncoderFactory encoderFactory = EncoderFactory.get();
  protected boolean autoRegisterSchema;
  protected boolean removeJavaProperties;
  protected boolean useLatestVersion;
  protected boolean latestCompatStrict;
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
    autoRegisterSchema = config.autoRegisterSchema();
    removeJavaProperties =
        config.getBoolean(KafkaAvroSerializerConfig.AVRO_REMOVE_JAVA_PROPS_CONFIG);
    useLatestVersion = config.useLatestVersion();
    latestCompatStrict = config.getLatestCompatibilityStrict();
  }

  protected KafkaAvroSerializerConfig serializerConfig(Map<String, ?> props) {
    return new KafkaAvroSerializerConfig(props);
  }

  protected KafkaAvroSerializerConfig serializerConfig(VerifiableProperties props) {
    return new KafkaAvroSerializerConfig(props.props());
  }

  protected byte[] serializeImpl(
      String subject, Object object, AvroSchema schema)
      throws SerializationException, InvalidConfigurationException {
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
        id = schemaRegistry.register(subject, schema);
      } else if (useLatestVersion) {
        restClientErrorMsg = "Error retrieving latest version of Avro schema";
        schema = (AvroSchema) lookupLatestVersion(subject, schema, latestCompatStrict);
        id = schemaRegistry.getId(subject, schema);
      } else {
        restClientErrorMsg = "Error retrieving Avro schema";
        id = schemaRegistry.getId(subject, schema);
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
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
        DatumWriter<Object> writer;
        writer = datumWriterCache.get(rawSchema, () -> {
          if (value instanceof SpecificRecord) {
            return new SpecificDatumWriter<>(rawSchema);
          } else if (useSchemaReflection) {
            return new ReflectDatumWriter<>(rawSchema);
          } else {
            return new GenericDatumWriter<>(rawSchema);
          }
        });
        writer.write(value, encoder);
        encoder.flush();
      }
      byte[] bytes = out.toByteArray();
      out.close();
      return bytes;
    } catch (ExecutionException ex) {
      throw new SerializationException("Error serializing Avro message", ex.getCause());
    } catch (IOException | RuntimeException e) {
      // avro serialization can throw AvroRuntimeException, NullPointerException,
      // ClassCastException, etc
      throw new SerializationException("Error serializing Avro message", e);
    } catch (RestClientException e) {
      throw toKafkaException(e, restClientErrorMsg + schema);
    }
  }
}
