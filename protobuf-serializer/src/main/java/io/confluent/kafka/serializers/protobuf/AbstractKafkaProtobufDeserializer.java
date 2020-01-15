/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package io.confluent.kafka.serializers.protobuf;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MessageLite;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;

public abstract class AbstractKafkaProtobufDeserializer<T extends MessageLite>
    extends AbstractKafkaSchemaSerDe {
  protected Class<T> specificProtobufClass;
  protected Method parseMethod;

  /**
   * Sets properties for this deserializer without overriding the schema registry client itself.
   * Useful for testing, where a mock client is injected.
   */
  @SuppressWarnings("unchecked")
  protected void configure(KafkaProtobufDeserializerConfig config) {
    configureClientProperties(config, new ProtobufSchemaProvider());
    try {
      this.specificProtobufClass = (Class<T>) config.getClass(
          KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_CLASS_CONFIG);
      if (specificProtobufClass != null && !specificProtobufClass.equals(Object.class)) {
        this.parseMethod = specificProtobufClass.getDeclaredMethod("parseFrom", ByteBuffer.class);
      }
    } catch (Exception e) {
      throw new ConfigException("Proto class "
          + specificProtobufClass.getCanonicalName()
          + " is not a valid proto3 message class", e);
    }
  }

  protected KafkaProtobufDeserializerConfig deserializerConfig(Map<String, ?> props) {
    try {
      return new KafkaProtobufDeserializerConfig(props);
    } catch (io.confluent.common.config.ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  protected KafkaProtobufDeserializerConfig deserializerConfig(VerifiableProperties props) {
    return new KafkaProtobufDeserializerConfig(props.props());
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
  protected T deserialize(byte[] payload) throws SerializationException {
    return (T) deserialize(false, null, null, payload);
  }

  // The Object return type is a bit messy, but this is the simplest way to have
  // flexible decoding and not duplicate deserialization code multiple times for different variants.
  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean isKey, byte[] payload
  ) throws SerializationException {

    // Even if the caller requests schema & version, if the payload is null we cannot include it.
    // The caller must handle this case.
    if (payload == null) {
      return null;
    }

    int id = -1;
    try {
      ByteBuffer buffer = getByteBuffer(payload);
      id = buffer.getInt();
      ProtobufSchema schema = ((ProtobufSchema) schemaRegistry.getSchemaById(id));
      String subject = null;
      if (includeSchemaAndVersion) {
        subject = subjectName(topic, isKey, schema);
        schema = schemaForDeserialize(id, schema, subject, isKey);
      }

      int length = buffer.limit() - 1 - idSize;
      int start = buffer.position() + buffer.arrayOffset();

      Object value;
      if (parseMethod != null) {
        try {
          value = parseMethod.invoke(null, buffer);
        } catch (Exception e) {
          throw new ConfigException("Not a valid proto3 builder", e);
        }
      } else {
        value = DynamicMessage.parseFrom(schema.toDescriptor(),
            new ByteArrayInputStream(buffer.array(), start, length)
        );
      }

      if (includeSchemaAndVersion) {
        // Annotate the schema with the version. Note that we only do this if the schema +
        // version are requested, i.e. in Kafka Connect converters. This is critical because that
        // code *will not* rely on exact schema equality. Regular deserializers *must not* include
        // this information because it would return schemas which are not equivalent.
        //
        // Note, however, that we also do not fill in the connect.version field. This allows the
        // Converter to let a version provided by a Kafka Connect source take priority over the
        // schema registry's ordering (which is implicit by auto-registration time rather than
        // explicit from the Connector).

        Integer version = schemaVersion(topic, isKey, id, subject, schema, value);
        return new ProtobufSchemaAndValue(
            new ProtobufSchema(schema.canonicalString(),
                schema.references(),
                schema.resolvedReferences(),
                version
            ),
            value
        );
      }

      return value;
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error deserializing Protobuf message for id " + id, e);
    } catch (RestClientException e) {
      throw new SerializationException("Error retrieving Protobuf schema for id " + id, e);
    }
  }

  private Integer schemaVersion(
      String topic, Boolean isKey, int id, String subject, ProtobufSchema schema, Object value
  ) throws IOException, RestClientException {
    Integer version;
    if (isDeprecatedSubjectNameStrategy(isKey)) {
      subject = getSubjectName(topic, isKey, value, schema);
      ProtobufSchema subjectSchema =
          (ProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(subject,
          id
      );
      version = schemaRegistry.getVersion(subject, subjectSchema);
    } else {
      //we already got the subject name
      version = schemaRegistry.getVersion(subject, schema);
    }
    return version;
  }

  private String subjectName(String topic, Boolean isKey, ProtobufSchema schemaFromRegistry) {
    return isDeprecatedSubjectNameStrategy(isKey)
           ? null
           : getSubjectName(topic, isKey, null, schemaFromRegistry);
  }

  private ProtobufSchema schemaForDeserialize(
      int id, ProtobufSchema schemaFromRegistry, String subject, Boolean isKey
  ) throws IOException, RestClientException {
    return isDeprecatedSubjectNameStrategy(isKey)
           ? ProtobufSchemaUtils.copyOf(schemaFromRegistry)
           : (ProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
  }

  protected ProtobufSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, byte[] payload
  ) throws SerializationException {
    return (ProtobufSchemaAndValue) deserialize(true, topic, isKey, payload);
  }
}
