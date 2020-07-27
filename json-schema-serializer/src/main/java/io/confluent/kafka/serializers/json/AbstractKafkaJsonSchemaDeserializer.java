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
 */


package io.confluent.kafka.serializers.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;

public abstract class AbstractKafkaJsonSchemaDeserializer<T> extends AbstractKafkaSchemaSerDe {
  protected ObjectMapper objectMapper = Jackson.newObjectMapper();
  protected Class<T> type;
  protected String typeProperty;
  protected boolean validate;

  /**
   * Sets properties for this deserializer without overriding the schema registry client itself.
   * Useful for testing, where a mock client is injected.
   */
  @SuppressWarnings("unchecked")
  protected void configure(KafkaJsonSchemaDeserializerConfig config, Class<T> type) {
    configureClientProperties(config, new JsonSchemaProvider());
    this.type = type;

    boolean failUnknownProperties =
        config.getBoolean(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES);
    this.objectMapper.configure(
        DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
        failUnknownProperties
    );
    this.validate = config.getBoolean(KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA);
    this.typeProperty = config.getString(KafkaJsonSchemaDeserializerConfig.TYPE_PROPERTY);
  }

  protected KafkaJsonSchemaDeserializerConfig deserializerConfig(Map<String, ?> props) {
    try {
      return new KafkaJsonSchemaDeserializerConfig(props);
    } catch (io.confluent.common.config.ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  protected KafkaJsonSchemaDeserializerConfig deserializerConfig(VerifiableProperties props) {
    return new KafkaJsonSchemaDeserializerConfig(props.props());
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
  ) {

    // Even if the caller requests schema & version, if the payload is null we cannot include it.
    // The caller must handle this case.
    if (payload == null) {
      return null;
    }

    int id = -1;
    try {
      ByteBuffer buffer = getByteBuffer(payload);
      id = buffer.getInt();
      JsonSchema schema = ((JsonSchema) schemaRegistry.getSchemaById(id));
      Schema entireSchema = (Schema)getEntireSchema(id);

      String subject = "";

      if (includeSchemaAndVersion) {
        subject = subjectName(topic, isKey, schema);
        schema = schemaForDeserialize(id, schema, subject, isKey);
      }

      int length = buffer.limit() - 1 - idSize;
      int start = buffer.position() + buffer.arrayOffset();

      String typeName = schema.getString(typeProperty);

      Object value;
      if (typeName != null) {
        value = deriveType(buffer, length, start, typeName);
      } else if (entireSchema instanceof CombinedSchema) {
        value = deriveType(buffer, length, start, (CombinedSchema)entireSchema);
      } else if (type != null) {
        value = objectMapper.readValue(buffer.array(), start, length, type);
      } else {
        value = objectMapper.readTree(new ByteArrayInputStream(buffer.array(), start, length));
      }

      if (validate) {
        try {
          schema.validate(value);
        } catch (JsonProcessingException | ValidationException e) {
          throw new SerializationException("JSON "
              + value
              + " does not match schema "
              + schema.canonicalString(), e);
        }
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
        return new JsonSchemaAndValue(schema.copy(version), value);
      }

      return value;
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error deserializing JSON message for id " + id, e);
    } catch (RestClientException e) {
      throw new SerializationException("Error retrieving JSON schema for id " + id, e);
    }
  }

  private Schema getEntireSchema(int id) throws IOException, RestClientException {
    Collection<String> subjects = schemaRegistry.getAllSubjectsById(id);
    if (subjects == null) {
      return null;
    }

    Optional<String> subjectOpt = subjects.stream().filter(x -> x != null).findAny();
    if (!subjectOpt.isPresent()) {
      return null;
    }

    String subject = subjectOpt.get();

    try {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
      Optional<ParsedSchema> parsedSchemaOpt = schemaRegistry.parseSchema(
              schemaMetadata.getSchemaType(),
              schemaMetadata.getSchema(),
              schemaMetadata.getReferences()
      );

      if (!parsedSchemaOpt.isPresent()) {
        return null;
      }

      return (Schema) parsedSchemaOpt.get().rawSchema();

    } catch (NullPointerException e) {
      return null;
    }
  }

  private Object deriveType(
          ByteBuffer buffer,
          int length,
          int start,
          CombinedSchema combinedSchema) {

    for (Schema subschema : combinedSchema.getSubschemas()) {
      Schema referredSchema = subschema;
      if (subschema instanceof ReferenceSchema) {
        referredSchema = ((ReferenceSchema)subschema).getReferredSchema();
      }

      if (referredSchema instanceof ObjectSchema) {
        ObjectSchema objSchema = (ObjectSchema)referredSchema;
        Map<String, Object> unprocessedProperties = objSchema.getUnprocessedProperties();
        String typeName = (String) unprocessedProperties.getOrDefault(typeProperty, "");

        try {
          return deriveType(buffer, length, start, typeName);
        } catch (SerializationException | IOException e) {
          continue;
        }
      }
    }

    throw new SerializationException(
            "Can not deserialize object using " + combinedSchema.toString());
  }

  private Object deriveType(
      ByteBuffer buffer, int length, int start, String typeName
  ) throws IOException {
    try {
      Class<?> cls = Class.forName(typeName);
      return objectMapper.readValue(buffer.array(), start, length, cls);
    } catch (ClassNotFoundException e) {
      throw new SerializationException("Class " + typeName + " could not be found.");
    }
  }

  private Integer schemaVersion(
      String topic, Boolean isKey, int id, String subject, JsonSchema schema, Object value
  ) throws IOException, RestClientException {
    Integer version;
    if (isDeprecatedSubjectNameStrategy(isKey)) {
      subject = getSubjectName(topic, isKey, value, schema);
      JsonSchema subjectSchema = (JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
      version = schemaRegistry.getVersion(subject, subjectSchema);
    } else {
      //we already got the subject name
      version = schemaRegistry.getVersion(subject, schema);
    }
    return version;
  }

  private String subjectName(String topic, Boolean isKey, JsonSchema schemaFromRegistry) {
    return isDeprecatedSubjectNameStrategy(isKey)
           ? null
           : getSubjectName(topic, isKey, null, schemaFromRegistry);
  }

  private JsonSchema schemaForDeserialize(
      int id, JsonSchema schemaFromRegistry, String subject, Boolean isKey
  ) throws IOException, RestClientException {
    return isDeprecatedSubjectNameStrategy(isKey)
           ? JsonSchemaUtils.copyOf(schemaFromRegistry)
           : (JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
  }

  protected JsonSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, byte[] payload
  ) throws SerializationException {
    return (JsonSchemaAndValue) deserialize(true, topic, isKey, payload);
  }
}
