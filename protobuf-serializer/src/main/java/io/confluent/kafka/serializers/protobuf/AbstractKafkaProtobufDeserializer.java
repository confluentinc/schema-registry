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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RulePhase;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import io.confluent.kafka.serializers.schema.id.SchemaIdDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;

public abstract class AbstractKafkaProtobufDeserializer<T extends Message>
    extends AbstractKafkaSchemaSerDe {

  private static final int DEFAULT_CACHE_CAPACITY = 1000;

  protected Class<T> specificProtobufClass;
  protected Method parseMethod;
  protected boolean deriveType;
  private final Map<Pair<String, ProtobufSchema>, ProtobufSchema> schemaCache;

  public AbstractKafkaProtobufDeserializer() {
    schemaCache = new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  }

  /**
   * Sets properties for this deserializer without overriding the schema registry client itself.
   * Useful for testing, where a mock client is injected.
   */
  protected void configure(KafkaProtobufDeserializerConfig config, Class<T> type) {
    configureClientProperties(config, new ProtobufSchemaProvider());
    try {
      this.specificProtobufClass = type;
      if (specificProtobufClass != null && !specificProtobufClass.equals(Object.class)) {
        this.parseMethod = specificProtobufClass.getDeclaredMethod(
            "parseFrom", ByteBuffer.class, ExtensionRegistryLite.class);
      }
      this.deriveType = config.getBoolean(KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG);
    } catch (Exception e) {
      throw new ConfigException("Class " + specificProtobufClass.getCanonicalName()
          + " is not a valid protobuf message class", e);
    }
  }

  protected KafkaProtobufDeserializerConfig deserializerConfig(Map<String, ?> props) {
    try {
      return new KafkaProtobufDeserializerConfig(props);
    } catch (ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  protected KafkaProtobufDeserializerConfig deserializerConfig(Properties props) {
    return new KafkaProtobufDeserializerConfig(props);
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
  protected T deserialize(byte[] payload)
      throws SerializationException, InvalidConfigurationException {
    return (T) deserialize(false, null, isKey, payload);
  }

  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean isKey, byte[] payload
  ) throws SerializationException, InvalidConfigurationException {
    return deserialize(includeSchemaAndVersion, topic, isKey, null, payload);
  }

  // The Object return type is a bit messy, but this is the simplest way to have
  // flexible decoding and not duplicate deserialization code multiple times for different variants.
  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean isKey, Headers headers, byte[] payload
  ) throws SerializationException, InvalidConfigurationException {
    if (schemaRegistry == null) {
      throw new InvalidConfigurationException(
          "SchemaRegistryClient not found. You need to configure the deserializer "
              + "or use deserializer constructor with SchemaRegistryClient.");
    }
    // Even if the caller requests schema & version, if the payload is null we cannot include it.
    // The caller must handle this case.
    if (payload == null) {
      return null;
    }

    SchemaId schemaId = new SchemaId(ProtobufSchema.TYPE);
    try (SchemaIdDeserializer schemaIdDeserializer = schemaIdDeserializer(isKey)) {
      ByteBuffer buffer =
          schemaIdDeserializer.deserialize(topic, isKey, headers, payload, schemaId);
      String subject = isKey == null || strategyUsesSchema(isKey)
          ? getContextName(topic) : subjectName(topic, isKey, null);
      ProtobufSchema schema = (ProtobufSchema) getSchemaBySchemaId(subject, schemaId);
      MessageIndexes indexes = new MessageIndexes(schemaId.getMessageIndexes());
      String name = schema.toMessageName(indexes);
      schema = schemaWithName(schema, name);
      if (isKey != null && strategyUsesSchema(isKey)) {
        subject = subjectName(topic, isKey, schema);
        schema = schemaForDeserialize(schemaId, schema, subject, isKey);
      }
      Object buf = executeRules(
          subject, topic, headers, payload, RulePhase.ENCODING, RuleMode.READ, null,
          schema, buffer
      );
      buffer = buf instanceof byte[] ? ByteBuffer.wrap((byte[]) buf) : (ByteBuffer) buf;

      ProtobufSchema readerSchema = null;
      if (metadata != null) {
        readerSchema = (ProtobufSchema) getLatestWithMetadata(subject).getSchema();
      } else if (useLatestVersion) {
        readerSchema = (ProtobufSchema) lookupLatestVersion(subject, schema, false).getSchema();
      }
      if (readerSchema != null && readerSchema.toDescriptor(name) != null) {
        readerSchema = schemaWithName(readerSchema, name);
      }
      if (includeSchemaAndVersion || readerSchema != null) {
        Integer version = schemaVersion(topic, isKey, schemaId, subject, schema, null);
        schema = schema.copy(version);
        schema = schemaWithName(schema, name);
      }
      List<Migration> migrations = Collections.emptyList();
      if (readerSchema != null) {
        migrations = getMigrations(subject, schema, readerSchema);
      }

      int length = buffer.remaining();
      int start = buffer.position() + buffer.arrayOffset();

      Object message = null;
      if (!migrations.isEmpty()) {
        message = DynamicMessage.parseFrom(schema.toDescriptor(),
            new ByteArrayInputStream(buffer.array(), start, length),
            ProtobufSchema.EXTENSION_REGISTRY);
        message = executeMigrations(migrations, subject, topic, headers, message);
        message = readerSchema.fromJson((JsonNode) message);
      }

      if (readerSchema != null) {
        schema = readerSchema;
      }
      if (schema.ruleSet() != null && schema.ruleSet().hasRules(RulePhase.DOMAIN, RuleMode.READ)) {
        if (message == null) {
          message = DynamicMessage.parseFrom(schema.toDescriptor(),
              new ByteArrayInputStream(buffer.array(), start, length),
              ProtobufSchema.EXTENSION_REGISTRY);
        }
        message = executeRules(
            subject, topic, headers, payload, RuleMode.READ, null, schema, message
        );
      }

      ByteBuffer protobufBytes = buffer;
      if (message != null) {
        protobufBytes = ByteBuffer.wrap(((Message) message).toByteArray());
        length = buffer.limit();
        start = 0;
      }

      Object value;
      if (parseMethod != null) {
        try {
          value = parseMethod.invoke(null, protobufBytes, ProtobufSchema.EXTENSION_REGISTRY);
        } catch (Exception e) {
          throw new ConfigException("Not a valid protobuf builder", e);
        }
      } else if (deriveType) {
        value = deriveType(protobufBytes, schema);
      } else {
        Descriptor descriptor = schema.toDescriptor();
        if (descriptor == null) {
          throw new SerializationException("Could not find descriptor with name " + schema.name());
        }
        value = DynamicMessage.parseFrom(descriptor,
            new ByteArrayInputStream(protobufBytes.array(), start, length),
            ProtobufSchema.EXTENSION_REGISTRY
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

        return new ProtobufSchemaAndValue(schema, value);
      }

      return value;
    } catch (InterruptedIOException e) {
      throw new TimeoutException("Error deserializing Protobuf message for id " + schemaId, e);
    } catch (IOException | RuntimeException e) {
      throw new SerializationException(
          "Error deserializing Protobuf message for id " + schemaId, e);
    } catch (RestClientException e) {
      throw toKafkaException(e, "Error retrieving Protobuf schema for id " + schemaId);
    } finally {
      postOp(payload);
    }
  }

  private ProtobufSchema schemaWithName(ProtobufSchema schema, String name) {
    Pair<String, ProtobufSchema> cacheKey = new Pair<>(name, schema);
    return schemaCache.computeIfAbsent(cacheKey, k -> schema.copy(name));
  }

  private Object deriveType(ByteBuffer buffer, ProtobufSchema schema) {
    String clsName = schema.fullName();
    if (clsName == null) {
      throw new SerializationException("If `derive.type` is true, then either "
          + "`java_outer_classname` or `java_multiple_files = true` must be set "
          + "in the Protobuf schema");
    }
    try {
      Class<?> cls = Class.forName(clsName);
      Method parseMethod = cls.getDeclaredMethod(
          "parseFrom", ByteBuffer.class, ExtensionRegistryLite.class);
      return parseMethod.invoke(null, buffer, ProtobufSchema.EXTENSION_REGISTRY);
    } catch (ClassNotFoundException e) {
      throw new SerializationException("Class " + clsName + " could not be found.");
    } catch (NoSuchMethodException e) {
      throw new SerializationException("Class " + clsName
          + " is not a valid protobuf message class", e);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new SerializationException("Not a valid protobuf builder");
    }
  }

  private Integer schemaVersion(
      String topic, boolean isKey, SchemaId schemaId,
      String subject, ProtobufSchema schema, Object value
  ) throws IOException, RestClientException {
    Integer version = null;
    ProtobufSchema subjectSchema = (ProtobufSchema) getSchemaBySchemaId(subject, schemaId);
    Metadata metadata = subjectSchema.metadata();
    if (metadata != null) {
      version = metadata.getConfluentVersionNumber();
    }
    if (version == null) {
      version = schemaRegistry.getVersion(subject, subjectSchema);
    }
    return version;
  }

  private String subjectName(String topic, boolean isKey, ProtobufSchema schemaFromRegistry) {
    return getSubjectName(topic, isKey, null, schemaFromRegistry);
  }

  private ProtobufSchema schemaForDeserialize(
      SchemaId schemaId, ProtobufSchema schemaFromRegistry, String subject, boolean isKey
  ) throws IOException, RestClientException {
    return (ProtobufSchema) getSchemaBySchemaId(subject, schemaId);
  }

  protected ProtobufSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, Headers headers, byte[] payload
  ) throws SerializationException {
    return (ProtobufSchemaAndValue) deserialize(true, topic, isKey, headers, payload);
  }

  static class Pair<K, V> {
    private final K key;
    private final V value;

    public Pair(K key, V value) {
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
      Pair<?, ?> pair = (Pair<?, ?>) o;
      return Objects.equals(key, pair.key)
          && Objects.equals(value, pair.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }

    @Override
    public String toString() {
      return "Pair{"
          + "key=" + key
          + ", value=" + value
          + '}';
    }
  }
}
