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

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.kafka.serializers.context.NullContextNameStrategy;
import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;

/**
 * Common fields and helper methods for both the serializer and the deserializer.
 */
public abstract class AbstractKafkaSchemaSerDe implements Closeable {

  protected static final byte MAGIC_BYTE = 0x0;
  protected static final int idSize = 4;
  protected static final int DEFAULT_CACHE_CAPACITY = 1000;

  protected SchemaRegistryClient schemaRegistry;
  protected ContextNameStrategy contextNameStrategy = new NullContextNameStrategy();
  protected Object keySubjectNameStrategy = new TopicNameStrategy();
  protected Object valueSubjectNameStrategy = new TopicNameStrategy();
  protected Map<SubjectSchema, ParsedSchema> latestVersions =
      new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  protected boolean useSchemaReflection;


  protected void configureClientProperties(
      AbstractKafkaSchemaSerDeConfig config,
      SchemaProvider provider) {
    if (schemaRegistry == null) {
      List<String> urls = config.getSchemaRegistryUrls();
      int maxSchemaObject = config.getMaxSchemasPerSubject();
      Map<String, Object> originals = config.originalsWithPrefix("");
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          urls,
          maxSchemaObject,
          Collections.singletonList(provider),
          originals,
          config.requestHeaders()
      );
    }

    contextNameStrategy = config.contextNameStrategy();
    keySubjectNameStrategy = config.keySubjectNameStrategy();
    valueSubjectNameStrategy = config.valueSubjectNameStrategy();
    useSchemaReflection = config.useSchemaReflection();
  }

  /**
   * Get the subject name for the given topic and value type.
   */
  protected String getSubjectName(String topic, boolean isKey, Object value, ParsedSchema schema) {
    Object subjectNameStrategy = subjectNameStrategy(isKey);
    String subject;
    if (subjectNameStrategy instanceof SubjectNameStrategy) {
      subject = ((SubjectNameStrategy) subjectNameStrategy).subjectName(topic, isKey, schema);
    } else {
      subject = ((io.confluent.kafka.serializers.subject.SubjectNameStrategy) subjectNameStrategy)
          .getSubjectName(topic, isKey, value);
    }
    return getContextName(topic, subject);
  }

  protected String getContextName(String topic) {
    return getContextName(topic, null);
  }

  // Visible for testing
  protected String getContextName(String topic, String subject) {
    String contextName = contextNameStrategy.contextName(topic);
    if (contextName != null) {
      contextName = QualifiedSubject.normalizeContext(contextName);
      return subject != null ? contextName + subject : contextName;
    } else {
      return subject;
    }
  }

  protected boolean strategyUsesSchema(boolean isKey) {
    Object subjectNameStrategy = subjectNameStrategy(isKey);
    if (subjectNameStrategy instanceof SubjectNameStrategy) {
      return ((SubjectNameStrategy) subjectNameStrategy).usesSchema();
    } else {
      return false;
    }
  }

  protected boolean isDeprecatedSubjectNameStrategy(boolean isKey) {
    Object subjectNameStrategy = subjectNameStrategy(isKey);
    return !(
        subjectNameStrategy
            instanceof io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy);
  }

  private Object subjectNameStrategy(boolean isKey) {
    return isKey ? keySubjectNameStrategy : valueSubjectNameStrategy;
  }

  /**
   * Get the subject name used by the old Encoder interface, which relies only on the value type
   * rather than the topic.
   */
  protected String getOldSubjectName(Object value) {
    if (value instanceof GenericContainer) {
      return ((GenericContainer) value).getSchema().getName() + "-value";
    } else {
      throw new SerializationException("Primitive types are not supported yet");
    }
  }

  @Deprecated
  public int register(String subject, Schema schema) throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public int register(String subject, ParsedSchema schema) throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public int register(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema, normalize);
  }

  @Deprecated
  public Schema getById(int id) throws IOException, RestClientException {
    return schemaRegistry.getById(id);
  }

  public ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
    return schemaRegistry.getSchemaById(id);
  }

  @Deprecated
  public Schema getBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    return schemaRegistry.getBySubjectAndId(subject, id);
  }

  public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    return schemaRegistry.getSchemaBySubjectAndId(subject, id);
  }

  protected ParsedSchema lookupSchemaBySubjectAndId(
      String subject, int id, ParsedSchema schema, boolean idCompatStrict)
      throws IOException, RestClientException {
    ParsedSchema lookupSchema = getSchemaBySubjectAndId(subject, id);
    if (idCompatStrict && !lookupSchema.isBackwardCompatible(schema).isEmpty()) {
      throw new IOException("Incompatible schema " + lookupSchema.canonicalString()
          + " with refs " + lookupSchema.references()
          + " of type " + lookupSchema.schemaType()
          + " for schema " + schema.canonicalString()
          + ". Set id.compatibility.strict=false to disable this check");
    }
    return lookupSchema;
  }

  protected ParsedSchema lookupLatestVersion(
      String subject, ParsedSchema schema, boolean latestCompatStrict)
      throws IOException, RestClientException {
    return lookupLatestVersion(schemaRegistry, subject, schema, latestVersions, latestCompatStrict);
  }

  protected static ParsedSchema lookupLatestVersion(
      SchemaRegistryClient schemaRegistry,
      String subject,
      ParsedSchema schema,
      Map<SubjectSchema, ParsedSchema> cache,
      boolean latestCompatStrict)
      throws IOException, RestClientException {
    SubjectSchema ss = new SubjectSchema(subject, schema);
    ParsedSchema latestVersion = null;
    if (cache != null) {
      latestVersion = cache.get(ss);
    }
    if (latestVersion == null) {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
      Optional<ParsedSchema> optSchema =
          schemaRegistry.parseSchema(
              new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                  null, schemaMetadata));
      latestVersion = optSchema.orElseThrow(
          () -> new IOException("Invalid schema " + schemaMetadata.getSchema()
              + " with refs " + schemaMetadata.getReferences()
              + " of type " + schemaMetadata.getSchemaType()));
      // Sanity check by testing latest is backward compatibility with schema
      // Don't test for forward compatibility so unions can be handled properly
      if (latestCompatStrict && !latestVersion.isBackwardCompatible(schema).isEmpty()) {
        throw new IOException("Incompatible schema " + schemaMetadata.getSchema()
            + " with refs " + schemaMetadata.getReferences()
            + " of type " + schemaMetadata.getSchemaType()
            + " for schema " + schema.canonicalString()
            + ". Set latest.compatibility.strict=false to disable this check");
      }
      if (cache != null) {
        cache.put(ss, latestVersion);
      }
    }
    return latestVersion;
  }

  protected ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    return buffer;
  }

  protected static KafkaException toKafkaException(RestClientException e, String errorMessage) {
    if (e.getErrorCode() / 100 == 4 /* Client Error */) {
      return new InvalidConfigurationException(e.getMessage());
    } else {
      return new SerializationException(errorMessage, e);
    }
  }

  @Override
  public void close() throws IOException {
    if (schemaRegistry != null) {
      schemaRegistry.close();
    }
  }

  protected static class SubjectSchema {
    private String subject;
    private ParsedSchema schema;

    public SubjectSchema(String subject, ParsedSchema schema) {
      this.subject = subject;
      this.schema = schema;
    }

    public String getSubject() {
      return subject;
    }

    public ParsedSchema getSchema() {
      return schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubjectSchema that = (SubjectSchema) o;
      return subject.equals(that.subject)
          && schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subject, schema);
    }
  }
}
