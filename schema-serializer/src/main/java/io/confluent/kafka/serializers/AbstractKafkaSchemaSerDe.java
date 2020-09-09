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
import java.util.Objects;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;

/**
 * Common fields and helper methods for both the serializer and the deserializer.
 */
public abstract class AbstractKafkaSchemaSerDe {

  protected static final byte MAGIC_BYTE = 0x0;
  protected static final int idSize = 4;
  private static int DEFAULT_CACHE_CAPACITY = 1000;
  private static final String MOCK_URL_PREFIX = "mock://";

  protected SchemaRegistryClient schemaRegistry;
  protected Object keySubjectNameStrategy = new TopicNameStrategy();
  protected Object valueSubjectNameStrategy = new TopicNameStrategy();
  protected Cache<SubjectSchema, ParsedSchema> latestVersions =
      new SynchronizedCache<>(new LRUCache<>(DEFAULT_CACHE_CAPACITY));
  protected boolean useSchemaReflection;


  protected void configureClientProperties(
      AbstractKafkaSchemaSerDeConfig config,
      SchemaProvider provider) {
    List<String> urls = config.getSchemaRegistryUrls();
    int maxSchemaObject = config.getMaxSchemasPerSubject();
    Map<String, Object> originals = config.originalsWithPrefix("");
    if (null == schemaRegistry) {
      String mockScope = validateAndMaybeGetMockScope(urls);
      List<SchemaProvider> providers = Collections.singletonList(provider);
      if (mockScope != null) {
        schemaRegistry = MockSchemaRegistry.getClientForScope(mockScope, providers);
      } else {
        schemaRegistry = new CachedSchemaRegistryClient(
            urls,
            maxSchemaObject,
            providers,
            originals,
            config.requestHeaders()
        );
      }
    }
    keySubjectNameStrategy = config.keySubjectNameStrategy();
    valueSubjectNameStrategy = config.valueSubjectNameStrategy();
    useSchemaReflection = config.useSchemaReflection();
  }

  private static String validateAndMaybeGetMockScope(final List<String> urls) {
    final List<String> mockScopes = new LinkedList<>();
    for (final String url : urls) {
      if (url.startsWith(MOCK_URL_PREFIX)) {
        mockScopes.add(url.substring(MOCK_URL_PREFIX.length()));
      }
    }

    if (mockScopes.isEmpty()) {
      return null;
    } else if (mockScopes.size() > 1) {
      throw new ConfigException(
              "Only one mock scope is permitted for 'schema.registry.url'. Got: " + urls
      );
    } else if (urls.size() > mockScopes.size()) {
      throw new ConfigException(
              "Cannot mix mock and real urls for 'schema.registry.url'. Got: " + urls
      );
    } else {
      return mockScopes.get(0);
    }
  }

  /**
   * Get the subject name for the given topic and value type.
   */
  protected String getSubjectName(String topic, boolean isKey, Object value, ParsedSchema schema) {
    Object subjectNameStrategy = subjectNameStrategy(isKey);
    if (subjectNameStrategy instanceof SubjectNameStrategy) {
      return ((SubjectNameStrategy) subjectNameStrategy).subjectName(topic, isKey, schema);
    } else {
      return ((io.confluent.kafka.serializers.subject.SubjectNameStrategy) subjectNameStrategy)
          .getSubjectName(topic, isKey, value);
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

  protected ParsedSchema lookupLatestVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return lookupLatestVersion(schemaRegistry, subject, schema, latestVersions);
  }

  protected static ParsedSchema lookupLatestVersion(
      SchemaRegistryClient schemaRegistry,
      String subject,
      ParsedSchema schema,
      Cache<SubjectSchema, ParsedSchema> cache)
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
              schemaMetadata.getSchemaType(),
              schemaMetadata.getSchema(),
              schemaMetadata.getReferences());
      latestVersion = optSchema.orElseThrow(
          () -> new IOException("Invalid schema " + schemaMetadata.getSchema()
              + " with refs " + schemaMetadata.getReferences()
              + " of type " + schemaMetadata.getSchemaType()));
      // Sanity check by testing latest is backward compatibility with schema
      // Don't test for forward compatibility so unions can be handled properly
      if (!latestVersion.isBackwardCompatible(schema).isEmpty()) {
        throw new IOException("Incompatible schema " + schemaMetadata.getSchema()
            + " with refs " + schemaMetadata.getReferences()
            + " of type " + schemaMetadata.getSchemaType()
            + " for schema " + schema.canonicalString());
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
