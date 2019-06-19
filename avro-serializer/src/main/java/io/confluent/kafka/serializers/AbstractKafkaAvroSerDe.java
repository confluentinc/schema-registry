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

import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Common fields and helper methods for both the serializer and the deserializer.
 */
public abstract class AbstractKafkaAvroSerDe {

  protected static final byte MAGIC_BYTE = 0x0;
  protected static final int idSize = 4;
  private static final String MOCK_URL_PREFIX = "mock://";

  protected SchemaRegistryClient schemaRegistry;
  protected Object keySubjectNameStrategy = new TopicNameStrategy();
  protected Object valueSubjectNameStrategy = new TopicNameStrategy();


  protected void configureClientProperties(AbstractKafkaAvroSerDeConfig config) {
    try {
      if (null == schemaRegistry) {
        List<String> urls = config.getSchemaRegistryUrls();
        String mockScope = validateAndMaybeGetMockScope(urls);
        if (mockScope != null) {
          schemaRegistry = MockSchemaRegistry.getClientForScope(mockScope);
        } else {
          int maxSchemaObject = config.getMaxSchemasPerSubject();
          Map<String, Object> originals = config.originalsWithPrefix("");
          schemaRegistry = new CachedSchemaRegistryClient(urls, maxSchemaObject, originals);
        }
      }
      keySubjectNameStrategy = config.keySubjectNameStrategy();
      valueSubjectNameStrategy = config.valueSubjectNameStrategy();
    } catch (io.confluent.common.config.ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
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
  protected String getSubjectName(String topic, boolean isKey, Object value, Schema schema) {
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

  public int register(String subject, Schema schema) throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public Schema getById(int id) throws IOException, RestClientException {
    return schemaRegistry.getById(id);
  }

  public Schema getBySubjectAndId(String subject, int id) throws IOException, RestClientException {
    return schemaRegistry.getBySubjectAndId(subject, id);
  }
}
