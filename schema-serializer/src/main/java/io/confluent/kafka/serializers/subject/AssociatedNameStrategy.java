/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.serializers.subject;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SubjectNameStrategy} that will query schema registry for
 * the associated subject name for the topic.  The topic is passed as the resource name
 * to schema registry.  If there is a configuration property named
 * "kafka.cluster.id", then its value will be passed as the resource namespace;
 * otherwise the value "-" will be passed as the resource namespace.
 * If more than subject is returned from the query, an exception will be thrown.
 * If no subjects are returned from the query, then the behavior will fall back
 * to {@link TopicNameStrategy}, unless the configuration property
 * "fallback.subject.name.strategy.type" is set to "RECORD", "TOPIC_RECORD", or "NONE".
 */
public class AssociatedNameStrategy implements SubjectNameStrategy {

  private static final Logger log = LoggerFactory.getLogger(AssociatedNameStrategy.class);

  public static final String KAFKA_CLUSTER_ID = "kafka.cluster.id";
  public static final String NAMESPACE_WILDCARD = "-";
  public static final String FALLBACK_SUBJECT_NAME_STRATEGY_TYPE =
      "fallback.subject.name.strategy.type";
  private static final int DEFAULT_CACHE_CAPACITY = 1000;

  private SchemaRegistryClient client;
  private String kafkaClusterId;
  private SubjectNameStrategy fallbackSubjectNameStrategy = new TopicNameStrategy();
  private LoadingCache<CacheKey, String> subjectNameCache;

  public AssociatedNameStrategy() {
    this.subjectNameCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_CAPACITY)
        .build(new CacheLoader<CacheKey, String>() {
          @Override
          public String load(CacheKey key) throws Exception {
            return loadSubjectName(key.topic, key.isKey, key.schema);
          }
        });
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Object kafkaClusterIdConfig = configs.get(KAFKA_CLUSTER_ID);
    if (kafkaClusterIdConfig != null) {
      this.kafkaClusterId = kafkaClusterIdConfig.toString();
    }
    Object fallbackConfig = configs.get(FALLBACK_SUBJECT_NAME_STRATEGY_TYPE);
    if (fallbackConfig != null) {
      switch (fallbackConfig.toString().toUpperCase()) {
        case "TOPIC":
          this.fallbackSubjectNameStrategy = new TopicNameStrategy();
          break;
        case "RECORD":
          this.fallbackSubjectNameStrategy = new RecordNameStrategy();
          break;
        case "TOPIC_RECORD":
          this.fallbackSubjectNameStrategy = new TopicRecordNameStrategy();
          break;
        case "NONE":
          this.fallbackSubjectNameStrategy = null;
          break;
        default:
          throw new IllegalArgumentException("Invalid value for "
              + FALLBACK_SUBJECT_NAME_STRATEGY_TYPE + ": " + fallbackConfig);
      }
    } else {
      // default is TopicNameStrategy
      this.fallbackSubjectNameStrategy = new TopicNameStrategy();
    }
  }

  @Override
  public void setSchemaRegistryClient(SchemaRegistryClient client) {
    this.client = client;
  }

  @Override
  public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
    if (topic == null) {
      return null;
    }

    if (client == null) {
      if (fallbackSubjectNameStrategy != null) {
        log.warn("Client is not set in AssociatedNameStrategy, perhaps configure() on serde "
            + " was not called, using fallback strategy");
        return fallbackSubjectNameStrategy.subjectName(topic, isKey, schema);
      } else {
        throw new SerializationException("Client is not set in AssociatedNameStrategy, "
            + " perhaps configure() on serde was not called");
      }
    }

    try {
      return subjectNameCache.get(new CacheKey(topic, isKey, schema));
    } catch (Exception e) {
      if (e.getCause() instanceof SerializationException) {
        throw (SerializationException) e.getCause();
      }
      throw new SerializationException(e.getCause());
    }
  }

  private String loadSubjectName(String topic, boolean isKey, ParsedSchema schema)
      throws IOException, RestClientException {
    List<Association> associations;
    try {
      associations = client.getAssociationsByResourceName(
          topic,
          kafkaClusterId != null ? kafkaClusterId : NAMESPACE_WILDCARD,
          "topic",
          Collections.singletonList(isKey ? "key" : "value"),
          null,
          0,
          -1
      );
    } catch (RestClientException e) {
      if (e.getStatus() == 404) {
        if (fallbackSubjectNameStrategy != null) {
          log.warn("Associations endpoint not found (404), using fallback strategy");
          return fallbackSubjectNameStrategy.subjectName(topic, isKey, schema);
        } else {
          throw new SerializationException("No associated subject found for topic " + topic);
        }
      }
      throw e;
    }
    if (associations.size() > 1) {
      throw new SerializationException("Multiple associated subjects found for topic " + topic);
    } else if (associations.size() == 1) {
      return associations.get(0).getSubject();
    } else if (fallbackSubjectNameStrategy != null) {
      return fallbackSubjectNameStrategy.subjectName(topic, isKey, schema);
    } else {
      throw new SerializationException("No associated subject found for topic " + topic);
    }
  }

  /**
   * Cache key that combines topic and isKey values.
   */
  private static class CacheKey {
    private final String topic;
    private final boolean isKey;
    private final ParsedSchema schema;

    CacheKey(String topic, boolean isKey, ParsedSchema schema) {
      this.topic = topic;
      this.isKey = isKey;
      this.schema = schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return isKey == cacheKey.isKey
          && Objects.equals(topic, cacheKey.topic)
          && Objects.equals(schema, cacheKey.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topic, isKey, schema);
    }
  }
}
