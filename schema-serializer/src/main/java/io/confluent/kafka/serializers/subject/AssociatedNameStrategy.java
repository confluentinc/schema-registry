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
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.SerializationException;

/**
 * A {@link SubjectNameStrategy} that will query schema registry for
 * the associated subject name for the topic.  The topic is passed as the resource name
 * to schema registry.  If there is a configuration property named
 * "kafka.cluster.id", then its value will be passed as the resource namespace;
 * otherwise the value "-" will be passed as the resource namespace.
 * If more than subject is returned from the query, an exception will be thrown.
 * If no subjects are returned from the query, then the behavior will fall back
 * to {@link TopicNameStrategy}, unless the configuration property
 * "skip.topic.name.strategy.fallback" is set to true, in which case an exception
 * will be thrown.
 */
public class AssociatedNameStrategy implements SubjectNameStrategy {

  public static final String KAFKA_CLUSTER_ID = "kafka.cluster.id";
  public static final String NAMESPACE_WILDCARD = "-";
  public static final String SKIP_TOPIC_NAME_STRATEGY_FALLBACK =
      "skip.topic.name.strategy.fallback";
  private static final int DEFAULT_CACHE_CAPACITY = 1000;

  private SchemaRegistryClient client;
  private String kafkaClusterId;
  private boolean skipTopicNameStrategyFallback;
  private LoadingCache<CacheKey, String> subjectNameCache;

  @Override
  public void configure(Map<String, ?> configs) {
    Object kafkaClusterIdConfig = configs.get(KAFKA_CLUSTER_ID);
    if (kafkaClusterIdConfig != null) {
      this.kafkaClusterId = kafkaClusterIdConfig.toString();
    }
    Object skipConfig = configs.get(SKIP_TOPIC_NAME_STRATEGY_FALLBACK);
    if (skipConfig != null) {
      this.skipTopicNameStrategyFallback = Boolean.parseBoolean(skipConfig.toString());
    }
    this.subjectNameCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_CAPACITY)
        .build(new CacheLoader<CacheKey, String>() {
          @Override
          public String load(CacheKey key) throws Exception {
            return loadSubjectName(key.topic, key.isKey);
          }
        });
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
    
    try {
      return subjectNameCache.get(new CacheKey(topic, isKey));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SerializationException) {
        throw (SerializationException) e.getCause();
      }
      throw new SerializationException(e.getCause());
    }
  }

  private String loadSubjectName(String topic, boolean isKey) 
      throws IOException, RestClientException {
    List<Association> associations = client.getAssociationsByResourceName(
        topic,
        kafkaClusterId != null ? kafkaClusterId : NAMESPACE_WILDCARD,
        "topic",
        Collections.singletonList(isKey ? "key" : "value"),
        null,
        0,
        -1
    );
    if (associations.size() > 1) {
      throw new SerializationException("Multiple associated subjects found for topic " + topic);
    } else if (associations.size() == 1) {
      return associations.get(0).getSubject();
    } else {
      if (skipTopicNameStrategyFallback) {
        throw new SerializationException("No associated subject found for topic " + topic);
      }
      // fall back to TopicNameStrategy
      return isKey ? topic + "-key" : topic + "-value";
    }
  }

  /**
   * Cache key that combines topic and isKey values.
   */
  private static class CacheKey {
    private final String topic;
    private final boolean isKey;

    CacheKey(String topic, boolean isKey) {
      this.topic = topic;
      this.isKey = isKey;
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
      return isKey == cacheKey.isKey && Objects.equals(topic, cacheKey.topic);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topic, isKey);
    }
  }
}
