/*
 * Copyright 2026 Confluent Inc.
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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A subclass of {@link AssociatedNameStrategy} that automatically discovers the topic ID
 * using an {@link AdminClient}, rather than requiring it to be configured explicitly via
 * {@link AssociatedNameStrategy#TOPIC_ID}.
 *
 * <p>If the topic ID is explicitly configured, that value takes precedence.
 * Otherwise, the strategy uses an AdminClient to look up the topic ID per topic name,
 * caching the results. The {@code bootstrap.servers} property must be present in the configs
 * for auto-discovery to work.
 */
public class AdminAssociatedNameStrategy extends AssociatedNameStrategy {

  private static final Logger log =
      LoggerFactory.getLogger(AdminAssociatedNameStrategy.class);

  private static final long DEFAULT_TIMEOUT_SECS = 30;
  private static final int DEFAULT_CACHE_CAPACITY = 1000;

  private final LoadingCache<String, String> topicIdCache = CacheBuilder.newBuilder()
      .maximumSize(DEFAULT_CACHE_CAPACITY)
      .build(new CacheLoader<>() {
        @Override
        public String load(String topic) {
          return discoverTopicId(topic);
        }
      });

  @Override
  protected String resolveTopicId(String topic) {
    // If explicitly configured, use that
    String configured = super.resolveTopicId(topic);
    if (configured != null) {
      return configured;
    }

    try {
      return topicIdCache.get(topic);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ConfigException) {
        throw (ConfigException) e.getCause();
      }
      throw new SerializationException("Failed to resolve topic ID for " + topic, e.getCause());
    }
  }

  private String discoverTopicId(String topic) {
    Map<String, ?> configs = getConfigs();
    if (configs == null) {
      throw new ConfigException("Cannot auto-discover topic ID: configs not available");
    }
    Object bootstrapServers = configs.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    if (bootstrapServers == null) {
      throw new ConfigException(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
          null, "required for auto-discovering topic ID");
    }

    try (AdminClient adminClient = createAdminClient(configs)) {
      Collection<TopicDescription> descriptions = adminClient.describeTopics(
              Collections.singletonList(topic)).allTopicNames()
          .get(DEFAULT_TIMEOUT_SECS, TimeUnit.SECONDS).values();
      if (descriptions.isEmpty()) {
        throw new ConfigException("Topic " + topic
            + " not found, cannot auto-discover topic ID");
      }
      TopicDescription desc = descriptions.iterator().next();
      String topicId = desc.topicId().toString();
      log.info("Auto-discovered topic ID for {}: {}", topic, topicId);
      return topicId;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConfigException("Interrupted while auto-discovering topic ID");
    } catch (java.util.concurrent.ExecutionException e) {
      throw new ConfigException("Failed to auto-discover topic ID: "
          + e.getCause().getMessage());
    } catch (TimeoutException e) {
      throw new ConfigException("Timed out auto-discovering topic ID after "
          + DEFAULT_TIMEOUT_SECS + " seconds");
    } catch (ConfigException e) {
      throw e;
    } catch (Exception e) {
      throw new ConfigException("Failed to create AdminClient for topic ID discovery: "
          + e.getMessage());
    }
  }

  /**
   * Creates an {@link AdminClient} from the provided configs.
   * Override this method to provide a custom or mock AdminClient for testing.
   *
   * @param configs the configuration properties
   * @return an AdminClient instance
   */
  @SuppressWarnings("unchecked")
  protected AdminClient createAdminClient(Map<String, ?> configs) {
    return AdminClient.create((Map<String, Object>) configs);
  }
}
