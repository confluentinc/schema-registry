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

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A subclass of {@link AssociatedNameStrategy} that automatically discovers the Kafka cluster ID
 * using an {@link AdminClient}, rather than requiring it to be configured explicitly via
 * {@link AssociatedNameStrategy#KAFKA_CLUSTER_ID}.
 *
 * <p>If the cluster ID is explicitly configured, that value takes precedence.
 * Otherwise, the strategy creates an AdminClient from the provided configs to discover
 * the cluster ID. The {@code bootstrap.servers} property must be present in the configs
 * for auto-discovery to work.
 */
public class AdminAssociatedNameStrategy extends AssociatedNameStrategy {

  private static final Logger log =
      LoggerFactory.getLogger(AdminAssociatedNameStrategy.class);

  private static final long DEFAULT_TIMEOUT_SECS = 30;

  @Override
  public String getKafkaClusterId() {
    // If explicitly configured, use that
    String configured = super.getKafkaClusterId();
    if (configured != null) {
      return configured;
    }

    // Auto-discover from Kafka
    Map<String, ?> configs = getConfigs();
    if (configs == null) {
      throw new ConfigException("Cannot auto-discover Kafka cluster ID: configs not available");
    }
    Object bootstrapServers = configs.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    if (bootstrapServers == null) {
      throw new ConfigException(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
          null, "required for auto-discovering Kafka cluster ID");
    }

    try (AdminClient adminClient = createAdminClient(configs)) {
      String clusterId = adminClient.describeCluster().clusterId()
          .get(DEFAULT_TIMEOUT_SECS, TimeUnit.SECONDS);
      log.info("Auto-discovered Kafka cluster ID: {}", clusterId);
      return clusterId;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConfigException("Interrupted while auto-discovering Kafka cluster ID");
    } catch (ExecutionException e) {
      throw new ConfigException("Failed to auto-discover Kafka cluster ID: "
          + e.getCause().getMessage());
    } catch (TimeoutException e) {
      throw new ConfigException("Timed out auto-discovering Kafka cluster ID after "
          + DEFAULT_TIMEOUT_SECS + " seconds");
    } catch (Exception e) {
      throw new ConfigException("Failed to create AdminClient for cluster ID discovery: "
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
