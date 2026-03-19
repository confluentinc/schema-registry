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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

public class AdminAssociatedNameStrategyTest {

  @Test
  public void testExplicitClusterIdTakesPrecedence() {
    AdminAssociatedNameStrategy strategy = new AdminAssociatedNameStrategy();
    strategy.setSchemaRegistryClient(new MockSchemaRegistryClient());
    Map<String, Object> configs = new HashMap<>();
    configs.put(AssociatedNameStrategy.KAFKA_CLUSTER_ID, "explicit-cluster-id");
    strategy.configure(configs);

    assertEquals("explicit-cluster-id", strategy.getKafkaClusterId());
  }

  @Test
  public void testNoBootstrapServersThrowsConfigException() {
    AdminAssociatedNameStrategy strategy = new AdminAssociatedNameStrategy();
    strategy.setSchemaRegistryClient(new MockSchemaRegistryClient());
    Map<String, Object> configs = new HashMap<>();

    assertThrows(ConfigException.class, () -> strategy.configure(configs));
  }

  @Test
  public void testInvalidBootstrapServersThrowsConfigException() {
    AdminAssociatedNameStrategy strategy = new AdminAssociatedNameStrategy();
    strategy.setSchemaRegistryClient(new MockSchemaRegistryClient());
    Map<String, Object> configs = new HashMap<>();
    configs.put("bootstrap.servers", "invalid:9999");

    assertThrows(ConfigException.class, () -> strategy.configure(configs));
  }

  @Test
  public void testExplicitClusterIdBypassesAdminClient() {
    AdminAssociatedNameStrategy strategy = new AdminAssociatedNameStrategy();
    strategy.setSchemaRegistryClient(new MockSchemaRegistryClient());
    Map<String, Object> configs = new HashMap<>();
    configs.put(AssociatedNameStrategy.KAFKA_CLUSTER_ID, "explicit-cluster-id");
    configs.put("bootstrap.servers", "invalid:9999");
    strategy.configure(configs);

    assertEquals("explicit-cluster-id", strategy.getKafkaClusterId());
  }

  @Test
  public void testAutoDiscoverClusterIdWithMockAdminClient() {
    AdminClient mockAdminClient = mock(AdminClient.class);
    DescribeClusterResult mockResult = mock(DescribeClusterResult.class);
    when(mockAdminClient.describeCluster()).thenReturn(mockResult);
    when(mockAdminClient.describeCluster(any())).thenReturn(mockResult);
    when(mockResult.clusterId()).thenReturn(KafkaFuture.completedFuture("mock-cluster-id"));

    AdminAssociatedNameStrategy strategy = new AdminAssociatedNameStrategy() {
      @Override
      protected AdminClient createAdminClient(Map<String, ?> configs) {
        return mockAdminClient;
      }
    };
    strategy.setSchemaRegistryClient(new MockSchemaRegistryClient());
    Map<String, Object> configs = new HashMap<>();
    configs.put("bootstrap.servers", "localhost:9092");
    strategy.configure(configs);

    assertEquals("mock-cluster-id", strategy.getKafkaClusterId());
  }
}
