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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

public class AdminAssociatedNameStrategyTest {

  private static final AvroSchema SCHEMA = new AvroSchema(
      "{\"type\":\"record\",\"name\":\"com.example.MyRecord\","
          + "\"fields\":[{\"type\":\"string\",\"name\":\"f1\"}]}");

  @Test
  public void testExplicitTopicIdTakesPrecedence() {
    AdminAssociatedNameStrategy strategy = new AdminAssociatedNameStrategy();
    strategy.setSchemaRegistryClient(new MockSchemaRegistryClient());
    Map<String, Object> configs = new HashMap<>();
    configs.put(AssociatedNameStrategy.TOPIC_ID, "explicit-topic-id");
    strategy.configure(configs);

    assertEquals("explicit-topic-id", strategy.resolveTopicId("any-topic"));
  }

  @Test
  public void testNoBootstrapServersThrowsConfigException() {
    AdminAssociatedNameStrategy strategy = new AdminAssociatedNameStrategy();
    strategy.setSchemaRegistryClient(new MockSchemaRegistryClient());
    Map<String, Object> configs = new HashMap<>();
    configs.put(AssociatedNameStrategy.FALLBACK_TYPE, "NONE");
    strategy.configure(configs);

    assertThrows(SerializationException.class,
        () -> strategy.subjectName("my-topic", false, SCHEMA));
  }

  @Test
  public void testExplicitTopicIdBypassesAdminClient() {
    AdminAssociatedNameStrategy strategy = new AdminAssociatedNameStrategy();
    strategy.setSchemaRegistryClient(new MockSchemaRegistryClient());
    Map<String, Object> configs = new HashMap<>();
    configs.put(AssociatedNameStrategy.TOPIC_ID, "explicit-topic-id");
    configs.put("bootstrap.servers", "invalid:9999");
    strategy.configure(configs);

    assertEquals("explicit-topic-id", strategy.resolveTopicId("any-topic"));
  }

  @Test
  public void testAutoDiscoverTopicIdWithMockAdminClient() throws Exception {
    Uuid topicUuid = Uuid.randomUuid();
    AdminClient mockAdminClient = mock(AdminClient.class);
    DescribeTopicsResult mockResult = mock(DescribeTopicsResult.class);
    TopicDescription topicDesc = new TopicDescription(
        "my-topic", false, Collections.emptyList(), Collections.emptySet(), topicUuid);
    when(mockAdminClient.describeTopics(Collections.singletonList("my-topic")))
        .thenReturn(mockResult);
    when(mockResult.allTopicNames())
        .thenReturn(KafkaFuture.completedFuture(
            Collections.singletonMap("my-topic", topicDesc)));

    MockSchemaRegistryClient mockClient = new MockSchemaRegistryClient();
    mockClient.register("associated-subject", SCHEMA);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "my-topic", "my-namespace", topicUuid.toString(), "topic",
        Collections.singletonList(new AssociationCreateOrUpdateInfo(
            "associated-subject", "value", null, null, null, null)));
    mockClient.createAssociation(request);

    AdminAssociatedNameStrategy strategy = new AdminAssociatedNameStrategy() {
      @Override
      protected AdminClient createAdminClient(Map<String, ?> configs) {
        return mockAdminClient;
      }
    };
    strategy.setSchemaRegistryClient(mockClient);
    Map<String, Object> configs = new HashMap<>();
    configs.put("bootstrap.servers", "localhost:9092");
    configs.put(AssociatedNameStrategy.FALLBACK_TYPE, "NONE");
    strategy.configure(configs);

    assertEquals("associated-subject", strategy.subjectName("my-topic", false, SCHEMA));
  }
}
