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
import static org.junit.Assert.assertNull;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.Uuid;
import org.junit.Before;
import org.junit.Test;

public class AssociatedNameStrategyTest {

  private static final AvroSchema SCHEMA = new AvroSchema(
      "{\"type\":\"record\",\"name\":\"com.example.MyRecord\","
          + "\"fields\":[{\"type\":\"string\",\"name\":\"f1\"}]}");

  private AssociatedNameStrategy strategy;

  @Before
  public void setUp() {
    strategy = new AssociatedNameStrategy();
    strategy.setSchemaRegistryClient(new MockSchemaRegistryClient());
    Map<String, Object> configs = new HashMap<>();
    configs.put(AssociatedNameStrategy.FALLBACK_TYPE, "RECORD");
    strategy.configure(configs);
  }

  @Test
  public void testNullSchemaReturnsNull() {
    assertNull(strategy.subjectName("my-topic", false, null));
  }

  @Test
  public void testNonNullSchemaReturnsFallbackSubjectName() {
    assertEquals("com.example.MyRecord", strategy.subjectName("my-topic", false, SCHEMA));
  }

  @Test
  public void testDiscoveredKafkaClusterIdDisambiguates() throws Exception {
    Uuid topicUuid = Uuid.randomUuid();
    Uuid otherTopicUuid = Uuid.randomUuid();

    MockSchemaRegistryClient mockClient = new MockSchemaRegistryClient();
    mockClient.register("correct-subject", SCHEMA);
    mockClient.register("other-subject", SCHEMA);

    // Create two associations for the same topic name in different namespaces
    AssociationCreateOrUpdateRequest request1 = new AssociationCreateOrUpdateRequest(
        "my-topic", "cluster-1", topicUuid.toString(), "topic",
        Collections.singletonList(new AssociationCreateOrUpdateInfo(
            "correct-subject", "value", null, null, null, null)));
    mockClient.createAssociation(request1);

    AssociationCreateOrUpdateRequest request2 = new AssociationCreateOrUpdateRequest(
        "my-topic", "cluster-2", otherTopicUuid.toString(), "topic",
        Collections.singletonList(new AssociationCreateOrUpdateInfo(
            "other-subject", "value", null, null, null, null)));
    mockClient.createAssociation(request2);

    AssociatedNameStrategy strategy = new AssociatedNameStrategy();
    strategy.setSchemaRegistryClient(mockClient);
    Map<String, Object> configs = new HashMap<>();
    configs.put(AssociatedNameStrategy.FALLBACK_TYPE, "NONE");
    strategy.configure(configs);

    // Simulate ClusterResourceListener callback
    strategy.setKafkaClusterId("cluster-1");

    assertEquals("correct-subject", strategy.subjectName("my-topic", false, SCHEMA));
  }

}
