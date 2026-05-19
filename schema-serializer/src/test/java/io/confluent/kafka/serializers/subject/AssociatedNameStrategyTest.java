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

import com.google.common.base.Ticker;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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

  @Test
  public void testCacheTtlExpiry() throws Exception {
    AtomicLong nanos = new AtomicLong(0);
    Ticker fakeTicker = new Ticker() {
      @Override
      public long read() {
        return nanos.get();
      }
    };

    Uuid topicUuid = Uuid.randomUuid();
    MockSchemaRegistryClient mockClient = new MockSchemaRegistryClient();
    mockClient.register("subject-v1", SCHEMA);

    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "my-topic", "-", topicUuid.toString(), "topic",
        Collections.singletonList(new AssociationCreateOrUpdateInfo(
            "subject-v1", "value", null, null, null, null)));
    mockClient.createAssociation(request);

    AssociatedNameStrategy strategy = new AssociatedNameStrategy(fakeTicker);
    strategy.setSchemaRegistryClient(mockClient);
    Map<String, Object> configs = new HashMap<>();
    configs.put(AssociatedNameStrategy.CACHE_EXPIRY_SECS, "10");
    configs.put(AssociatedNameStrategy.FALLBACK_TYPE, "NONE");
    strategy.configure(configs);

    // First call should cache "subject-v1"
    assertEquals("subject-v1", strategy.subjectName("my-topic", false, SCHEMA));

    // Update the association to point to a different subject
    mockClient.register("subject-v2", SCHEMA);
    mockClient.deleteAssociations(
        topicUuid.toString(), "topic", Collections.singletonList("value"), false);
    AssociationCreateOrUpdateRequest request2 = new AssociationCreateOrUpdateRequest(
        "my-topic", "-", topicUuid.toString(), "topic",
        Collections.singletonList(new AssociationCreateOrUpdateInfo(
            "subject-v2", "value", null, null, null, null)));
    mockClient.createAssociation(request2);

    // Before TTL expires, should still return cached value
    nanos.set(TimeUnit.SECONDS.toNanos(9));
    assertEquals("subject-v1", strategy.subjectName("my-topic", false, SCHEMA));

    // After TTL expires, should reload and return new value
    nanos.set(TimeUnit.SECONDS.toNanos(11));
    assertEquals("subject-v2", strategy.subjectName("my-topic", false, SCHEMA));
  }

}
