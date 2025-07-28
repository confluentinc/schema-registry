/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.leaderelector.kafka;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SchemaRegistryCoordinatorTest {

  private static final String LEADER_ID = "leader";
  private static final String MEMBER_ID = "member";
  private static final String LEADER_HOST = "leaderHost";
  private static final int LEADER_PORT = 8083;

  private static final SchemaRegistryIdentity LEADER_INFO = new SchemaRegistryIdentity(
      LEADER_HOST,
      LEADER_PORT,
      true,
      SchemaRegistryConfig.HTTP
  );
  private static final SchemaRegistryIdentity INELIGIBLE_LEADER_INFO = new SchemaRegistryIdentity(
      LEADER_HOST,
      LEADER_PORT,
      false,
      SchemaRegistryConfig.HTTP
  );

  private String groupId = "test-group";
  private int sessionTimeoutMs = 10;
  private int rebalanceTimeoutMs = 60;
  private int heartbeatIntervalMs = 2;
  private long retryBackoffMs = 100;
  private MockTime time;
  private MockClient client;
  private Cluster cluster = TestUtils.singletonCluster("topic", 1);
  private Node node = cluster.nodes().get(0);
  private Metadata metadata;
  private Metrics metrics;
  private ConsumerNetworkClient consumerClient;
  private MockRebalanceListener rebalanceListener;
  private SchemaRegistryCoordinator coordinator;

  @Before
  public void setup() {
    this.time = new MockTime();
    this.metadata = new Metadata(0, Long.MAX_VALUE, new LogContext(), new ClusterResourceListeners());
    this.client = new MockClient(time, new MockClient.MockMetadataUpdater() {
      @Override
      public List<Node> fetchNodes() {
        return cluster.nodes();
      }

      @Override
      public boolean isUpdateNeeded() {
        return false;
      }

      @Override
      public void update(Time time, MockClient.MetadataUpdate update) {
        throw new UnsupportedOperationException();
      }
    });

    LogContext logContext = new LogContext();
    this.consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time, 100, 1000, Integer.MAX_VALUE);
    this.metrics = new Metrics(time);
    this.rebalanceListener = new MockRebalanceListener();

    this.coordinator = new SchemaRegistryCoordinator(
        logContext,
        consumerClient,
        groupId,
        rebalanceTimeoutMs,
        sessionTimeoutMs,
        heartbeatIntervalMs,
        metrics,
        "sr-" + groupId,
        time,
        retryBackoffMs,
        LEADER_INFO,
        rebalanceListener,
        null
    );
  }

  @After
  public void teardown() {
    this.metrics.close();
  }

  // We only test functionality unique to SchemaRegistryCoordinator. Most functionality is already
  // well tested via the tests that cover AbstractCoordinator & ConsumerCoordinator.

  @Test
  public void testMetadata() {
    JoinGroupRequestData.JoinGroupRequestProtocolCollection serialized = coordinator.metadata();
    assertEquals(1, serialized.size());

    JoinGroupRequestData.JoinGroupRequestProtocol defaultMetadata = serialized.iterator().next();
    assertEquals(SchemaRegistryCoordinator.SR_SUBPROTOCOL_V0, defaultMetadata.name());
    SchemaRegistryIdentity state
        = SchemaRegistryProtocol.deserializeMetadata(ByteBuffer.wrap(defaultMetadata.metadata()));
    assertEquals(LEADER_INFO, state);
  }

  @Test
  public void testNormalJoinGroupLeader() {
    final String consumerId = LEADER_ID;

    client.prepareResponse(groupCoordinatorResponse(node, consumerId, Errors.NONE));
    coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

    // normal join group
    Map<String, SchemaRegistryIdentity> memberInfo = Collections.singletonMap(consumerId, LEADER_INFO);
    client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberInfo, Errors.NONE));
    SyncGroupResponse syncGroupResponse = syncGroupResponse(
        SchemaRegistryProtocol.Assignment.NO_ERROR,
        consumerId,
        LEADER_INFO,
        Errors.NONE
    );
    client.prepareResponse(new MockClient.RequestMatcher() {
      @Override
      public boolean matches(AbstractRequest body) {
        SyncGroupRequest sync = (SyncGroupRequest) body;
        return sync.data().memberId().equals(consumerId) &&
               sync.data().generationId() == 1 &&
               sync.groupAssignments().containsKey(consumerId);
      }
    }, syncGroupResponse);
    coordinator.ensureActiveGroup();

    assertFalse(coordinator.rejoinNeededOrPending());
    assertEquals(0, rebalanceListener.revokedCount);
    assertEquals(1, rebalanceListener.assignedCount);
    assertFalse(rebalanceListener.assignments.get(0).failed());
    assertEquals(consumerId, rebalanceListener.assignments.get(0).leader());
    assertEquals(LEADER_INFO, rebalanceListener.assignments.get(0).leaderIdentity());
  }

  @Test
  public void testJoinGroupLeaderNoneEligible() {
    final String consumerId = LEADER_ID;

    client.prepareResponse(groupCoordinatorResponse(node, consumerId, Errors.NONE));
    coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

    Map<String, SchemaRegistryIdentity> memberInfo = Collections.singletonMap(
        consumerId,
        INELIGIBLE_LEADER_INFO
    );
    client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberInfo, Errors.NONE));
    SyncGroupResponse syncGroupResponse = syncGroupResponse(
        SchemaRegistryProtocol.Assignment.NO_ERROR,
        null,
        null,
        Errors.NONE
    );
    client.prepareResponse(new MockClient.RequestMatcher() {
      @Override
      public boolean matches(AbstractRequest body) {
        SyncGroupRequest sync = (SyncGroupRequest) body;
        return sync.data().memberId().equals(consumerId) &&
               sync.data().generationId() == 1 &&
               sync.groupAssignments().containsKey(consumerId);
      }
    }, syncGroupResponse);

    coordinator.ensureActiveGroup();

    assertFalse(coordinator.rejoinNeededOrPending());
    assertEquals(0, rebalanceListener.revokedCount);
    assertEquals(1, rebalanceListener.assignedCount);
    // No leader isn't considered a failure
    assertFalse(rebalanceListener.assignments.get(0).failed());
    assertNull(rebalanceListener.assignments.get(0).leader());
    assertNull(rebalanceListener.assignments.get(0).leaderIdentity());
  }

  @Test
  public void testJoinGroupLeaderDuplicateUrls() {
    final String consumerId = LEADER_ID;

    client.prepareResponse(groupCoordinatorResponse(node, consumerId, Errors.NONE));
    coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

    Map<String, SchemaRegistryIdentity> memberInfo = new HashMap<>();
    // intentionally duplicate info to get duplicate URLs
    memberInfo.put(LEADER_ID, LEADER_INFO);
    memberInfo.put(MEMBER_ID, LEADER_INFO);
    client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberInfo, Errors.NONE));
    SyncGroupResponse syncGroupResponse = syncGroupResponse(
        SchemaRegistryProtocol.Assignment.DUPLICATE_URLS,
        null,
        null,
        Errors.NONE
    );
    client.prepareResponse(new MockClient.RequestMatcher() {
      @Override
      public boolean matches(AbstractRequest body) {
        SyncGroupRequest sync = (SyncGroupRequest) body;
        return sync.data().memberId().equals(consumerId) &&
               sync.data().generationId() == 1 &&
               sync.groupAssignments().containsKey(consumerId);
      }
    }, syncGroupResponse);

    coordinator.ensureActiveGroup();

    assertFalse(coordinator.rejoinNeededOrPending());
    assertEquals(0, rebalanceListener.revokedCount);
    assertEquals(1, rebalanceListener.assignedCount);
    assertTrue(rebalanceListener.assignments.get(0).failed());
    assertNull(rebalanceListener.assignments.get(0).leader());
    assertNull(rebalanceListener.assignments.get(0).leaderIdentity());
  }

  @Test
  public void testNormalJoinGroupFollower() {
    final String consumerId = MEMBER_ID;

    client.prepareResponse(groupCoordinatorResponse(node, consumerId, Errors.NONE));
    coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

    // normal join group
    client.prepareResponse(joinGroupFollowerResponse(1, consumerId, LEADER_ID, Errors.NONE));
    SyncGroupResponse syncGroupResponse = syncGroupResponse(
        SchemaRegistryProtocol.Assignment.NO_ERROR,
        LEADER_ID,
        LEADER_INFO,
        Errors.NONE
    );
    client.prepareResponse(new MockClient.RequestMatcher() {
      @Override
      public boolean matches(AbstractRequest body) {
        SyncGroupRequest sync = (SyncGroupRequest) body;
        return sync.data().memberId().equals(consumerId) &&
               sync.data().generationId() == 1 &&
               sync.groupAssignments().isEmpty();
      }
    }, syncGroupResponse);
    coordinator.ensureActiveGroup();

    assertFalse(coordinator.rejoinNeededOrPending());
    assertEquals(0, rebalanceListener.revokedCount);
    assertEquals(1, rebalanceListener.assignedCount);
    assertFalse(rebalanceListener.assignments.get(0).failed());
    assertEquals(LEADER_ID, rebalanceListener.assignments.get(0).leader());
    assertEquals(LEADER_INFO, rebalanceListener.assignments.get(0).leaderIdentity());
  }

  private FindCoordinatorResponse groupCoordinatorResponse(Node node, String key,  Errors error) {
    return FindCoordinatorResponse.prepareResponse(error, key, node);
  }

  private JoinGroupResponse joinGroupLeaderResponse(
      int generationId,
      String memberId,
      Map<String, SchemaRegistryIdentity> memberLeaderEligibility,
      Errors error
  ) {
    List<JoinGroupResponseData.JoinGroupResponseMember> metadata = new ArrayList<>();
    for (Map.Entry<String, SchemaRegistryIdentity> configStateEntry : memberLeaderEligibility.entrySet()) {
      SchemaRegistryIdentity memberIdentity = configStateEntry.getValue();
      ByteBuffer buf = SchemaRegistryProtocol.serializeMetadata(memberIdentity);
      metadata.add(new JoinGroupResponseData.JoinGroupResponseMember()
          .setMemberId(configStateEntry.getKey())
          .setMetadata(buf.array()));
    }
    return new JoinGroupResponse(new JoinGroupResponseData()
            .setErrorCode(error.code())
            .setGenerationId(generationId)
            .setProtocolName(SchemaRegistryCoordinator.SR_SUBPROTOCOL_V0)
            .setMemberId(memberId)
            .setLeader(memberId)
            .setMembers(metadata), (short) 0);
  }

  private JoinGroupResponse joinGroupFollowerResponse(
      int generationId,
      String memberId,
      String leaderId,
      Errors error
  ) {
    return new JoinGroupResponse(new JoinGroupResponseData()
        .setErrorCode(error.code())
        .setGenerationId(generationId)
        .setProtocolName(SchemaRegistryCoordinator.SR_SUBPROTOCOL_V0)
        .setMemberId(memberId)
        .setLeader(leaderId)
        .setMembers(Collections.emptyList()),
        (short) 0
    );
  }

  private SyncGroupResponse syncGroupResponse(
      short assignmentError,
      String leader,
      SchemaRegistryIdentity leaderIdentity,
      Errors error
  ) {
    SchemaRegistryProtocol.Assignment assignment = new SchemaRegistryProtocol.Assignment(
        assignmentError, leader, leaderIdentity
    );
    ByteBuffer buf = SchemaRegistryProtocol.serializeAssignment(assignment);
    return new SyncGroupResponse(new SyncGroupResponseData()
        .setErrorCode(error.code())
        .setAssignment(buf.array())
    );
  }

  private static class MockRebalanceListener implements SchemaRegistryRebalanceListener {
    public List<SchemaRegistryProtocol.Assignment> assignments = new ArrayList<>();

    public int revokedCount = 0;
    public int assignedCount = 0;

    @Override
    public void onAssigned(SchemaRegistryProtocol.Assignment assignment, int generation) {
      this.assignments.add(assignment);
      assignedCount++;
    }

    @Override
    public void onRevoked() {
      revokedCount++;
    }
  }
}
