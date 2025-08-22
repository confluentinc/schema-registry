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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.confluent.kafka.schemaregistry.leaderelector.kafka.SchemaRegistryProtocol.Assignment;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class SchemaRegistryCoordinatorTest {

  private static final String LEADER_ID = "leader";
  private static final String MEMBER_ID = "member";
  private static final String LEADER_HOST = "leaderHost";
  private static final int LEADER_PORT = 8083;
  private static final String MEMBER_0 = "member-0";
  private static final String MEMBER_0_HOST = "member-0-host";
  private static final String MEMBER_1 = "member-1";
  private static final String MEMBER_1_HOST = "member-1-host";
  private static final String MEMBER_2 = "member-2";
  private static final String MEMBER_2_HOST = "member-2-host";


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
  private static final SchemaRegistryIdentity MEMBER_0_INFO = new SchemaRegistryIdentity(
          MEMBER_0_HOST,
          LEADER_PORT,
          true,
          SchemaRegistryConfig.HTTPS
  );
  private static final SchemaRegistryIdentity MEMBER_1_INFO = new SchemaRegistryIdentity(
          MEMBER_1_HOST,
          LEADER_PORT,
          true,
          SchemaRegistryConfig.HTTPS
  );
  private static final SchemaRegistryIdentity MEMBER_2_INFO = new SchemaRegistryIdentity(
          MEMBER_2_HOST,
          LEADER_PORT,
          true,
          SchemaRegistryConfig.HTTPS
  );

  private MockTime time;
  private MockClient client;
  private final Cluster cluster = TestUtils.clusterWith(3, "topic", 3);
  private final Node node = cluster.nodes().get(0);
  private Metrics metrics;
  private MockRebalanceListener rebalanceListener;
  private MockRebalanceListener member0RebalanceListener;
  private MockRebalanceListener member1RebalanceListener;
  private MockRebalanceListener member2RebalanceListener;
  private SchemaRegistryCoordinator coordinator;
  private SchemaRegistryCoordinator member0Coordinator;
  private SchemaRegistryCoordinator member1Coordinator;
  private SchemaRegistryCoordinator member2Coordinator;
  private LogContext logContext;
  private ConsumerNetworkClient consumerClient;
  private final String groupId = "test-group";
  private final int sessionTimeoutMs = 10;
  private final int rebalanceTimeoutMs = 60;
  private final int heartbeatIntervalMs = 2;
  private final long retryBackoffMs = 100;
  private final long retryBackoffMaxMs = 1000;

  @Before
  public void setup() {
    this.time = new MockTime();
    Metadata metadata = new Metadata(0, Long.MAX_VALUE, Long.MAX_VALUE, new LogContext(), new ClusterResourceListeners());
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


    this.logContext = new LogContext();
    this.consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time, 100, 1000, Integer.MAX_VALUE);
    this.metrics = new Metrics(time);
    this.rebalanceListener = new MockRebalanceListener();
    this.member0RebalanceListener = new MockRebalanceListener();
    this.member1RebalanceListener = new MockRebalanceListener();
    this.member2RebalanceListener = new MockRebalanceListener();

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
        retryBackoffMaxMs,
        LEADER_INFO,
        rebalanceListener,
        null,
        false
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
    initiateFindJoinSyncFlow(LEADER_ID, coordinator, Collections.singletonMap(LEADER_ID, LEADER_INFO), true, LEADER_ID, LEADER_INFO, Assignment.NO_ERROR);

    validate(coordinator, rebalanceListener, 0, 1, false, LEADER_ID, LEADER_INFO);
  }

  @Test
  public void testJoinGroupLeaderNoneEligible() {
    initiateFindJoinSyncFlow(LEADER_ID, coordinator, Collections.singletonMap(LEADER_ID, INELIGIBLE_LEADER_INFO), true, null, null, Assignment.NO_ERROR);

    validate(coordinator, rebalanceListener, 0, 1, false, null, null);
  }

  @Test
  public void testJoinGroupLeaderDuplicateUrls() {
    Map<String, SchemaRegistryIdentity> memberInfo = new HashMap<>();
    // intentionally duplicate info to get duplicate URLs
    memberInfo.put(LEADER_ID, LEADER_INFO);
    memberInfo.put(MEMBER_ID, LEADER_INFO);
    initiateFindJoinSyncFlow(LEADER_ID, coordinator, memberInfo, true, null, null, Assignment.DUPLICATE_URLS);

    validate(coordinator, rebalanceListener, 0, 1, true, null, null);
  }

  @Test
  public void testNormalJoinGroupFollower() {
    initiateFindJoinSyncFlow(MEMBER_ID, coordinator, null, false, LEADER_ID, LEADER_INFO, Assignment.NO_ERROR);

    validate(coordinator, rebalanceListener, 0, 1, false, LEADER_ID, LEADER_INFO);
  }

  /*
  Tests to check leader assignment changes during roll
   */
  @Test
  public void testCoordinatorDuringRollWithStickyLeaderElection() {
    initializeCoordinators(true);
    // member 0 joins group
    Map<String, SchemaRegistryIdentity> groupMembership = new HashMap<>();
    groupMembership.put(MEMBER_0, MEMBER_0_INFO);
    initiateFindJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 0, 1, false, MEMBER_0, MEMBER_0_INFO);

    // member 1 joins group
    groupMembership.put(MEMBER_1, MEMBER_1_INFO);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateFindJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 1, 2, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 0, 1, false, MEMBER_0, MEMBER_0_INFO);

    // member 2 joins group
    groupMembership.put(MEMBER_2, MEMBER_2_INFO);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateFindJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 2, 3, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 1, 2, false, MEMBER_0, MEMBER_0_INFO);
    validate(member2Coordinator, member2RebalanceListener, 0, 1, false, MEMBER_0, MEMBER_0_INFO);

    // member 2 leaves group
    groupMembership.remove(MEMBER_2);
    member2Coordinator.setAssignmentSnapshot(null);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 3, 4, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 2, 3, false, MEMBER_0, MEMBER_0_INFO);

    // member 2 re-joins group
    groupMembership.put(MEMBER_2, MEMBER_2_INFO);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 4, 5, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 3, 4, false, MEMBER_0, MEMBER_0_INFO);
    validate(member2Coordinator, member2RebalanceListener, 0, 2, false, MEMBER_0, MEMBER_0_INFO);

    // member 1 leaves group
    groupMembership.remove(MEMBER_1);
    member1Coordinator.setAssignmentSnapshot(null);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 5, 6, false, MEMBER_0, MEMBER_0_INFO);
    validate(member2Coordinator, member2RebalanceListener, 1, 3, false, MEMBER_0, MEMBER_0_INFO);

    // member 1 re-joins group
    groupMembership.put(MEMBER_1, MEMBER_1_INFO);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 6, 7, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 3, 5, false, MEMBER_0, MEMBER_0_INFO);
    validate(member2Coordinator, member2RebalanceListener, 2, 4, false, MEMBER_0, MEMBER_0_INFO);

    // member 0 leaves group
    groupMembership.remove(MEMBER_0);
    member0Coordinator.setAssignmentSnapshot(null);
    member0Coordinator.getIdentity().setLeader(false);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, true, MEMBER_1, MEMBER_1_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_1, MEMBER_1_INFO, Assignment.NO_ERROR);
    validate(member1Coordinator, member1RebalanceListener, 4, 6, false, MEMBER_1, MEMBER_1_INFO);
    validate(member2Coordinator, member2RebalanceListener, 3, 5, false, MEMBER_1, MEMBER_1_INFO);

    // member 0 re-joins group
    groupMembership.put(MEMBER_0, MEMBER_0_INFO);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_1_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_1_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_0, MEMBER_1_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 6, 8, false, MEMBER_0, MEMBER_1_INFO);
    validate(member1Coordinator, member1RebalanceListener, 5, 7, false, MEMBER_0, MEMBER_1_INFO);
    validate(member2Coordinator, member2RebalanceListener, 4, 6, false, MEMBER_0, MEMBER_1_INFO);
  }

  @Test
  public void testCoordinatorDuringRoll() {
    initializeCoordinators(false);
    // member 0 joins group
    Map<String, SchemaRegistryIdentity> groupMembership = new HashMap<>();
    groupMembership.put(MEMBER_0, MEMBER_0_INFO);
    initiateFindJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 0, 1, false, MEMBER_0, MEMBER_0_INFO);

    // member 1 joins group
    groupMembership.put(MEMBER_1, MEMBER_1_INFO);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateFindJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 1, 2, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 0, 1, false, MEMBER_0, MEMBER_0_INFO);

    // member 2 joins group
    groupMembership.put(MEMBER_2, MEMBER_2_INFO);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateFindJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 2, 3, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 1, 2, false, MEMBER_0, MEMBER_0_INFO);
    validate(member2Coordinator, member2RebalanceListener, 0, 1, false, MEMBER_0, MEMBER_0_INFO);

    // member 2 leaves group
    groupMembership.remove(MEMBER_2);
    member2Coordinator.setAssignmentSnapshot(null);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 3, 4, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 2, 3, false, MEMBER_0, MEMBER_0_INFO);

    // member 2 re-joins group
    groupMembership.put(MEMBER_2, MEMBER_2_INFO);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 4, 5, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 3, 4, false, MEMBER_0, MEMBER_0_INFO);
    validate(member2Coordinator, member2RebalanceListener, 0, 2, false, MEMBER_0, MEMBER_0_INFO);

    // member 1 leaves group
    groupMembership.remove(MEMBER_1);
    member1Coordinator.setAssignmentSnapshot(null);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 5, 6, false, MEMBER_0, MEMBER_0_INFO);
    validate(member2Coordinator, member2RebalanceListener, 1, 3, false, MEMBER_0, MEMBER_0_INFO);

    // member 1 re-joins group
    groupMembership.put(MEMBER_1, MEMBER_1_INFO);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 6, 7, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 3, 5, false, MEMBER_0, MEMBER_0_INFO);
    validate(member2Coordinator, member2RebalanceListener, 2, 4, false, MEMBER_0, MEMBER_0_INFO);

    // member 0 leaves group
    groupMembership.remove(MEMBER_0);
    member0Coordinator.setAssignmentSnapshot(null);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, true, MEMBER_1, MEMBER_1_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_1, MEMBER_1_INFO, Assignment.NO_ERROR);
    validate(member1Coordinator, member1RebalanceListener, 4, 6, false, MEMBER_1, MEMBER_1_INFO);
    validate(member2Coordinator, member2RebalanceListener, 3, 5, false, MEMBER_1, MEMBER_1_INFO);

    // member 0 re-joins group
    groupMembership.put(MEMBER_0, MEMBER_0_INFO);
    initiateJoinSyncFlow(MEMBER_0, member0Coordinator, groupMembership, true, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_1, member1Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    initiateJoinSyncFlow(MEMBER_2, member2Coordinator, groupMembership, false, MEMBER_0, MEMBER_0_INFO, Assignment.NO_ERROR);
    validate(member0Coordinator, member0RebalanceListener, 6, 8, false, MEMBER_0, MEMBER_0_INFO);
    validate(member1Coordinator, member1RebalanceListener, 5, 7, false, MEMBER_0, MEMBER_0_INFO);
    validate(member2Coordinator, member2RebalanceListener, 4, 6, false, MEMBER_0, MEMBER_0_INFO);
  }

  private void initializeCoordinators(boolean stickyLeaderElection) {
    this.member0Coordinator = new SchemaRegistryCoordinator(logContext,
            consumerClient,
            groupId,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            heartbeatIntervalMs,
            new Metrics(time),
            "sr-" + groupId,
            time,
            retryBackoffMs,
            retryBackoffMaxMs,
            MEMBER_0_INFO,
            member0RebalanceListener,
            null,
            stickyLeaderElection);
    this.member1Coordinator = new SchemaRegistryCoordinator(logContext,
            consumerClient,
            groupId,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            heartbeatIntervalMs,
            new Metrics(time),
            "sr-" + groupId,
            time,
            retryBackoffMs,
            retryBackoffMaxMs,
            MEMBER_1_INFO,
            member1RebalanceListener,
            null,
            stickyLeaderElection);
    this.member2Coordinator = new SchemaRegistryCoordinator(logContext,
            consumerClient,
            groupId,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            heartbeatIntervalMs,
            new Metrics(time),
            "sr-" + groupId,
            time,
            retryBackoffMs,
            retryBackoffMaxMs,
            MEMBER_2_INFO,
            member2RebalanceListener,
            null,
            stickyLeaderElection);
  }

  private void validate(SchemaRegistryCoordinator memberCoordinator,
                        MockRebalanceListener memberRebalanceListener,
                        int revokedCount,
                        int assignedCount,
                        boolean assignmentFailed,
                        String leaderElector,
                        SchemaRegistryIdentity leaderIdentity) {
    assertFalse(memberCoordinator.rejoinNeededOrPending());
    assertEquals(revokedCount, memberRebalanceListener.revokedCount);
    assertEquals(assignedCount, memberRebalanceListener.assignedCount);
    assertEquals(assignmentFailed, memberRebalanceListener.assignments.get(assignedCount - 1).failed());
    assertEquals(leaderElector, memberRebalanceListener.assignments.get(assignedCount - 1).leader());
    assertEquals(leaderIdentity, memberRebalanceListener.assignments.get(assignedCount - 1).leaderIdentity());
  }

  private void initiateJoinSyncFlow(String member,
                                    SchemaRegistryCoordinator memberCoordinator,
                                    Map<String, SchemaRegistryIdentity> groupMembership,
                                    boolean isLeaderElector,
                                    String leaderElector,
                                    SchemaRegistryIdentity leaderIdentity,
                                    short assignmentError) {
    // normal join group
    if (isLeaderElector) {
      client.prepareResponse(joinGroupLeaderResponse(1, member, groupMembership, Errors.NONE));
      SyncGroupResponse syncGroupResponse = syncGroupResponse(assignmentError,
              leaderElector,
              leaderIdentity,
              Errors.NONE);
      client.prepareResponse(body -> {
        SyncGroupRequest sync = (SyncGroupRequest) body;
        return sync.data().memberId().equals(member) &&
                sync.data().generationId() == 1 &&
                sync.groupAssignments().containsKey(member);
      }, syncGroupResponse);
    }
    else {
      client.prepareResponse(joinGroupFollowerResponse(1, member, leaderElector, Errors.NONE));
      SyncGroupResponse syncGroupResponse = syncGroupResponse(assignmentError,
              leaderElector,
              leaderIdentity,
              Errors.NONE);
      client.prepareResponse(body -> {
        SyncGroupRequest sync = (SyncGroupRequest) body;
        return sync.data().memberId().equals(member) &&
                sync.data().generationId() == 1;
      }, syncGroupResponse);
    }
    memberCoordinator.requestRejoin("join group");
    memberCoordinator.ensureActiveGroup();
  }

  private void initiateFindJoinSyncFlow(String member,
                                        SchemaRegistryCoordinator memberCoordinator,
                                        Map<String, SchemaRegistryIdentity> groupMembership,
                                        boolean isLeaderElector,
                                        String leaderElector,
                                        SchemaRegistryIdentity leaderIdentity,
                                        short assignmentError) {
    client.prepareResponse(groupCoordinatorResponse(node, member, Errors.NONE));
    memberCoordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

    // normal join group
    initiateJoinSyncFlow(member, memberCoordinator, groupMembership, isLeaderElector, leaderElector, leaderIdentity, assignmentError);
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
    Assignment assignment = new Assignment(
        assignmentError, leader, leaderIdentity
    );
    ByteBuffer buf = SchemaRegistryProtocol.serializeAssignment(assignment);
    return new SyncGroupResponse(new SyncGroupResponseData()
        .setErrorCode(error.code())
        .setAssignment(buf.array())
    );
  }

  private static class MockRebalanceListener implements SchemaRegistryRebalanceListener {
    public List<Assignment> assignments = new ArrayList<>();

    public int revokedCount = 0;
    public int assignedCount = 0;

    @Override
    public void onAssigned(Assignment assignment, int generation) {
      this.assignments.add(assignment);
      assignedCount++;
    }

    @Override
    public void onRevoked() {
      revokedCount++;
    }
  }
}
