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

package io.confluent.kafka.schemaregistry.masterelector.kafka;

import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This class manages the coordination process with the Kafka group coordinator on the broker for
 * coordinating schema registry instances.
 */
final class SchemaRegistryCoordinator extends AbstractCoordinator implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryCoordinator.class);

  public static final String SR_SUBPROTOCOL_V0 = "v0";

  private final SchemaRegistryIdentity identity;
  private SchemaRegistryProtocol.Assignment assignmentSnapshot;
  private final SchemaRegistryRebalanceListener listener;

  /**
   * Initialize the coordination manager.
   */
  public SchemaRegistryCoordinator(
      LogContext logContext,
      ConsumerNetworkClient client,
      String groupId,
      int rebalanceTimeoutMs,
      int sessionTimeoutMs,
      int heartbeatIntervalMs,
      Metrics metrics,
      String metricGrpPrefix,
      Time time,
      long retryBackoffMs,
      SchemaRegistryIdentity identity,
      SchemaRegistryRebalanceListener listener) {
    super(
        new GroupRebalanceConfig(
            sessionTimeoutMs,
            rebalanceTimeoutMs,
            heartbeatIntervalMs,
            groupId,
            Optional.empty(),
            retryBackoffMs,
            true
        ),
        logContext,
        client,
        metrics,
        metricGrpPrefix,
        time
    );
    this.identity = identity;
    this.assignmentSnapshot = null;
    this.listener = listener;
  }

  @Override
  public String protocolType() {
    return "sr";
  }

  public void poll(long timeout) {
    // poll for io until the timeout expires
    final long start = time.milliseconds();
    long now = start;
    long remaining;

    do {
      if (coordinatorUnknown()) {
        ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        now = time.milliseconds();
      }

      if (rejoinNeededOrPending()) {
        ensureActiveGroup();
        now = time.milliseconds();
      }

      pollHeartbeat(now);

      long elapsed = now - start;
      remaining = timeout - elapsed;

      // Note that because the network client is shared with the background heartbeat thread,
      // we do not want to block in poll longer than the time to the next heartbeat.
      client.poll(time.timer(Math.min(Math.max(0, remaining), timeToNextHeartbeat(now))));

      now = time.milliseconds();
      elapsed = now - start;
      remaining = timeout - elapsed;
    } while (remaining > 0);
  }

  @Override
  public JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
    ByteBuffer metadata = SchemaRegistryProtocol.serializeMetadata(identity);
    return new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
            Collections.singletonList(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(SR_SUBPROTOCOL_V0)
                    .setMetadata(metadata.array())).iterator());
  }

  @Override
  protected void onJoinComplete(
      int generation,
      String memberId,
      String protocol,
      ByteBuffer memberAssignment
  ) {
    assignmentSnapshot = SchemaRegistryProtocol.deserializeAssignment(memberAssignment);
    listener.onAssigned(assignmentSnapshot, generation);
  }

  @Override
  protected Map<String, ByteBuffer> performAssignment(
      String kafkaLeaderId, // Kafka group "leader" who does assignment, *not* the SR master
      String protocol,
      List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata
  ) {
    log.debug("Performing assignment");

    Map<String, SchemaRegistryIdentity> memberConfigs = new HashMap<>();
    for (JoinGroupResponseData.JoinGroupResponseMember entry : allMemberMetadata) {
      SchemaRegistryIdentity identity
          = SchemaRegistryProtocol.deserializeMetadata(ByteBuffer.wrap(entry.metadata()));
      memberConfigs.put(entry.memberId(), identity);
    }

    log.debug("Member information: {}", memberConfigs);

    // Compute the leader as the master-eligible member with the "smallest" (lexicographically) ID.
    // This doesn't guarantee a member will stay master until it leaves the group, but should
    // usually keep the master assigned to the same member across rebalances.
    SchemaRegistryIdentity masterIdentity = null;
    String masterKafkaId = null;
    Set<String> urls = new HashSet<>();
    for (Map.Entry<String, SchemaRegistryIdentity> entry : memberConfigs.entrySet()) {
      String kafkaMemberId = entry.getKey();
      SchemaRegistryIdentity memberIdentity = entry.getValue();
      urls.add(memberIdentity.getUrl());
      boolean eligible = memberIdentity.getMasterEligibility();
      boolean smallerIdentity = masterIdentity == null
                                || memberIdentity.getUrl().compareTo(masterIdentity.getUrl()) < 0;
      if (eligible && smallerIdentity) {
        masterKafkaId = kafkaMemberId;
        masterIdentity = memberIdentity;
      }
    }
    short error = SchemaRegistryProtocol.Assignment.NO_ERROR;

    // Validate that schema registry instances aren't trying to use the same URL
    if (urls.size() != memberConfigs.size()) {
      log.error("Found duplicate URLs for schema registry group members. This indicates a "
                + "misconfiguration and is common when executing in containers. Use the host.name "
                + "configuration to set each instance's advertised host name to a value that is "
                + "routable from all other schema registry instances.");
      error = SchemaRegistryProtocol.Assignment.DUPLICATE_URLS;
    }

    // All members currently receive the same assignment information since it is just the leader ID
    Map<String, ByteBuffer> groupAssignment = new HashMap<>();
    SchemaRegistryProtocol.Assignment assignment
        = new SchemaRegistryProtocol.Assignment(error, masterKafkaId, masterIdentity);
    log.debug("Assignment: {}", assignment);
    for (String member : memberConfigs.keySet()) {
      groupAssignment.put(member, SchemaRegistryProtocol.serializeAssignment(assignment));
    }
    return groupAssignment;
  }

  @Override
  protected void onJoinPrepare(int generation, String memberId) {
    log.debug("Revoking previous assignment {}", assignmentSnapshot);
    if (assignmentSnapshot != null) {
      listener.onRevoked();
    }
  }

  @Override
  protected synchronized boolean ensureCoordinatorReady(Timer timer) {
    return super.ensureCoordinatorReady(timer);
  }

  @Override
  protected boolean rejoinNeededOrPending() {
    return super.rejoinNeededOrPending() || assignmentSnapshot == null;
  }
}
