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

package io.confluent.kafka.schemaregistry.leaderelector.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryInitializationException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.IdGenerationException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.storage.LeaderElector;
import io.confluent.kafka.schemaregistry.storage.LeaderAwareSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;
import io.confluent.kafka.schemaregistry.utils.ZkUtils;

@Deprecated
public class ZookeeperLeaderElector implements LeaderElector {

  private static final Logger log = LoggerFactory.getLogger(ZookeeperLeaderElector.class);
  @Deprecated
  private static final String LEADER_PATH = "/schema_registry_master";

  private final boolean isEligibleForLeaderElection;
  private final ZkClient zkClient;
  private final ZkUtils zkUtils;
  private final SchemaRegistryIdentity myIdentity;
  private final String myIdentityString;
  private final LeaderAwareSchemaRegistry schemaRegistry;


  public ZookeeperLeaderElector(SchemaRegistryConfig config,
                                SchemaRegistryIdentity myIdentity,
                                LeaderAwareSchemaRegistry schemaRegistry)
      throws SchemaRegistryStoreException {

    this.isEligibleForLeaderElection = myIdentity.getLeaderEligibility();
    this.zkUtils = config.zkUtils();
    this.zkClient = zkUtils.zkClient();

    this.myIdentity = myIdentity;
    try {
      this.myIdentityString = myIdentity.toJson();
    } catch (IOException e) {
      throw new SchemaRegistryStoreException(String.format(
          "Error while serializing schema registry identity %s to json", myIdentity.toString()), e);
    }
    this.schemaRegistry = schemaRegistry;

    zkClient.subscribeStateChanges(new SessionExpirationListener());
    zkClient.subscribeDataChanges(LEADER_PATH, new LeaderChangeListener());
  }

  public void init() throws SchemaRegistryTimeoutException, SchemaRegistryStoreException,
      SchemaRegistryInitializationException, IdGenerationException {
    if (isEligibleForLeaderElection) {
      electLeader();
    } else {
      readCurrentLeader();
    }
  }

  public void close() {
    zkClient.unsubscribeAll();
    zkUtils.close();
  }

  public void electLeader() throws
      SchemaRegistryStoreException, SchemaRegistryTimeoutException,
      SchemaRegistryInitializationException, IdGenerationException {
    SchemaRegistryIdentity leaderIdentity = null;
    try {
      zkUtils.createEphemeralPathExpectConflict(LEADER_PATH, myIdentityString);
      log.info("Successfully elected the new leader: " + myIdentityString);
      leaderIdentity = myIdentity;
      schemaRegistry.setLeader(leaderIdentity);
    } catch (ZkNodeExistsException znee) {
      readCurrentLeader();
    }
  }

  public void readCurrentLeader()
      throws SchemaRegistryTimeoutException, SchemaRegistryStoreException,
      SchemaRegistryInitializationException, IdGenerationException {
    SchemaRegistryIdentity leaderIdentity = null;
    // If someone else has written the path, read the new leader back
    try {
      String leaderIdentityString = zkUtils.readData(LEADER_PATH).getData();
      try {
        leaderIdentity = SchemaRegistryIdentity.fromJson(leaderIdentityString);
      } catch (IOException ioe) {
        log.error("Can't parse schema registry identity json string " + leaderIdentityString);
      }
    } catch (ZkNoNodeException znne) {
      // NOTE: leaderIdentity is already initialized to null. The leader will then be updated to
      // null so register requests directed to this node can throw the right error code back
    }
    if (myIdentity.equals(leaderIdentity)) {
      log.error("The node's identity is same as elected leader. Check the ``listeners`` config or "
                + "the ``host.name`` and the ``port`` config");
      throw new SchemaRegistryInitializationException("Invalid identity");
    }
    schemaRegistry.setLeader(leaderIdentity);
  }

  private class LeaderChangeListener implements IZkDataListener {

    public LeaderChangeListener() {
    }

    /**
     * Called when the leader information stored in ZooKeeper has changed (or, in some cases,
     * deleted).
     *
     * <p>>** Note ** The ZkClient library has unexpected behavior - under certain conditions,
     * handleDataChange may be called instead of handleDataDeleted when the ephemeral node holding
     * LEADER_PATH is deleted. Therefore it is necessary to call electLeader() here to ensure
     * every eligible node participates in election after a deletion event.
     *
     * @throws Exception On any error.
     */
    @Override
    public void handleDataChange(String dataPath, Object data) {
      try {
        if (isEligibleForLeaderElection) {
          electLeader();
        } else {
          readCurrentLeader();
        }
      } catch (SchemaRegistryException e) {
        log.error("Error while reading the schema registry leader", e);
      }
    }

    /**
     * Called when the leader information stored in zookeeper has been deleted. Try to elect as the
     * leader
     *
     * @throws Exception On any error.
     */
    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      if (isEligibleForLeaderElection) {
        electLeader();
      } else {
        schemaRegistry.setLeader(null);
      }
    }
  }

  private class SessionExpirationListener implements IZkStateListener {

    public SessionExpirationListener() {
    }

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would
     * have to re-create any ephemeral nodes here.
     *
     * @throws Exception On any error.
     */
    @Override
    public void handleNewSession() throws Exception {
      if (isEligibleForLeaderElection) {
        electLeader();
      } else {
        readCurrentLeader();
      }
    }

    @Override
    public void handleSessionEstablishmentError(Throwable t) throws Exception {
      log.error("Failed to re-establish Zookeeper connection: ", t);
      throw new SchemaRegistryStoreException("Couldn't establish Zookeeper connection", t);
    }
  }
}
