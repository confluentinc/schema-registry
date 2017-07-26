/**
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.masterelector.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.storage.MasterElector;
import io.confluent.kafka.schemaregistry.storage.SchemaIdRange;
import io.confluent.kafka.schemaregistry.storage.MasterAwareSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;
import kafka.utils.ZkUtils;
import scala.Tuple2;

public class ZookeeperMasterElector implements MasterElector {

  private static final Logger log = LoggerFactory.getLogger(ZookeeperMasterElector.class);
  private static final String MASTER_PATH = "/schema_registry_master";
  public static final int ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE = 20;
  public static final String ZOOKEEPER_SCHEMA_ID_COUNTER = "/schema_id_counter";
  private static final int ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_WRITE_RETRY_BACKOFF_MS = 50;

  private final boolean isEligibleForMasterElection;
  private final ZkClient zkClient;
  private final ZkUtils zkUtils;
  private final SchemaRegistryIdentity myIdentity;
  private final String myIdentityString;
  private final MasterAwareSchemaRegistry schemaRegistry;


  public ZookeeperMasterElector(SchemaRegistryConfig config,
                                SchemaRegistryIdentity myIdentity,
                                MasterAwareSchemaRegistry schemaRegistry)
      throws SchemaRegistryStoreException {

    this.isEligibleForMasterElection = myIdentity.getMasterEligibility();
    this.zkUtils = createZkNamespace(config);
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
    zkClient.subscribeDataChanges(MASTER_PATH, new MasterChangeListener());
  }

  public void init() throws SchemaRegistryTimeoutException, SchemaRegistryStoreException {
    if (isEligibleForMasterElection) {
      electMaster();
    } else {
      readCurrentMaster();
    }
  }

  public void close() {
    zkClient.unsubscribeAll();
    zkUtils.close();
  }

  public void electMaster() throws
                            SchemaRegistryStoreException, SchemaRegistryTimeoutException {
    SchemaRegistryIdentity masterIdentity = null;
    try {
      zkUtils.createEphemeralPathExpectConflict(MASTER_PATH, myIdentityString,
                                                zkUtils.defaultAcls(MASTER_PATH));
      log.info("Successfully elected the new master: " + myIdentityString);
      masterIdentity = myIdentity;
      schemaRegistry.setMaster(masterIdentity);
    } catch (ZkNodeExistsException znee) {
      readCurrentMaster();
    }
  }

  public void readCurrentMaster()
      throws SchemaRegistryTimeoutException, SchemaRegistryStoreException {
    SchemaRegistryIdentity masterIdentity = null;
    // If someone else has written the path, read the new master back
    try {
      String masterIdentityString = zkUtils.readData(MASTER_PATH)._1();
      try {
        masterIdentity = SchemaRegistryIdentity.fromJson(masterIdentityString);
      } catch (IOException ioe) {
        log.error("Can't parse schema registry identity json string " + masterIdentityString);
      }
    } catch (ZkNoNodeException znne) {
      // NOTE: masterIdentity is already initialized to null. The master will then be updated to 
      // null so register requests directed to this node can throw the right error code back
    }
    schemaRegistry.setMaster(masterIdentity);
  }

  public SchemaIdRange nextRange() throws SchemaRegistryStoreException {
    int base = nextSchemaIdCounterBatch();
    return new SchemaIdRange(base, getInclusiveUpperBound(base));
  }

  private ZkUtils createZkNamespace(SchemaRegistryConfig config) {
    boolean zkAclsEnabled = config.checkZkAclConfig();
    String schemaRegistryZkNamespace =
        config.getString(SchemaRegistryConfig.SCHEMAREGISTRY_ZK_NAMESPACE);
    String srClusterZkUrl = config.schemaRegistryZkUrl();
    int zkSessionTimeoutMs =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG);

    int kafkaNamespaceIndex = srClusterZkUrl.indexOf("/");
    String zkConnForNamespaceCreation = kafkaNamespaceIndex > 0
                                        ? srClusterZkUrl.substring(0, kafkaNamespaceIndex)
                                        : srClusterZkUrl;

    final String schemaRegistryNamespace = "/" + schemaRegistryZkNamespace;
    final String schemaRegistryZkUrl = zkConnForNamespaceCreation + schemaRegistryNamespace;

    ZkUtils zkUtilsForNamespaceCreation = ZkUtils.apply(
        zkConnForNamespaceCreation,
        zkSessionTimeoutMs, zkSessionTimeoutMs, zkAclsEnabled);
    zkUtilsForNamespaceCreation.makeSurePersistentPathExists(
        schemaRegistryNamespace,
        zkUtilsForNamespaceCreation.defaultAcls(schemaRegistryNamespace));
    log.info("Created schema registry namespace "
             + zkConnForNamespaceCreation
             + schemaRegistryNamespace);
    zkUtilsForNamespaceCreation.close();
    return ZkUtils.apply(
        schemaRegistryZkUrl,
        zkSessionTimeoutMs,
        zkSessionTimeoutMs,
        zkAclsEnabled
    );
  }

  /**
   * Allocate and lock the next batch of schema ids. Signal a global lock over the next batch by
   * writing the inclusive upper bound of the batch to ZooKeeper. I.e. the value stored in
   * ZOOKEEPER_SCHEMA_ID_COUNTER in ZooKeeper indicates the current max allocated id for assignment.
   *
   * <p>When a schema registry server is initialized, kafka may have preexisting persistent
   * schema -> id assignments, and zookeeper may have preexisting counter data.
   * Therefore, when allocating the next batch of ids, it's necessary to ensure the entire new batch
   * is greater than the greatest id in kafka and also greater than the previously recorded batch
   * in zookeeper.
   *
   * <p>Return the first available id in the newly allocated batch of ids.
   */
  private Integer nextSchemaIdCounterBatch() throws SchemaRegistryStoreException {
    int nextIdBatchLowerBound = 1;

    while (true) {

      if (!zkUtils.zkClient().exists(ZOOKEEPER_SCHEMA_ID_COUNTER)) {
        // create ZOOKEEPER_SCHEMA_ID_COUNTER if it already doesn't exist

        try {
          nextIdBatchLowerBound = getNextBatchLowerBoundFromKafkaStore();
          int nextIdBatchUpperBound = getInclusiveUpperBound(nextIdBatchLowerBound);
          zkUtils.createPersistentPath(ZOOKEEPER_SCHEMA_ID_COUNTER,
                                       String.valueOf(nextIdBatchUpperBound),
                                       zkUtils.defaultAcls(ZOOKEEPER_SCHEMA_ID_COUNTER));
          return nextIdBatchLowerBound;
        } catch (ZkNodeExistsException ignore) {
          // A zombie master may have created this zk node after the initial existence check
          // Ignore and try again
        }
      } else { // ZOOKEEPER_SCHEMA_ID_COUNTER exists

        // read the latest counter value
        final Tuple2<String, Stat> counterValue = zkUtils.readData(ZOOKEEPER_SCHEMA_ID_COUNTER);
        final String counterData = counterValue._1();
        final Stat counterStat = counterValue._2();
        if (counterData == null) {
          throw new SchemaRegistryStoreException(
              "Failed to read schema id counter " + ZOOKEEPER_SCHEMA_ID_COUNTER
              + " from zookeeper");
        }

        // Compute the lower bound of next id batch based on zk data and kafkastore data
        int zkIdCounterValue = Integer.parseInt(counterData);
        int zkNextIdBatchLowerBound = zkIdCounterValue + 1;
        if (zkIdCounterValue % ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE != 0) {
          // ZooKeeper id counter should be an integer multiple of id batch size in normal
          // operation; handle corrupted/stale id counter data gracefully by bumping
          // up to the next id batch

          // fixedZkIdCounterValue is the smallest multiple of
          // ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE greater than the bad zkIdCounterValue
          int fixedZkIdCounterValue = ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE
                                      * (1
                                         + zkIdCounterValue
                                           / ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE);
          zkNextIdBatchLowerBound = fixedZkIdCounterValue + 1;

          log.warn(
              "Zookeeper schema id counter is not an integer multiple of id batch size."
              + " Zookeeper may have stale id counter data.\n"
              + "zk id counter: " + zkIdCounterValue + "\n"
              + "id batch size: " + ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE);
        }
        nextIdBatchLowerBound =
            Math.max(zkNextIdBatchLowerBound, getNextBatchLowerBoundFromKafkaStore());
        String
            nextIdBatchUpperBound =
            String.valueOf(getInclusiveUpperBound(nextIdBatchLowerBound));

        // conditionally update the zookeeper path with the upper bound of the new id batch.
        // newSchemaIdCounterDataVersion < 0 indicates a failed conditional update.
        // Most probable cause is the existence of another master which tries to do the same
        // counter batch allocation at the same time. If this happens, re-read the value and
        // continue until one master is determined to be the zombie master.
        // NOTE: The handling of multiple masters is still a TODO
        int newSchemaIdCounterDataVersion =
            (Integer) zkUtils.conditionalUpdatePersistentPath(
                ZOOKEEPER_SCHEMA_ID_COUNTER,
                nextIdBatchUpperBound,
                counterStat.getVersion(),
                null)._2();
        if (newSchemaIdCounterDataVersion >= 0) {
          break;
        }
      }
      try {
        // Wait a bit and attempt id batch allocation again
        Thread.sleep(ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_WRITE_RETRY_BACKOFF_MS);
      } catch (InterruptedException ignored) {
        // ignored
      }
    }

    return nextIdBatchLowerBound;
  }

  /**
   * Return a minimum lower bound on the next batch of ids based on ids currently in the
   * kafka store.
   */
  private int getNextBatchLowerBoundFromKafkaStore() {
    if (schemaRegistry.getMaxIdInKafkaStore() <= 0) {
      return 1;
    }

    int
        nextBatchLowerBound =
        1 + schemaRegistry.getMaxIdInKafkaStore() / ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
    return 1 + nextBatchLowerBound * ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
  }

  /**
   * E.g. if inclusiveLowerBound is 61, and BATCH_SIZE is 20, the inclusiveUpperBound should be 80.
   */
  private int getInclusiveUpperBound(int inclusiveLowerBound) {
    return inclusiveLowerBound + ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE - 1;
  }

  private class MasterChangeListener implements IZkDataListener {

    public MasterChangeListener() {
    }

    /**
     * Called when the master information stored in ZooKeeper has changed (or, in some cases,
     * deleted).
     *
     * <p>>** Note ** The ZkClient library has unexpected behavior - under certain conditions,
     * handleDataChange may be called instead of handleDataDeleted when the ephemeral node holding
     * MASTER_PATH is deleted. Therefore it is necessary to call electMaster() here to ensure
     * every eligible node participates in election after a deletion event.
     *
     * @throws Exception On any error.
     */
    @Override
    public void handleDataChange(String dataPath, Object data) {
      try {
        if (isEligibleForMasterElection) {
          electMaster();
        } else {
          readCurrentMaster();
        }
      } catch (SchemaRegistryException e) {
        log.error("Error while reading the schema registry master", e);
      }
    }

    /**
     * Called when the master information stored in zookeeper has been deleted. Try to elect as the
     * leader
     *
     * @throws Exception On any error.
     */
    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      if (isEligibleForMasterElection) {
        electMaster();
      } else {
        schemaRegistry.setMaster(null);
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
      if (isEligibleForMasterElection) {
        electMaster();
      } else {
        readCurrentMaster();
      }
    }

    @Override
    public void handleSessionEstablishmentError(Throwable t) throws Exception {
      log.error("Failed to re-establish Zookeeper connection: ", t);
      throw new SchemaRegistryStoreException("Couldn't establish Zookeeper connection", t);
    }
  }
}
