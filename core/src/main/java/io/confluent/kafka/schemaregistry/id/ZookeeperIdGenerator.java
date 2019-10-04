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

package io.confluent.kafka.schemaregistry.id;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.exceptions.IdGenerationException;
import io.confluent.kafka.schemaregistry.masterelector.zookeeper.ZookeeperMasterElector;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.kafka.schemaregistry.utils.ZkData;
import io.confluent.kafka.schemaregistry.utils.ZkUtils;

import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZookeeperIdGenerator implements IdGenerator {

  public static final int ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE = 20;
  public static final String ZOOKEEPER_SCHEMA_ID_COUNTER = "/schema_id_counter";
  private static final Logger log = LoggerFactory.getLogger(ZookeeperMasterElector.class);
  private static final int ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_WRITE_RETRY_BACKOFF_MS = 50;

  private ZkUtils zkUtils;
  // Hand out this id during the next schema registration. Indexed from 1.
  private int nextAvailableSchemaId;
  // Tracks the upper bound of the current id batch (inclusive). When nextAvailableSchemaId goes
  // above this value, it's time to allocate a new batch of ids
  private int idBatchInclusiveUpperBound;
  // Track the largest id in the kafka store so far (-1 indicates none in the store)
  // This is automatically updated by the KafkaStoreReaderThread every time a new Schema is added
  // Used to ensure that any newly allocated batch of ids does not overlap
  // with any id in the kafkastore. Primarily for bootstrapping the SchemaRegistry when
  // data is already in the kafkastore.
  private int maxIdInKafkaStore = -1;


  @Override
  public int id(Schema schema) throws IdGenerationException {
    int id = nextAvailableSchemaId;
    nextAvailableSchemaId++;
    if (reachedEndOfIdBatch()) {
      init();
    }
    return id;
  }

  @Override
  public int getMaxId(Integer currentId) {
    if (currentId > maxIdInKafkaStore) {
      log.debug("Requested ID is greater than max ID");
    }
    return maxIdInKafkaStore;
  }

  @Override
  public void configure(SchemaRegistryConfig config) {
    this.zkUtils = config.zkUtils();
  }

  @Override
  public void init() throws IdGenerationException {
    SchemaIdRange nextRange = nextRange();
    nextAvailableSchemaId = nextRange.base();
    idBatchInclusiveUpperBound = nextRange.end();
  }

  @Override
  public void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue) {
    if (maxIdInKafkaStore < schemaValue.getId()) {
      maxIdInKafkaStore = schemaValue.getId();
    }
  }

  private SchemaIdRange nextRange() throws IdGenerationException {
    int base = nextSchemaIdCounterBatch();
    return new SchemaIdRange(base, getInclusiveUpperBound(base));
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
  private Integer nextSchemaIdCounterBatch() throws IdGenerationException {
    int nextIdBatchLowerBound = 1;

    while (true) {

      if (!zkUtils.zkClient().exists(ZOOKEEPER_SCHEMA_ID_COUNTER)) {
        // create ZOOKEEPER_SCHEMA_ID_COUNTER if it already doesn't exist

        try {
          nextIdBatchLowerBound = getNextBatchLowerBoundFromKafkaStore();
          int nextIdBatchUpperBound = getInclusiveUpperBound(nextIdBatchLowerBound);
          zkUtils.createPersistentPath(ZOOKEEPER_SCHEMA_ID_COUNTER,
              String.valueOf(nextIdBatchUpperBound));
          return nextIdBatchLowerBound;
        } catch (ZkNodeExistsException ignore) {
          // A zombie master may have created this zk node after the initial existence check
          // Ignore and try again
        }
      } else { // ZOOKEEPER_SCHEMA_ID_COUNTER exists

        // read the latest counter value
        final ZkData counterValue = zkUtils.readData(ZOOKEEPER_SCHEMA_ID_COUNTER);
        final String counterData = counterValue.getData();
        final Stat counterStat = counterValue.getStat();
        if (counterData == null) {
          throw new IdGenerationException(
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
                counterStat.getVersion());
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
    if (maxIdInKafkaStore <= 0) {
      return 1;
    }

    int nextBatchLowerBound = 1 + maxIdInKafkaStore / ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
    return 1 + nextBatchLowerBound * ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
  }

  /**
   * E.g. if inclusiveLowerBound is 61, and BATCH_SIZE is 20, the inclusiveUpperBound should be 80.
   */
  private int getInclusiveUpperBound(int inclusiveLowerBound) {
    return inclusiveLowerBound + ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE - 1;
  }

  /**
   * If true, it's time to allocate a new batch of ids with a call to nextSchemaIdCounterBatch()
   */
  private boolean reachedEndOfIdBatch() {
    return nextAvailableSchemaId > idBatchInclusiveUpperBound;
  }



}
