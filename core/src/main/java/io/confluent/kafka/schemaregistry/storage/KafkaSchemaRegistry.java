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

package io.confluent.kafka.schemaregistry.storage;

import io.confluent.common.metrics.JmxReporter;
import io.confluent.common.metrics.MetricConfig;
import io.confluent.common.metrics.MetricName;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.MetricsReporter;
import io.confluent.common.metrics.Sensor;
import io.confluent.common.metrics.stats.Gauge;
import io.confluent.common.utils.SystemTime;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.exceptions.IncompatibleSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryInitializationException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownMasterException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.kafka.schemaregistry.zookeeper.SchemaRegistryIdentity;
import io.confluent.kafka.schemaregistry.zookeeper.ZookeeperMasterElector;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import io.confluent.rest.exceptions.RestException;
import kafka.utils.ZkUtils;

import org.apache.kafka.common.config.ConfigException;

import scala.Tuple2;

import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.avro.reflect.Nullable;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class KafkaSchemaRegistry implements SchemaRegistry {

  /**
   * Schema versions under a particular subject are indexed from MIN_VERSION.
   */
  public static final int MIN_VERSION = 1;
  public static final int MAX_VERSION = Integer.MAX_VALUE;
  public static final int ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE = 20;
  public static final String ZOOKEEPER_SCHEMA_ID_COUNTER = "/schema_id_counter";
  private static final int ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_WRITE_RETRY_BACKOFF_MS = 50;
  private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistry.class);
  final Map<Integer, SchemaKey> guidToSchemaKey;
  final Map<MD5, SchemaIdAndSubjects> schemaHashToGuid;
  private final KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore;
  private final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer;
  private final SchemaRegistryIdentity myIdentity;
  private final Object masterLock = new Object();
  private final AvroCompatibilityLevel defaultCompatibilityLevel;
  private final String schemaRegistryZkNamespace;
  private final String kafkaClusterZkUrl;
  private final int zkSessionTimeoutMs;
  private final int kafkaStoreTimeoutMs;
  private final boolean isEligibleForMasterElector;
  private String schemaRegistryZkUrl;
  private ZkUtils zkUtils;
  private SchemaRegistryIdentity masterIdentity;
  private RestService masterRestService;
  private ZookeeperMasterElector masterElector = null;
  private Metrics metrics;
  private Sensor masterNodeSensor;
  private boolean zkAclsEnabled;

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

  public KafkaSchemaRegistry(SchemaRegistryConfig config,
                             Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer)
      throws SchemaRegistryException {
    String host = config.getString(SchemaRegistryConfig.HOST_NAME_CONFIG);
    int port = getPortForIdentity(config.getInt(SchemaRegistryConfig.PORT_CONFIG),
                                  config.getList(RestConfig.LISTENERS_CONFIG));
    this.schemaRegistryZkNamespace =
        config.getString(SchemaRegistryConfig.SCHEMAREGISTRY_ZK_NAMESPACE);
    this.isEligibleForMasterElector = config.getBoolean(SchemaRegistryConfig.MASTER_ELIGIBILITY);
    this.myIdentity = new SchemaRegistryIdentity(host, port, isEligibleForMasterElector);
    this.kafkaClusterZkUrl =
        config.getString(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG);
    this.zkSessionTimeoutMs =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG);
    this.kafkaStoreTimeoutMs =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG);
    this.serializer = serializer;
    this.defaultCompatibilityLevel = config.compatibilityType();
    this.guidToSchemaKey = new HashMap<Integer, SchemaKey>();
    this.schemaHashToGuid = new HashMap<MD5, SchemaIdAndSubjects>();
    kafkaStore =
        new KafkaStore<SchemaRegistryKey, SchemaRegistryValue>(
            config,
            new KafkaStoreMessageHandler(this),
            this.serializer,
            new InMemoryStore<SchemaRegistryKey, SchemaRegistryValue>(), new NoopKey());
    MetricConfig metricConfig =
        new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
            .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                        TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters =
        config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                                      MetricsReporter.class);
    String jmxPrefix = "kafka.schema.registry";
    reporters.add(new JmxReporter(jmxPrefix));
    this.metrics = new Metrics(metricConfig, reporters, new SystemTime());
    this.masterNodeSensor = metrics.sensor("master-slave-role");
    MetricName
        m = new MetricName("master-slave-role", "master-slave-role",
                           "1.0 indicates the node is the active master in the cluster and is the"
                           + " node where all register schema and config update requests are "
                           + "served.");
    this.masterNodeSensor.add(m, new Gauge());
    this.zkAclsEnabled = checkZkAclConfig(config);
  }

  /**
   * Checks if the user has configured ZooKeeper ACLs or not. Throws an exception if the ZooKeeper
   * client is set to create znodes with an ACL, yet the JAAS config is not present. Otherwise,
   * returns whether or not the user has enabled ZooKeeper ACLs.
   */
  public static boolean checkZkAclConfig(SchemaRegistryConfig config) {
    if (config.getBoolean(SchemaRegistryConfig.ZOOKEEPER_SET_ACL_CONFIG) && !JaasUtils
        .isZkSecurityEnabled()) {
      throw new ConfigException(
          SchemaRegistryConfig.ZOOKEEPER_SET_ACL_CONFIG
          + " is set to true but ZooKeeper's "
          + "JAAS SASL configuration is not configured.");
    }
    return config.getBoolean(SchemaRegistryConfig.ZOOKEEPER_SET_ACL_CONFIG);
  }

  /**
   * A Schema Registry instance's identity is in part the port it listens on. Currently the port can
   * either be configured via the deprecated `port` configuration, or via the `listeners`
   * configuration.
   *
   * <p>This method uses `Application.parseListeners()` from `rest-utils` to get a list of
   * listeners, and returns the port of the first listener to be used for the instance's identity.
   *
   * <p></p>In theory, any port from any listener would be sufficient. Choosing the first, instead
   * of say the last, is arbitrary.
   */
  // TODO: once RestConfig.PORT_CONFIG is deprecated, remove the port parameter.
  static int getPortForIdentity(int port, List<String> configuredListeners) {
    List<URI> listeners = Application.parseListeners(configuredListeners, port,
                                                     Arrays.asList("http", "https"), "http");
    return listeners.get(0).getPort();
  }

  @Override
  public void init() throws SchemaRegistryInitializationException {
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      throw new SchemaRegistryInitializationException(
          "Error initializing kafka store while initializing schema registry", e);
    }

    try {
      createZkNamespace();
      masterElector = new ZookeeperMasterElector(zkUtils, myIdentity, this,
                                                 isEligibleForMasterElector);
    } catch (SchemaRegistryStoreException e) {
      throw new SchemaRegistryInitializationException(
          "Error electing master while initializing schema registry", e);
    } catch (SchemaRegistryTimeoutException e) {
      throw new SchemaRegistryInitializationException(e);
    }
  }

  private void createZkNamespace() {
    int kafkaNamespaceIndex = kafkaClusterZkUrl.indexOf("/");
    String zkConnForNamespaceCreation = kafkaNamespaceIndex > 0
                                        ? kafkaClusterZkUrl.substring(0, kafkaNamespaceIndex)
                                        : kafkaClusterZkUrl;

    String schemaRegistryNamespace = "/" + schemaRegistryZkNamespace;
    schemaRegistryZkUrl = zkConnForNamespaceCreation + schemaRegistryNamespace;

    ZkUtils zkUtilsForNamespaceCreation = ZkUtils.apply(
        zkConnForNamespaceCreation,
        zkSessionTimeoutMs, zkSessionTimeoutMs, zkAclsEnabled);
    // create the zookeeper namespace using cluster.name if it doesn't already exist
    zkUtilsForNamespaceCreation.makeSurePersistentPathExists(
        schemaRegistryNamespace,
        zkUtilsForNamespaceCreation.DefaultAcls());
    log.info("Created schema registry namespace "
             + zkConnForNamespaceCreation
             + schemaRegistryNamespace);
    zkUtilsForNamespaceCreation.close();
    this.zkUtils = ZkUtils.apply(
        schemaRegistryZkUrl, zkSessionTimeoutMs, zkSessionTimeoutMs, zkAclsEnabled);
  }

  public boolean isMaster() {
    synchronized (masterLock) {
      if (masterIdentity != null && masterIdentity.equals(myIdentity)) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * 'Inform' this SchemaRegistry instance which SchemaRegistry is the current master.
   * If this instance is set as the new master, ensure it is up-to-date with data in
   * the kafka store, and tell Zookeeper to allocate the next batch of schema IDs.
   *
   * @param newMaster Identity of the current master. null means no master is alive.
   */
  public void setMaster(@Nullable SchemaRegistryIdentity newMaster)
      throws SchemaRegistryTimeoutException, SchemaRegistryStoreException {
    log.debug("Setting the master to " + newMaster);

    // Only schema registry instances eligible for master can be set to master
    if (newMaster != null && !newMaster.getMasterEligibility()) {
      throw new IllegalStateException(
          "Tried to set an ineligible node to master: " + newMaster);
    }

    synchronized (masterLock) {
      SchemaRegistryIdentity previousMaster = masterIdentity;
      masterIdentity = newMaster;

      if (masterIdentity == null) {
        masterRestService = null;
      } else {
        masterRestService = new RestService(String.format("http://%s:%d",
                                                          masterIdentity.getHost(),
                                                          masterIdentity.getPort()));
      }

      if (masterIdentity != null && !masterIdentity.equals(previousMaster) && isMaster()) {
        nextAvailableSchemaId = nextSchemaIdCounterBatch();
        idBatchInclusiveUpperBound = getInclusiveUpperBound(nextAvailableSchemaId);

        // The new master may not know the exact last offset in the Kafka log. So, mark the
        // last offset invalid here and let the logic in register() deal with it later.
        kafkaStore.markLastWrittenOffsetInvalid();
      }

      masterNodeSensor.record(isMaster() ? 1.0 : 0.0);
    }
  }

  /**
   * Return json data encoding basic information about this SchemaRegistry instance, such as
   * host, port, etc.
   */
  public SchemaRegistryIdentity myIdentity() {
    return myIdentity;
  }

  /**
   * Return the identity of the SchemaRegistry that this instance thinks is current master.
   * Any request that requires writing new data gets forwarded to the master.
   */
  public SchemaRegistryIdentity masterIdentity() {
    synchronized (masterLock) {
      return masterIdentity;
    }
  }

  @Override
  public int register(String subject,
                      Schema schema)
      throws SchemaRegistryException {
    try {
      // Ensure cache is up-to-date before any potential writes
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(kafkaStoreTimeoutMs);

      // see if the schema to be registered already exists
      AvroSchema avroSchema = canonicalizeSchema(schema);
      MD5 md5 = MD5.ofString(schema.getSchema());
      int schemaId = -1;
      if (this.schemaHashToGuid.containsKey(md5)) {
        SchemaIdAndSubjects schemaIdAndSubjects = this.schemaHashToGuid.get(md5);
        if (schemaIdAndSubjects.hasSubject(subject)) {
          // return only if the schema was previously registered under the input subject
          return schemaIdAndSubjects.getSchemaId();
        } else {
          // need to register schema under the input subject
          schemaId = schemaIdAndSubjects.getSchemaId();
        }
      }

      // determine the latest version of the schema in the subject
      Iterator<Schema> allVersions = getAllVersions(subject);
      List<String> allSchemas = new ArrayList<>();
      Schema latestSchema = null;
      int newVersion = MIN_VERSION;
      while (allVersions.hasNext()) {
        latestSchema = allVersions.next();
        allSchemas.add(latestSchema.getSchema());
        newVersion = latestSchema.getVersion() + 1;
      }

      // assign a guid and put the schema in the kafka store
      if (latestSchema == null || isCompatible(subject, avroSchema.canonicalString,
                                               allSchemas)) {
        schema.setVersion(newVersion);

        if (schemaId >= 0) {
          schema.setId(schemaId);
        } else {
          schema.setId(nextAvailableSchemaId);
          nextAvailableSchemaId++;
        }
        if (reachedEndOfIdBatch()) {
          idBatchInclusiveUpperBound = getInclusiveUpperBound(nextSchemaIdCounterBatch());
        }

        SchemaValue schemaValue = new SchemaValue(schema);
        kafkaStore.put(new SchemaKey(subject, newVersion), schemaValue);
        return schema.getId();
      } else {
        throw new IncompatibleSchemaException(
            "New schema is incompatible with an earlier schema.");
      }
    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while registering the schema in the"
                                             + " backend Kafka store", e);
    }
  }

  public int registerOrForward(String subject,
                               Schema schema,
                               Map<String, String> headerProperties)
      throws SchemaRegistryException {
    Schema existingSchema = lookUpSchemaUnderSubject(subject, schema);
    if (existingSchema != null) {
      return existingSchema.getId();
    }

    synchronized (masterLock) {
      if (isMaster()) {
        return register(subject, schema);
      } else {
        // forward registering request to the master
        if (masterIdentity != null) {
          return forwardRegisterRequestToMaster(subject, schema.getSchema(), headerProperties);
        } else {
          throw new UnknownMasterException("Register schema request failed since master is "
                                           + "unknown");
        }
      }
    }
  }

  /**
   * Checks if given schema was ever registered under a subject. If found, it returns the version of
   * the schema under the subject. If not, returns -1
   */
  public Schema lookUpSchemaUnderSubject(String subject, Schema schema)
      throws SchemaRegistryException {
    canonicalizeSchema(schema);
    // see if the schema to be registered already exists
    MD5 md5 = MD5.ofString(schema.getSchema());
    if (this.schemaHashToGuid.containsKey(md5)) {
      SchemaIdAndSubjects schemaIdAndSubjects = this.schemaHashToGuid.get(md5);
      if (schemaIdAndSubjects.hasSubject(subject)) {
        Schema matchingSchema = new Schema(subject,
                                           schemaIdAndSubjects.getVersion(subject),
                                           schemaIdAndSubjects.getSchemaId(),
                                           schema.getSchema());
        return matchingSchema;
      } else {
        // this schema was never registered under the input subject
        return null;
      }
    } else {
      // this schema was never registered in the registry under any subject
      return null;
    }
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
                                       zkUtils.DefaultAcls());
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
        int zkIdCounterValue = Integer.valueOf(counterData);
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

  private int forwardRegisterRequestToMaster(String subject, String schemaString,
                                             Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = masterRestService.getBaseUrls();

    RegisterSchemaRequest registerSchemaRequest = new RegisterSchemaRequest();
    registerSchemaRequest.setSchema(schemaString);
    log.debug(String.format("Forwarding registering schema request %s to %s",
                            registerSchemaRequest, baseUrl));
    try {
      int id = masterRestService.registerSchema(headerProperties, registerSchemaRequest, subject);
      return id;
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the registering schema request %s to %s",
                        registerSchemaRequest, baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private void forwardUpdateCompatibilityLevelRequestToMaster(
      String subject, AvroCompatibilityLevel compatibilityLevel,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = masterRestService.getBaseUrls();

    ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest();
    configUpdateRequest.setCompatibilityLevel(compatibilityLevel.name);
    log.debug(String.format("Forwarding update config request %s to %s",
                            configUpdateRequest, baseUrl));
    try {
      masterRestService.updateConfig(headerProperties, configUpdateRequest, subject);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the update config request %s to %s",
                        configUpdateRequest, baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private AvroSchema canonicalizeSchema(Schema schema) throws InvalidSchemaException {
    AvroSchema avroSchema = AvroUtils.parseSchema(schema.getSchema());
    if (avroSchema == null) {
      throw new InvalidSchemaException("Invalid schema " + schema.toString());
    }
    schema.setSchema(avroSchema.canonicalString);
    return avroSchema;
  }

  @Override
  public Schema get(String subject, int version) throws SchemaRegistryException {
    VersionId versionId = new VersionId(version);
    if (versionId.isLatest()) {
      return getLatestVersion(subject);
    } else {
      SchemaKey key = new SchemaKey(subject, version);
      try {
        SchemaValue schemaValue = (SchemaValue) kafkaStore.get(key);
        Schema schema = getSchemaEntityFromSchemaValue(schemaValue);
        return schema;
      } catch (StoreException e) {
        throw new SchemaRegistryStoreException(
            "Error while retrieving schema from the backend Kafka"
            + " store", e);
      }
    }
  }

  @Override
  public SchemaString get(int id) throws SchemaRegistryException {
    SchemaValue schema = null;
    try {
      SchemaKey subjectVersionKey = guidToSchemaKey.get(id);
      if (subjectVersionKey == null) {
        return null;
      }
      schema = (SchemaValue) kafkaStore.get(subjectVersionKey);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error while retrieving schema with id "
          + id
          + " from the backend Kafka"
          + " store", e);
    }
    SchemaString schemaString = new SchemaString();
    schemaString.setSchemaString(schema.getSchema());
    return schemaString;
  }

  @Override
  public Set<String> listSubjects() throws SchemaRegistryException {
    try {
      Iterator<SchemaRegistryKey> allKeys = kafkaStore.getAllKeys();
      return extractUniqueSubjects(allKeys);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  private Set<String> extractUniqueSubjects(Iterator<SchemaRegistryKey> allKeys) {
    Set<String> subjects = new HashSet<String>();
    while (allKeys.hasNext()) {
      SchemaRegistryKey k = allKeys.next();
      if (k instanceof SchemaKey) {
        SchemaKey key = (SchemaKey) k;
        subjects.add(key.getSubject());
      }
    }
    return subjects;
  }

  @Override
  public Iterator<Schema> getAllVersions(String subject) throws SchemaRegistryException {
    try {
      SchemaKey key1 = new SchemaKey(subject, MIN_VERSION);
      SchemaKey key2 = new SchemaKey(subject, MAX_VERSION);
      Iterator<SchemaRegistryValue> allVersions = kafkaStore.getAll(key1, key2);
      return sortSchemasByVersion(allVersions).iterator();
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  @Override
  public Schema getLatestVersion(String subject) throws SchemaRegistryException {
    try {
      SchemaKey key1 = new SchemaKey(subject, MIN_VERSION);
      SchemaKey key2 = new SchemaKey(subject, MAX_VERSION);
      Iterator<SchemaRegistryValue> allVersions = kafkaStore.getAll(key1, key2);
      Vector<Schema> sortedVersions = sortSchemasByVersion(allVersions);
      Schema latestSchema = null;
      if (sortedVersions.size() > 0) {
        latestSchema = sortedVersions.lastElement();
      }
      return latestSchema;
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  @Override
  public void close() {
    log.info("Shutting down schema registry");
    kafkaStore.close();
    if (masterElector != null) {
      masterElector.close();
    }
    if (zkUtils != null) {
      zkUtils.close();
    }
  }

  public void updateCompatibilityLevel(String subject, AvroCompatibilityLevel newCompatibilityLevel)
      throws SchemaRegistryStoreException, UnknownMasterException {
    ConfigKey configKey = new ConfigKey(subject);
    try {
      kafkaStore.put(configKey, new ConfigValue(newCompatibilityLevel));
      log.debug("Wrote new compatibility level: " + newCompatibilityLevel.name + " to the"
                + " Kafka data store with key " + configKey.toString());
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store",
                                             e);
    }
  }

  public void updateConfigOrForward(String subject, AvroCompatibilityLevel newCompatibilityLevel,
                                    Map<String, String> headerProperties)
      throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
             UnknownMasterException {
    synchronized (masterLock) {
      if (isMaster()) {
        updateCompatibilityLevel(subject, newCompatibilityLevel);
      } else {
        // forward update config request to the master
        if (masterIdentity != null) {
          forwardUpdateCompatibilityLevelRequestToMaster(subject, newCompatibilityLevel,
                                                         headerProperties);
        } else {
          throw new UnknownMasterException("Update config request failed since master is "
                                           + "unknown");
        }
      }
    }
  }

  public AvroCompatibilityLevel getCompatibilityLevel(String subject)
      throws SchemaRegistryStoreException {
    ConfigKey subjectConfigKey = new ConfigKey(subject);
    ConfigValue config;
    try {
      config = (ConfigValue) kafkaStore.get(subjectConfigKey);
      if (config == null && subject == null) {
        // if top level config was never updated, send the configured value for this instance
        config = new ConfigValue(this.defaultCompatibilityLevel);
      } else if (config == null) {
        config = new ConfigValue();
      }
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to read config from the kafka store", e);
    }
    return config.getCompatibilityLevel();
  }

  @Override
  public boolean isCompatible(String subject,
                              String newSchemaObj,
                              String latestSchema)
      throws SchemaRegistryException {
    if (latestSchema == null) {
      throw new InvalidSchemaException(
          "Latest schema not provided");
    }
    return isCompatible(subject, newSchemaObj, Collections.singletonList(latestSchema));
  }

  /**
   * @param previousSchemas Full schema history in chronological order
   */
  @Override
  public boolean isCompatible(String subject,
                              String newSchemaObj,
                              List<String> previousSchemas)
      throws SchemaRegistryException {

    if (previousSchemas == null || previousSchemas.isEmpty()) {
      throw new InvalidSchemaException(
          "Previous schema not provided");
    }

    List<org.apache.avro.Schema> previousAvroSchemas = new ArrayList<>(previousSchemas.size());
    for (String previousSchema : previousSchemas) {
      if (previousSchema == null) {
        throw new InvalidSchemaException(
            "Existing schema " + previousSchema + " is not a valid Avro schema");
      }
      AvroSchema previousAvroSchema = AvroUtils.parseSchema(previousSchema);
      previousAvroSchemas.add(previousAvroSchema.schemaObj);
    }

    AvroCompatibilityLevel compatibility = getCompatibilityLevel(subject);
    if (compatibility == null) {
      compatibility = getCompatibilityLevel(null);
    }
    return compatibility.compatibilityChecker
        .isCompatible(AvroUtils.parseSchema(newSchemaObj).schemaObj, previousAvroSchemas);
  }

  /**
   * For testing.
   */
  KafkaStore<SchemaRegistryKey, SchemaRegistryValue> getKafkaStore() {
    return this.kafkaStore;
  }

  private Vector<Schema> sortSchemasByVersion(Iterator<SchemaRegistryValue> schemas) {
    Vector<Schema> schemaVector = new Vector<Schema>();
    while (schemas.hasNext()) {
      SchemaValue schemaValue = (SchemaValue) schemas.next();
      schemaVector.add(getSchemaEntityFromSchemaValue(schemaValue));
    }
    Collections.sort(schemaVector);
    return schemaVector;
  }

  private Schema getSchemaEntityFromSchemaValue(SchemaValue schemaValue) {
    if (schemaValue == null) {
      return null;
    }
    return new Schema(schemaValue.getSubject(), schemaValue.getVersion(),
                      schemaValue.getId(), schemaValue.getSchema());
  }

  int getMaxIdInKafkaStore() {
    return this.maxIdInKafkaStore;
  }

  /**
   * This should only be updated by the KafkastoreReaderThread.
   */
  void setMaxIdInKafkaStore(int id) {
    this.maxIdInKafkaStore = id;
  }

  /**
   * If true, it's time to allocate a new batch of ids with a call to nextSchemaIdCounterBatch()
   */
  private boolean reachedEndOfIdBatch() {
    return nextAvailableSchemaId > idBatchInclusiveUpperBound;
  }

  /**
   * Return a minimum lower bound on the next batch of ids based on ids currently in the
   * kafka store.
   */
  private int getNextBatchLowerBoundFromKafkaStore() {
    if (this.getMaxIdInKafkaStore() <= 0) {
      return 1;
    }

    int
        nextBatchLowerBound =
        1 + this.getMaxIdInKafkaStore() / ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
    return 1 + nextBatchLowerBound * ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
  }

  /**
   * E.g. if inclusiveLowerBound is 61, and BATCH_SIZE is 20, the inclusiveUpperBound should be 80.
   */
  private int getInclusiveUpperBound(int inclusiveLowerBound) {
    return inclusiveLowerBound + ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE - 1;
  }
}
