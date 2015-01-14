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

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import io.confluent.common.utils.ZkData;
import io.confluent.common.utils.ZkUtils;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.rest.RegisterSchemaForwardingAgent;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.entities.Config;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.rest.exceptions.IncompatibleAvroSchemaException;
import io.confluent.kafka.schemaregistry.rest.exceptions.InvalidAvroException;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.kafka.schemaregistry.storage.serialization.ZkStringSerializer;
import io.confluent.kafka.schemaregistry.zookeeper.SchemaRegistryIdentity;
import io.confluent.kafka.schemaregistry.zookeeper.ZookeeperMasterElector;

public class KafkaSchemaRegistry implements SchemaRegistry {

  /**
   * Schema versions under a particular subject are indexed from MIN_VERSION.
   */
  public static final int MIN_VERSION = 1;
  public static final int MAX_VERSION = Integer.MAX_VALUE;
  private static final String ZOOKEEPER_SCHEMA_ID_COUNTER = "/schema_id_counter";
  private static final int ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE = 1000;
  private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistry.class);
  final Map<Long, SchemaKey> idCache;
  private final KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore;
  private final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer;
  private final SchemaRegistryIdentity myIdentity;
  private final Object masterLock = new Object();
  private final AvroCompatibilityLevel defaultCompatibilityLevel;
  private final ZkClient zkClient;
  private SchemaRegistryIdentity masterIdentity;
  private ZookeeperMasterElector masterElector = null;
  private AtomicLong schemaIdCounter;
  private long maxSchemaIdCounterValue;

  public KafkaSchemaRegistry(SchemaRegistryConfig config,
                             Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer)
      throws SchemaRegistryException {
    String host = config.getString(SchemaRegistryConfig.ADVERTISED_HOST_CONFIG);
    int port = config.getInt(SchemaRegistryConfig.PORT_CONFIG);
    myIdentity = new SchemaRegistryIdentity(host, port);

    String kafkaClusterZkUrl =
        config.getString(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG);
    int zkSessionTimeoutMs =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG);
    this.zkClient = new ZkClient(kafkaClusterZkUrl, zkSessionTimeoutMs, zkSessionTimeoutMs,
                                 new ZkStringSerializer());
    this.serializer = serializer;
    this.defaultCompatibilityLevel = config.compatibilityType();
    this.idCache = new HashMap<Long, SchemaKey>();
    kafkaStore =
        new KafkaStore<SchemaRegistryKey, SchemaRegistryValue>(config,
                                                               new KafkaStoreMessageHandler(this),
                                                               this.serializer,
                                                               new InMemoryStore<SchemaRegistryKey, SchemaRegistryValue>(),
                                                               zkClient);
  }

  @Override
  public void init() throws SchemaRegistryException {
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      throw new SchemaRegistryException("Error while initializing the datastore", e);
    }
    masterElector = new ZookeeperMasterElector(zkClient, myIdentity, this);
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

  public void setMaster(SchemaRegistryIdentity schemaRegistryIdentity)
      throws SchemaRegistryException {
    log.debug("Setting the master to " + schemaRegistryIdentity);
    synchronized (masterLock) {
      masterIdentity = schemaRegistryIdentity;
      if (isMaster()) {
        // To become the master, wait until the Kafka store is fully caught up.
        try {
          kafkaStore.waitUntilBootstrapCompletes();
        } catch (StoreException e) {
          throw new SchemaRegistryException(e);
        }
        schemaIdCounter = new AtomicLong(nextSchemaIdCounterBatch());
        maxSchemaIdCounterValue =
            schemaIdCounter.longValue() + ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
      }
    }
  }

  public SchemaRegistryIdentity myIdentity() {
    return myIdentity;
  }

  public SchemaRegistryIdentity masterIdentity() {
    synchronized (masterLock) {
      return masterIdentity;
    }
  }

  @Override
  public long register(String subject, Schema schema, RegisterSchemaForwardingAgent forwardingAgent)
      throws SchemaRegistryException {
    synchronized (masterLock) {
      if (isMaster()) {
        try {
          org.apache.avro.Schema avroSchemaObj = canonicalizeSchema(schema);

          Iterator<Schema> allVersions = getAllVersions(subject);
          Schema latestSchema = null;
          int newVersion = MIN_VERSION;
          // see if the schema to be registered already exists
          while (allVersions.hasNext()) {
            Schema s = allVersions.next();
            if (!s.getDeprecated()) {
              if (s.getSchema().compareTo(schema.getSchema()) == 0) {
                return s.getVersion();
              }
              latestSchema = s;
            }
            newVersion = s.getVersion() + 1;
          }

          if (latestSchema == null || isCompatible(subject, avroSchemaObj, latestSchema)) {
            SchemaKey keyForNewVersion = new SchemaKey(subject, newVersion);
            schema.setVersion(newVersion);
            schema.setId(schemaIdCounter.getAndIncrement());
            if (schemaIdCounter.get() == maxSchemaIdCounterValue) {
              maxSchemaIdCounterValue =
                  nextSchemaIdCounterBatch().longValue() + ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE;
            }
            kafkaStore.put(keyForNewVersion, schema);
            return schema.getId();
          } else {
            throw new IncompatibleAvroSchemaException(
                "New schema is incompatible with the latest schema " + latestSchema);
          }
        } catch (StoreException e) {
          throw new SchemaRegistryException("Error while registering the schema in the" +
                                            " backend Kafka store", e);
        }
      } else {
        // forward registering request to the master
        if (masterIdentity != null) {
          return forwardingAgent.forward(masterIdentity.getHost(), masterIdentity.getPort());
        } else {
          throw new SchemaRegistryException("Register schema request failed since master is "
                                            + "unknown");
        }
      }
    }
  }

  private Long nextSchemaIdCounterBatch() throws SchemaRegistryException {
    // create ZOOKEEPER_SCHEMA_ID_COUNTER if it already doesn't exist
    long schemaIdCounterThreshold = 0L;
    if (!zkClient.exists(ZOOKEEPER_SCHEMA_ID_COUNTER)) {
      ZkUtils.createPersistentPath(zkClient, ZOOKEEPER_SCHEMA_ID_COUNTER,
                                   String.valueOf(ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE));
    } else {
      // read the latest counter value
      ZkData counterValue = ZkUtils.readData(zkClient, ZOOKEEPER_SCHEMA_ID_COUNTER);
      if (counterValue.getData() != null) {
        schemaIdCounterThreshold = Long.valueOf(counterValue.getData());
      } else {
        throw new SchemaRegistryException("Failed to initialize schema registry. Failed to read "
                                          + "schema id counter " + ZOOKEEPER_SCHEMA_ID_COUNTER +
                                          " from zookeeper");
      }
      ZkUtils.updatePersistentPath(zkClient, ZOOKEEPER_SCHEMA_ID_COUNTER,
                                   String.valueOf(schemaIdCounterThreshold +
                                                  ZOOKEEPER_SCHEMA_ID_COUNTER_BATCH_SIZE));
    }
    return schemaIdCounterThreshold;
  }

  private org.apache.avro.Schema canonicalizeSchema(Schema schema) {
    AvroSchema avroSchema = AvroUtils.parseSchema(schema.getSchema());
    if (avroSchema == null) {
      throw new InvalidAvroException();
    }
    schema.setSchema(avroSchema.canonicalString);
    return avroSchema.schemaObj;
  }

  @Override
  public Schema get(String subject, int version) throws SchemaRegistryException {
    SchemaKey key = new SchemaKey(subject, version);
    try {
      Schema schema = (Schema) kafkaStore.get(key);
      return schema;
    } catch (StoreException e) {
      throw new SchemaRegistryException(
          "Error while retrieving schema from the backend Kafka" +
          " store", e);
    }
  }

  @Override
  public Schema get(long id) throws SchemaRegistryException {
    Schema schema = null;
    try {
      SchemaKey subjectVersionKey = idCache.get(id);
      schema = (Schema) kafkaStore.get(subjectVersionKey);
    } catch (StoreException e) {
      throw new SchemaRegistryException(
          "Error while retrieving schema with id " + id + " from the backend Kafka" +
          " store", e);
    }
    return schema;
  }

  @Override
  public Set<String> listSubjects() throws SchemaRegistryException {
    try {
      Iterator<SchemaRegistryKey> allKeys = kafkaStore.getAllKeys();
      return extractUniqueSubjects(allKeys);
    } catch (StoreException e) {
      throw new SchemaRegistryException(
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
      return sortSchemasByVersion(allVersions);
    } catch (StoreException e) {
      throw new SchemaRegistryException(
          "Error from the backend Kafka store", e);
    }
  }

  @Override
  public void deprecate(String subject, int version) throws SchemaRegistryException {
    SchemaKey key = new SchemaKey(subject, version);
    Schema schema = null;

    synchronized (masterLock) {
      if (isMaster()) {
        try {
          schema = (Schema) kafkaStore.get(key);
          schema.setDeprecated(true);
          kafkaStore.put(key, schema);
        } catch (StoreException e) {
          throw new SchemaRegistryException(
              "Error updating schema deprecation in the backend Kafka store", e);
        }
      } else {
        // TODO: logic to forward will be included as part of issue#35
        throw new SchemaRegistryException("Deprecate request failed since this is not the "
                                          + "master");
      }
    }
  }

  @Override
  public void close() {
    kafkaStore.close();
    if (masterElector != null) {
      masterElector.close();
    }
    if (zkClient != null) {
      zkClient.close();
    }
  }

  public void updateCompatibilityLevel(String subject, AvroCompatibilityLevel newCompatibilityLevel)
      throws SchemaRegistryException {
    synchronized (masterLock) {
      if (isMaster()) {
        ConfigKey configKey = new ConfigKey(subject);
        try {
          kafkaStore.put(configKey, new Config(newCompatibilityLevel));
          log.debug("Wrote new compatibility level: " + newCompatibilityLevel.name + " to the"
                    + " Kafka data store with key " + configKey.toString());
        } catch (StoreException e) {
          throw new SchemaRegistryException("Failed to write new config value to the store", e);
        }
      } else {
        // TODO: logic to forward will be included as part of issue#35
        throw new SchemaRegistryException("Config update request failed since this is not the "
                                          + "master");

      }
    }
  }

  public AvroCompatibilityLevel getCompatibilityLevel(String subject)
      throws SchemaRegistryException {
    ConfigKey subjectConfigKey = new ConfigKey(subject);
    Config config;
    try {
      config = (Config) kafkaStore.get(subjectConfigKey);
      if (config == null && subject == null) {
        // if top level config was never updated, send the configured value for this instance
        config = new Config(this.defaultCompatibilityLevel);
      } else if (config == null) {
        config = new Config();
      }
    } catch (StoreException e) {
      throw new SchemaRegistryException("Failed to read config from the kafka store", e);
    }
    return config.getCompatibilityLevel();
  }

  private Schema getLatestOrExistingSchema(String subject, Schema schema)
      throws SchemaRegistryException {
    Iterator<Schema> allVersions = getAllVersions(subject);
    Schema latestOrExistingSchema = null;
    // see if the schema to be registered already exists
    while (allVersions.hasNext()) {
      Schema s = allVersions.next();
      if (!s.getDeprecated()) {
        if (s.getSchema().compareTo(schema.getSchema()) == 0) {
          latestOrExistingSchema = s;
          break;
        }
      }
      latestOrExistingSchema = s;
    }
    return latestOrExistingSchema;
  }

  private boolean isCompatible(String subject,
                               org.apache.avro.Schema newSchemaObj,
                               Schema latestSchema)
      throws SchemaRegistryException {
    AvroSchema latestAvroSchema = AvroUtils.parseSchema(latestSchema.getSchema());
    if (latestAvroSchema == null) {
      throw new SchemaRegistryException(
          "Existing schema " + latestSchema + " is not a valid Avro schema");
    }
    AvroCompatibilityLevel compatibility = getCompatibilityLevel(subject);
    if (compatibility == null) {
      compatibility = getCompatibilityLevel(null);
    }
    return compatibility.compatibilityChecker
        .isCompatible(newSchemaObj, latestAvroSchema.schemaObj);
  }

  private Iterator<Schema> sortSchemasByVersion(Iterator<SchemaRegistryValue> schemas) {
    Vector<Schema> schemaVector = new Vector<Schema>();
    while (schemas.hasNext()) {
      schemaVector.add((Schema) schemas.next());
    }
    Collections.sort(schemaVector);
    return schemaVector.iterator();
  }
}
