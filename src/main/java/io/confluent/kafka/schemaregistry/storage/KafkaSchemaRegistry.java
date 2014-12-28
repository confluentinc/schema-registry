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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import io.confluent.kafka.schemaregistry.rest.RegisterSchemaForwardingAgent;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.kafka.schemaregistry.storage.serialization.StringSerializer;
import io.confluent.kafka.schemaregistry.storage.serialization.ZkStringSerializer;
import io.confluent.kafka.schemaregistry.zookeeper.SchemaRegistryIdentity;
import io.confluent.kafka.schemaregistry.zookeeper.ZookeeperMasterElector;

public class KafkaSchemaRegistry implements SchemaRegistry {

  private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistry.class);
  private static final char SEPARATOR = ',';
  private final KafkaStore<String, Schema> kafkaStore;
  private final Serializer<Schema> serializer;
  private final SchemaRegistryIdentity myIdentity;
  private final Object masterLock = new Object();
  private SchemaRegistryIdentity masterIdentity;
  private ZookeeperMasterElector masterElector = null;
  private final ZkClient zkClient;

  public KafkaSchemaRegistry(SchemaRegistryConfig config, Serializer<Schema> serializer)
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
    kafkaStore = new KafkaStore<String, Schema>(config,
                                                StringSerializer.INSTANCE, this.serializer,
                                                new InMemoryStore<String, Schema>(), zkClient);
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

  @Override
  public boolean isMaster() {
    synchronized (masterLock) {
      if (masterIdentity != null && masterIdentity.equals(myIdentity))
        return true;
      else
        return false;
    }
  }

  @Override
  public void setMaster(SchemaRegistryIdentity schemaRegistryIdentity)
      throws SchemaRegistryException {
    log.debug("Setting the master to " + schemaRegistryIdentity);
    synchronized (masterLock) {
      masterIdentity = schemaRegistryIdentity;
      if (isMaster()) {
        // To become the master, wait until the Kafka store is fully caught up.
        try {
          kafkaStore.waitUntilFullyCaughtUp();
        } catch (StoreException e) {
          throw new SchemaRegistryException(e);
        }
      }
    }
  }

  @Override
  public SchemaRegistryIdentity myIdentity() {
    return myIdentity;
  }

  @Override
  public SchemaRegistryIdentity masterIdentity() {
    synchronized (masterLock) {
      return masterIdentity;
    }
  }

  @Override
  public int register(String topic, Schema schema, RegisterSchemaForwardingAgent forwardingAgent)
      throws SchemaRegistryException {
    synchronized (masterLock) {
      if (isMaster()) {
        try {
          Iterator<Schema> allVersions = getAllVersions(topic);

          Schema latestSchema = null;
          int latestUsedSchemaVersion = 0;
          // see if the schema to be registered already exists
          while (allVersions.hasNext()) {
            Schema s = allVersions.next();
            if (!s.getDeprecated()) {
              if (s.getSchema().compareTo(schema.getSchema()) == 0)
                return s.getVersion();
              latestSchema = s;
            }
            latestUsedSchemaVersion = s.getVersion();
          }
          if (latestSchema == null || isCompatible(topic, schema, latestSchema)) {
            int newVersion = latestUsedSchemaVersion + 1;
            String keyForNewVersion = String.format("%s%c%d", topic, SEPARATOR, newVersion);
            schema.setVersion(newVersion);
            kafkaStore.put(keyForNewVersion, schema);
            return newVersion;
          } else
            throw new SchemaRegistryException("Incompatible schema");
        } catch (StoreException e) {
          throw new SchemaRegistryException("Error while registering the schema in the" +
                                            " backend Kafka store", e);
        }
      } else {
        // forward registering request to the master
        if (masterIdentity != null)
          return forwardingAgent.forward(masterIdentity.getHost(), masterIdentity.getPort());
        else
          throw new SchemaRegistryException("Unknown master while forwarding the request");
      }
    }
  }

  @Override
  public Schema get(String topic, int version) throws SchemaRegistryException {
    String key = topic + "," + version;
    try {
      Schema schema = kafkaStore.get(key);
      return schema;
    } catch (StoreException e) {
      throw new SchemaRegistryException(
          "Error while retrieving schema from the backend Kafka" +
          " store", e);
    }
  }

  @Override
  public Set<String> listTopics() throws SchemaRegistryException {
    try {
      Iterator<String> allKeys = kafkaStore.getAllKeys();
      return extractUniqueTopics(allKeys);
    } catch (StoreException e) {
      throw new SchemaRegistryException(
          "Error from the backend Kafka store", e);
    }
  }

  private Set<String> extractUniqueTopics(Iterator<String> allKeys) {
    Set<String> topics = new HashSet<String>();
    while (allKeys.hasNext())
      topics.add(allKeys.next().split(String.valueOf(SEPARATOR))[0]);
    return topics;
  }

  @Override
  public Iterator<Schema> getAllVersions(String topic) throws SchemaRegistryException {
    try {
      Iterator<Schema> allVersions = kafkaStore.getAll(String.format("%s%c", topic, SEPARATOR),
                                                       String.format("%s%c%c", topic, SEPARATOR,
                                                                     '9' + 1));
      return sortSchemasByVersion(allVersions);
    } catch (StoreException e) {
      throw new SchemaRegistryException(
          "Error from the backend Kafka store", e);
    }
  }

  private Iterator<Schema> sortSchemasByVersion(Iterator<Schema> schemas) {
    Vector<Schema> schemaVector = new Vector<Schema>();
    while (schemas.hasNext())
      schemaVector.add(schemas.next());
    Collections.sort(schemaVector);
    return schemaVector.iterator();
  }

  @Override
  public boolean isCompatible(String topic, Schema schema1, Schema schema2) {
    return true;
  }

  @Override
  public void close() {
    kafkaStore.close();
    if (masterElector != null)
      masterElector.close();
    if (zkClient != null)
      zkClient.close();
  }
}
