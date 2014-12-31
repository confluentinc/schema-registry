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
import io.confluent.kafka.schemaregistry.rest.RegisterSchemaForwardingAgent;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
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

  public static final char SCHEMA_KEY_SEPARATOR = ',';
  private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistry.class);
  private final KafkaStore<String, Schema> kafkaStore;
  private final Serializer<Schema> serializer;
  private final SchemaRegistryIdentity myIdentity;
  private final Object masterLock = new Object();
  private final ZkClient zkClient;
  private SchemaRegistryIdentity masterIdentity;
  private ZookeeperMasterElector masterElector = null;

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
    kafkaStore = new KafkaStore<String, Schema>(config, new KafkaStoreMessageHandler(),
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
      if (masterIdentity != null && masterIdentity.equals(myIdentity)) {
        return true;
      } else {
        return false;
      }
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
          kafkaStore.waitUntilBootstrapCompletes();
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
  public int register(String subject, Schema schema, RegisterSchemaForwardingAgent forwardingAgent)
      throws SchemaRegistryException {
    synchronized (masterLock) {
      if (isMaster()) {
        try {
          Iterator<Schema> allVersions = getAllVersions(subject);

          Schema latestSchema = null;
          int latestUsedSchemaVersion = 0;
          // see if the schema to be registered already exists
          while (allVersions.hasNext()) {
            Schema s = allVersions.next();
            if (!s.getDeprecated()) {
              if (s.getSchema().compareTo(schema.getSchema()) == 0) {
                return s.getVersion();
              }
              latestSchema = s;
            }
            latestUsedSchemaVersion = s.getVersion();
          }
          if (latestSchema == null || isCompatible(subject, schema, latestSchema)) {
            int newVersion = latestUsedSchemaVersion + 1;
            String
                keyForNewVersion =
                String.format("%s%c%d", subject, SCHEMA_KEY_SEPARATOR, newVersion);
            schema.setVersion(newVersion);
            kafkaStore.put(keyForNewVersion, schema);
          } else {
            throw new SchemaRegistryException("Incompatible schema");
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
    return schema.getVersion();
  }

  @Override
  public Schema get(String subject, int version) throws SchemaRegistryException {
    String key = subject + SCHEMA_KEY_SEPARATOR + version;
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
  public Set<String> listSubjects() throws SchemaRegistryException {
    try {
      Iterator<String> allKeys = kafkaStore.getAllKeys();
      return extractUniqueSubjects(allKeys);
    } catch (StoreException e) {
      throw new SchemaRegistryException(
          "Error from the backend Kafka store", e);
    }
  }

  private Set<String> extractUniqueSubjects(Iterator<String> allKeys) {
    Set<String> subjects = new HashSet<String>();
    while (allKeys.hasNext()) {
      String key = allKeys.next();
      subjects.add(key.split(String.valueOf(SCHEMA_KEY_SEPARATOR))[0]);
    }
    return subjects;
  }

  @Override
  public Iterator<Schema> getAllVersions(String subject) throws SchemaRegistryException {
    try {
      Iterator<Schema> allVersions = kafkaStore.getAll(String.format("%s%c", subject,
                                                                     SCHEMA_KEY_SEPARATOR),
                                                       String.format("%s%c%c", subject,
                                                                     SCHEMA_KEY_SEPARATOR,
                                                                     '9' + 1));
      return sortSchemasByVersion(allVersions);
    } catch (StoreException e) {
      throw new SchemaRegistryException(
          "Error from the backend Kafka store", e);
    }
  }

  @Override
  public boolean isCompatible(String subject, Schema schema1, Schema schema2) {
    return true;
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

  private Iterator<Schema> sortSchemasByVersion(Iterator<Schema> schemas) {
    Vector<Schema> schemaVector = new Vector<Schema>();
    while (schemas.hasNext()) {
      schemaVector.add(schemas.next());
    }
    Collections.sort(schemaVector);
    return schemaVector.iterator();
  }
}
