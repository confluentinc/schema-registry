package io.confluent.kafka.schemaregistry.storage;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

  private final Map<String, Integer> schemaVersions;
  private final Store<String, Schema> kafkaStore;
  private final Serializer<Schema> serializer;
  private final SchemaRegistryIdentity myIdentity;
  private final Object masterLock = new Object();
  private SchemaRegistryIdentity masterIdentity;
  private ZookeeperMasterElector masterElector = null;
  private final ZkClient zkClient;

  public KafkaSchemaRegistry(SchemaRegistryConfig config, Serializer<Schema> serializer)
      throws SchemaRegistryException {
    String host;
    try {
      host = config.getString(SchemaRegistryConfig.ADVERTISED_HOST_CONFIG);
      if (host.trim().isEmpty())
        host = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new SchemaRegistryException("Unknown hostname while bootstrapping the schema registry", e);
    }

    int port = config.getInt(SchemaRegistryConfig.PORT_CONFIG);
    myIdentity = new SchemaRegistryIdentity(SchemaRegistryIdentity.CURRENT_VERSION, host, port);

    String kafkaClusterZkUrl =
        config.getString(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG);
    int zkSessionTimeoutMs =
        config.getInt(KafkaStoreConfig.KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG);
    this.zkClient = new ZkClient(kafkaClusterZkUrl, zkSessionTimeoutMs, zkSessionTimeoutMs,
                                 new ZkStringSerializer());

    this.serializer = serializer;
    schemaVersions = new HashMap<String, Integer>();
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
    try {
      Iterator<Schema> allSchemas = kafkaStore.getAll(null, null);
      while (allSchemas.hasNext()) {
        Schema schema = allSchemas.next();
        log.debug("Applying schema " + schema.toString() + " to the schema version " +
                  "cache");
        schemaVersions.put(schema.getName(), schema.getVersion());
      }
    } catch (StoreException e) {
      throw new SchemaRegistryException("Error while bootstrapping the schema registry " +
                                        "from the backend Kafka store", e);
    }
    log.trace("Contents of version cache after bootstrap is complete" +
              schemaVersions.toString());

    masterElector = new ZookeeperMasterElector(zkClient, myIdentity, this);
    masterElector.init();
  }

  public boolean isMaster() {
    synchronized (masterLock) {
      if (masterIdentity != null && masterIdentity.equals(myIdentity))
        return true;
      else
        return false;
    }
  }

  public void setMaster(SchemaRegistryIdentity schemaRegistryIdentity) {
    synchronized(masterLock) {
      masterIdentity = schemaRegistryIdentity;
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
  public int register(String topic, Schema schema) throws SchemaRegistryException {
    synchronized (masterLock) {
      if (isMaster()) {
        int latestVersion = 0;
        if (schemaVersions.containsKey(topic)) {
          latestVersion = schemaVersions.get(topic);
        }
        int version = latestVersion + 1;
        String newKeyForLatestSchema = topic + "," + version;
        String keyForLatestSchema = topic + "," + latestVersion;
        Schema latestSchema = null;
        try {
          latestSchema = kafkaStore.get(keyForLatestSchema);
        } catch (StoreException e) {
          throw new SchemaRegistryException("Error while retrieving the latest schema from the" +
                                            " backend Kafka store", e);
        }
        if (isCompatible(topic, schema, latestSchema)) {
          try {
            schema.setVersion(version);
            log.trace("Adding schema to the Kafka store: " + schema.toString());
            kafkaStore.put(newKeyForLatestSchema, schema);
          } catch (StoreException e) {
            throw new SchemaRegistryException("Error while registering the schema in the" +
                                              " backend Kafka store", e);
          }
        }
        schemaVersions.put(topic, version);
        return version;
      } else {
        //TODO: forward registering request to the master
        throw new SchemaRegistryException("Forwarding to master now supported yet");
      }
    }
  }

  @Override
  public Schema get(String topic, int version) throws SchemaRegistryException {
    String key = topic + "," + version;
    Schema schema = null;
    try {
      schema = kafkaStore.get(key);
    } catch (StoreException e) {
      throw new SchemaRegistryException(
          "Error while retrieving schema from the backend Kafka" +
          " store", e);
    }
    return schema;
  }

  @Override
  public Set<String> listTopics() {
    return schemaVersions.keySet();
  }

  @Override
  public Iterator<Schema> getAll(String topic) throws StoreException {
    int earliestVersion = 1;
    int latestVersion = 1;
    if (schemaVersions.containsKey(topic)) {
      latestVersion = schemaVersions.get(topic) + 1;
    }
    String keyEarliestVersion = topic + "," + earliestVersion;
    String keyLatestVersion = topic + "," + latestVersion;
    return kafkaStore.getAll(keyEarliestVersion, keyLatestVersion);
  }

  @Override
  public Iterator<Schema> getAllVersions(String topic) throws StoreException {
    int earliestVersion = 1;
    int latestVersion = 1;
    if (schemaVersions.containsKey(topic)) {
      latestVersion = schemaVersions.get(topic) + 1;
    } else {
      log.trace("Schema for " + topic + " does not exist in version cache. " +
                "Defaulting to version 1 as latest version");
    }
    String keyEarliestVersion = topic + "," + earliestVersion;
    String keyLatestVersion = topic + "," + latestVersion;
    log.trace("Getting schemas between versions: " + earliestVersion + "," + latestVersion);
    return kafkaStore.getAll(keyEarliestVersion, keyLatestVersion);
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
