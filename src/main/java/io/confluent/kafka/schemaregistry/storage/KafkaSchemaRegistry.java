package io.confluent.kafka.schemaregistry.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class KafkaSchemaRegistry implements SchemaRegistry {

  private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistry.class);

  private final Map<String, Integer> schemaVersions;
  private final Store<String, Schema> kafkaStore;
  private final Serializer<Schema> serializer;

  public KafkaSchemaRegistry(SchemaRegistryConfig config, Serializer<Schema> serializer)
      throws SchemaRegistryException {
    this.serializer = serializer;
    schemaVersions = new HashMap<String, Integer>();
    StringSerializer stringSerializer = new StringSerializer();
    kafkaStore = new KafkaStore<String, Schema>(config,
                                                stringSerializer, this.serializer,
                                                new InMemoryStore<String, Schema>());
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
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
  }

  @Override
  public int register(String topic, Schema schema) throws SchemaRegistryException {
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
  }
}
