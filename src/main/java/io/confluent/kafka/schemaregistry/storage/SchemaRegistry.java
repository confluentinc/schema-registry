package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.kafka.schemaregistry.storage.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SchemaRegistry {
    private final Map<String, Integer> schemaVersions;
    private final Store<String, Schema> kafkaStore;
    private final Serializer<Schema> serializer;

    public SchemaRegistry(SchemaRegistryConfig config, Serializer<Schema> serializer) {
        this.serializer = serializer;
        schemaVersions = new HashMap<String, Integer>();
        StringSerializer stringSerializer = new StringSerializer();
        kafkaStore = new KafkaStore<String, Schema>(config,
            stringSerializer, this.serializer, new InMemoryStore<String, Schema>());
        try {
            kafkaStore.init();
        } catch (StoreInitializationException e) {
        }
        try {
            Iterator<Schema> allSchemas = kafkaStore.getAll(null, null);
            while(allSchemas.hasNext()) {
                Schema schema = allSchemas.next();
                System.out.println("Applying schema " + schema.toString() + " to the schema version " +
                    "cache");
                schemaVersions.put(schema.getName(), schema.getVersion());
            }
        } catch (StoreException e) {
            // TODO: throw meaningful exception
            e.printStackTrace();
        }
        System.out.println("Current contents of versionc cache" + schemaVersions.toString());
    }

    public int register(String topic, Schema schema) {
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
            // TODO: Throw a meaningful exception
            e.printStackTrace();
            return -1;
        }
        if (isCompatible(topic, schema, latestSchema)) {
            try {
                schema.setVersion(version);
                System.out.println("Adding schema to the Kafka store: " + schema.toString());
                kafkaStore.put(newKeyForLatestSchema, schema);
            } catch (StoreException e) {
                e.printStackTrace();
                return -1;
            }
        }
        schemaVersions.put(topic, version);
        return version;
    }

    public Schema get(String topic, int version) {
        String key = topic + "," + version;
        Schema schema = null;
        try {
            schema = kafkaStore.get(key);
        } catch (StoreException e) {
            // TODO: Throw a meaningful exception
            e.printStackTrace();
        }
        return schema;
    }

    public Set<String> listTopics() {
        return schemaVersions.keySet();
    }

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

    public Iterator<Schema> getAllVersions(String topic) throws StoreException {
        int earliestVersion = 1;
        int latestVersion = 1;
        if (schemaVersions.containsKey(topic)) {
            latestVersion = schemaVersions.get(topic) + 1;
        } else {
            System.err.println("Schema for " + topic + " does not exist in version cache. " +
                "Defaulting to version 1 as latest version");
        }
        String keyEarliestVersion = topic + "," + earliestVersion;
        String keyLatestVersion = topic + "," + latestVersion;
        System.out.println("Getting schemas between versions: " + earliestVersion + "," + latestVersion);
        return kafkaStore.getAll(keyEarliestVersion, keyLatestVersion);
    }

    public boolean isCompatible(String topic, Schema schema1, Schema schema2) {
        return true;
    }

    public void close() {
        kafkaStore.close();
    }
}
