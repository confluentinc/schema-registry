package io.confluent.kafka.schemaregistry.rest.client;


import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;

import java.util.*;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class LocalSchemaRegistryClientTest extends ClusterTestHarness {

    private KafkaSchemaRegistry schemaRegistry;
    private LocalSchemaRegistryClient client;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Properties props = new Properties();
        props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);
        props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        SchemaRegistryConfig config = new SchemaRegistryConfig(props);
        schemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
        schemaRegistry.init();

        client = new LocalSchemaRegistryClient(schemaRegistry);
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        Metadata metadata = new Metadata(null, map, null);
        client.register("subject1", new AvroSchema("{\"type\":\"record\",\"name\":\"myrecord1\", \"fields\":[{\"type\":\"string\",\"name\":\"f1\"}]}", Collections.emptyList(),  new HashMap<String, String>(), metadata, null, 2, true));
        client.register("subject2", new AvroSchema("{\"type\":\"record\",\"name\":\"myrecord2\",\"fields\":[{\"type\":\"string\",\"name\":\"f1\"}]}"));
    }

    @Test
    public void testGetSchemas() throws Exception {
        List<ParsedSchema> schemas = client.getSchemas("subject1", false, true);
        assertNotNull(schemas);
        assertEquals(1, schemas.size());
        ParsedSchema schema = schemas.get(0);
        assertEquals("AVRO", schema.schemaType());
        assertEquals("myrecord1", schema.name());

        // Expect multiple schemas.
        schemas = client.getSchemas("subject", false, true);
        assertEquals(2, schemas.size());
    }

    @Test
    public void testGetSchemaMetadata() throws Exception {
        SchemaMetadata sm = client.getSchemaMetadata("subject1", 1,true);
        assertNotNull(sm);
        assertEquals("AVRO", sm.getSchemaType());
        assertEquals("subject1", sm.getSubject());
        assertEquals(1, sm.getVersion());
        Metadata m = sm.getMetadata();
        assertEquals(1, m.getProperties().size());
        assertEquals("value1", m.getProperties().get("key1"));
    }
}
