package io.confluent.kafka.schemaregistry.rest.client;


import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidSchemaException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestOperationNotPermittedException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.StoreUtils;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;

import java.io.IOException;
import java.util.*;

import io.confluent.rest.exceptions.RestNotFoundException;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class LocalSchemaRegistryClientTest extends ClusterTestHarness {

    private KafkaSchemaRegistry schemaRegistry;
    private LocalSchemaRegistryClient client;
    private Metadata metadata = new Metadata(null, new HashMap<String, String>(){{put("key1", "value1");}}, null);
    private AvroSchema schema1 = new AvroSchema("{\"type\":\"record\",\"name\":\"myrecord1\", \"fields\":[{\"type\":\"string\",\"name\":\"f1\"}]}", Collections.emptyList(),  new HashMap<String, String>(), metadata, null, 2, true);
    private AvroSchema schema2 = new AvroSchema("{\"type\":\"record\",\"name\":\"myrecord2\",\"fields\":[{\"type\":\"string\",\"name\":\"f1\"}]}");

    private int id1;
    private int id2;

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
        id1 = client.register("subject1", schema1);
        id2 = client.register("subject2", schema2);
    }

    @Test
    public void testRegister_InvalidVersion() {
        // Version is not one more than previous version
        assertThrows(RestInvalidSchemaException.class, ()->client.register("subject1", schema1, 100, -1));
    }

    @Test
    public void testParseSchema() {
        Schema schema = new Schema(
                "subject1",
                -1,
                -1,
                AvroSchema.TYPE,
                Collections.emptyList(),
                StoreUtils.avroSchemaString(1));

        Optional<ParsedSchema> parsedSchema = client.parseSchema(schema);
        assertTrue(parsedSchema.isPresent());
        assertEquals("AVRO", parsedSchema.get().schemaType());
        assertEquals("Foo", parsedSchema.get().name());
    }

    @Test
    public void testGetSchemas() throws Exception {
        // Matches 1 schema.
        List<ParsedSchema> schemas = client.getSchemas("subject1", false, true);
        assertNotNull(schemas);
        assertEquals(1, schemas.size());
        ParsedSchema schema = schemas.get(0);
        assertEquals("AVRO", schema.schemaType());
        assertEquals("myrecord1", schema.name());

        // Matches multiple schemas.
        schemas = client.getSchemas("subject", false, true);
        assertEquals(2, schemas.size());

        // Matches 0 schema.
        schemas = client.getSchemas("subject123", false, true);
        assertEquals(0, schemas.size());
    }

    @Test
    public void testGetAllVersions() throws Exception {
        // Matches 1 schema.
        List<Integer> versions = client.getAllVersions("subject1");
        assertEquals(1, versions.size());
        assertEquals(1, versions.get(0).intValue());
    }

    @Test
    public void testGetAllVersions_NotFound() {
        // Subject doesn't exist.
        assertThrows(RestNotFoundException.class, () -> client.getAllVersions("subject123"));
    }

    @Test
    public void testGetSchemaBySubjectAndId() throws Exception {
        // Schema 1.
        ParsedSchema s1 = client.getSchemaBySubjectAndId("subjetc1", id1);
        assertNotNull(s1);
        assertEquals("myrecord1", s1.name());
        assertEquals("AVRO", s1.schemaType());
        assertEquals("value1", s1.metadata().getProperties().get("key1"));
        // Schema 2.
        ParsedSchema s2 = client.getSchemaBySubjectAndId("subjetc2", id2);
        assertNotNull(s2);
        assertEquals("myrecord2", s2.name());
        assertEquals("AVRO", s2.schemaType());
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

    @Test
    public void testGetConfig() throws Exception {
        client.updateConfig("subject1", new Config("FULL"));
        Config config = client.getConfig("subject1");
        assertEquals("FULL", config.getCompatibilityLevel());
    }

    @Test
    public void testUpdateConfig() throws Exception {
        // Update the config.
        Config config = new Config("FULL");
        client.updateConfig("subject1", config);
        assertEquals("FULL", client.getConfig("subject1").getCompatibilityLevel());
    }

    @Test
    public void testDeleteConfig() throws Exception {
        Config config = new Config("FULL");
        client.updateConfig("subject1", config);
        assertEquals("FULL", client.getConfig("subject1").getCompatibilityLevel());
        client.deleteConfig("subject1");
        // Should throw RestNotFoundException exception.
        assertThrows(RestNotFoundException.class, ()->client.getConfig("subject1"));
    }

    @Test
    public void testSetMode() throws Exception {
        assertEquals("READONLY", client.setMode("READONLY", "subject1", false));
        assertEquals("READONLY", client.getMode("subject1"));
    }

    @Test
    public void testSetMode_NotPermitted() {
        // Can't set to IMPORT mode as there is an existing schema.
        assertThrows(RestOperationNotPermittedException.class, ()->client.setMode("IMPORT", "subject1", false));
    }

    @Test
    public void testDeleteSubject() throws Exception {
        List<Integer> deletedVersions = client.deleteSubject("subject1", false);
        assertEquals(1, deletedVersions.size());
        assertEquals(1, deletedVersions.get(0).intValue());
    }

    @Test
    public void testGetVersion() throws Exception {
        assertEquals(1, client.getVersion("subject1", schema1));
        assertEquals(1, client.getVersion("subject2", schema2));
    }

    @Test
    public void testGetByVersion() throws Exception {
        Schema s1 = client.getByVersion("subject1", 1, false);
        assertEquals(id1, s1.   getId().intValue());
        Schema s2 = client.getByVersion("subject2", 1, false);
        assertEquals(id2, s2.getId().intValue());
    }
}
