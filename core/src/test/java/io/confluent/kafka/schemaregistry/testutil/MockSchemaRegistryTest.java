package io.confluent.kafka.schemaregistry.testutil;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.junit.Assert;
import org.junit.Test;

public class MockSchemaRegistryTest {
    @Test
    public void testGetClient() {
        final SchemaRegistryClient client = MockSchemaRegistry.getClientForScope("testGetClient");
        Assert.assertNotNull(client);
    }

    @Test
    public void testGetSameClient() {
        final String scope = "testGetSameClient";
        final SchemaRegistryClient client = MockSchemaRegistry.getClientForScope(scope);
        Assert.assertNotNull(client);
        Assert.assertSame(client, MockSchemaRegistry.getClientForScope(scope));
    }

    @Test
    public void testDropScope() {
        final String scope = "testDropScope";
        final SchemaRegistryClient client = MockSchemaRegistry.getClientForScope(scope);
        Assert.assertNotNull(client);
        MockSchemaRegistry.dropScope(scope);
        Assert.assertNotSame(client, MockSchemaRegistry.getClientForScope(scope));
    }
}
