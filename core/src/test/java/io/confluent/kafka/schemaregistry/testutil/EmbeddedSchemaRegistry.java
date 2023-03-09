/*
 * Copyright [2023 - 2023] Confluent Inc.
 */
package io.confluent.kafka.schemaregistry.testutil;

import org.apache.kafka.test.TestUtils;
import org.eclipse.jetty.server.Server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;

import static java.util.Objects.requireNonNull;

public class EmbeddedSchemaRegistry {

    private final String bootstrapServers;
    private Server registryServer;
    private String schemaRegistryUrl;
    public EmbeddedSchemaRegistry(String bootstrapServers) {
        this.bootstrapServers = requireNonNull(bootstrapServers);
    }

    public void start() throws Exception {
        Properties props = new Properties();
        props.setProperty(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(SchemaRegistryConfig.LISTENERS_CONFIG,
                "http://0.0.0.0:" + findAvailableOpenPort());

        SchemaRegistryRestApplication schemaRegistry = new SchemaRegistryRestApplication(
                new SchemaRegistryConfig(props));
        registryServer = schemaRegistry.createServer();
        registryServer.start();

        TestUtils.waitForCondition(() -> registryServer.isRunning(), 10000L,
                "Schema Registry start timed out.");

        schemaRegistryUrl = registryServer.getURI().toString();
    }

    public void stop() throws Exception{
        AutoCloseable closeable = registryServer::stop;
        if(closeable!=null) {
            closeable.close();
        }
    }

    public String schemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    private Integer findAvailableOpenPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
