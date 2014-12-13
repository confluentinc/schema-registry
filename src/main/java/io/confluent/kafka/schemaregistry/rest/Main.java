package io.confluent.kafka.schemaregistry.rest;

import io.confluent.rest.ConfigurationException;
import org.eclipse.jetty.server.Server;

import java.io.IOException;

public class Main {
    /**
     * Starts an embedded Jetty server running the REST server.
     */
    public static void main(String[] args) throws IOException {
        try {
            SchemaRegistryRestConfiguration config = new SchemaRegistryRestConfiguration((args.length > 0 ? args[0] : null));
            SchemaRegistryRestApplication app = new SchemaRegistryRestApplication(config);
            Server server = app.createServer();
            server.start();
            System.out.println("Server started, listening for requests...");
            server.join();
        } catch (ConfigurationException e) {
            System.out.println("Server configuration failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Server died unexpectedly: " + e.toString());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
