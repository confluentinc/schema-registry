package io.confluent.kafka.schemaregistry.rest;

import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.rest.ConfigurationException;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  /**
   * Starts an embedded Jetty server running the REST server.
   */
  public static void main(String[] args) throws IOException {

    try {
      SchemaRegistryRestConfiguration config =
          new SchemaRegistryRestConfiguration((args.length > 0 ? args[0] : null));
      SchemaRegistryRestApplication app = new SchemaRegistryRestApplication(config);
      Server server = app.createServer();
      server.start();
      log.info("Server started, listening for requests...");
      server.join();
    } catch (ConfigurationException e) {
      log.error("Server configuration failed: ", e);
      System.exit(1);
    } catch (Exception e) {
      log.error("Server died unexpectedly: ", e);
      System.exit(1);
    }
  }
}
