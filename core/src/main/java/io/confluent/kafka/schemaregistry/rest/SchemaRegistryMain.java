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

package io.confluent.kafka.schemaregistry.rest;

import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.rest.RestConfigException;

public class SchemaRegistryMain {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryMain.class);

  /**
   * Starts an embedded Jetty server running the REST server.
   */
  public static void main(String[] args) throws IOException {

    try {
      if (args.length != 1) {
        log.error("Properties file is required to start the schema registry REST instance");
        System.exit(1);
      }
      SchemaRegistryConfig config = new SchemaRegistryConfig(args[0]);
      SchemaRegistryRestApplication app = new SchemaRegistryRestApplication(config);
      Server server = app.createServer();
      server.start();
      log.info("Server started, listening for requests...");
      server.join();
    } catch (RestConfigException e) {
      log.error("Server configuration failed: ", e);
      System.exit(1);
    } catch (Exception e) {
      log.error("Server died unexpectedly: ", e);
      System.exit(1);
    }
  }
}
