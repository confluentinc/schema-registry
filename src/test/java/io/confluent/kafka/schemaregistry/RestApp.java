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
package io.confluent.kafka.schemaregistry;


import org.eclipse.jetty.server.Server;

import java.util.Properties;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.zookeeper.SchemaRegistryIdentity;

public class RestApp {

  public final Properties prop;
  public final String restConnect;
  public SchemaRegistryRestApplication restApp;
  public Server restServer;

  public RestApp(int port, String zkConnect, String kafkaTopic) {
    prop = new Properties();
    prop.setProperty(SchemaRegistryConfig.PORT_CONFIG, ((Integer) port).toString());
    prop.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, kafkaTopic);
    restConnect = String.format("http://localhost:%d", port);
  }

  public void start() throws Exception {
    restApp = new SchemaRegistryRestApplication(prop);
    restServer = restApp.createServer();
    restServer.start();
  }

  public void stop() throws Exception {
    restServer.stop();
    restServer.join();
    //TODO: hack to close the underlying resources during shutdown; need to fix rest util
    restApp.schemaRegistry().close();
  }

  public boolean isMaster() {
    return restApp.schemaRegistry().isMaster();
  }

  public void setMaster(SchemaRegistryIdentity schemaRegistryIdentity)
      throws SchemaRegistryException {
    restApp.schemaRegistry().setMaster(schemaRegistryIdentity);
  }

  public SchemaRegistryIdentity myIdentity() {
    return restApp.schemaRegistry().myIdentity();
  }

  public SchemaRegistryIdentity masterIdentity() {
    return restApp.schemaRegistry().masterIdentity();
  }
}
