/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.confluent.kafka.schemaregistry;


import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;
import org.eclipse.jetty.server.Server;

import java.util.Properties;

public class RestApp {

  public final Properties prop;
  public RestService restClient;
  public SchemaRegistryRestApplication restApp;
  public Server restServer;
  public String restConnect;

  public RestApp(int port, String zkConnect, String kafkaTopic) {
    this(port, zkConnect, kafkaTopic, CompatibilityLevel.NONE.name, null);
  }

  public RestApp(int port, String zkConnect, String kafkaTopic, String compatibilityType, Properties schemaRegistryProps) {
    this(port, zkConnect, null, kafkaTopic, compatibilityType, true, schemaRegistryProps);
  }

  public RestApp(int port,
                 String zkConnect, String kafkaTopic,
                 String compatibilityType, boolean leaderEligibility, Properties schemaRegistryProps) {
    this(port, zkConnect, null, kafkaTopic, compatibilityType,
        leaderEligibility, schemaRegistryProps);
  }

  public RestApp(int port,
                 String zkConnect, String bootstrapBrokers,
                 String kafkaTopic, String compatibilityType, boolean leaderEligibility,
                 Properties schemaRegistryProps) {
    prop = new Properties();
    if (schemaRegistryProps != null) {
      prop.putAll(schemaRegistryProps);
    }
    prop.setProperty(SchemaRegistryConfig.PORT_CONFIG, ((Integer) port).toString());
    if (zkConnect != null) {
      prop.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    }
    if (bootstrapBrokers != null) {
      prop.setProperty(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    }
    prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, kafkaTopic);
    prop.put(SchemaRegistryConfig.SCHEMA_COMPATIBILITY_CONFIG, compatibilityType);
    prop.put(SchemaRegistryConfig.LEADER_ELIGIBILITY, leaderEligibility);
  }

  public void start() throws Exception {
    restApp = new SchemaRegistryRestApplication(prop);
    restServer = restApp.createServer();
    restServer.start();
    restApp.postServerStart();
    restConnect = restServer.getURI().toString();
    if (restConnect.endsWith("/"))
      restConnect = restConnect.substring(0, restConnect.length()-1);
    restClient = new RestService(restConnect);
  }

  public void stop() throws Exception {
    if (restClient != null) {
      restClient.close();
      restClient = null;
    }
    if (restServer != null) {
      restServer.stop();
      restServer.join();
    }
  }

  /**
   * This method must be called before calling {@code RestApp.start()}
   * for the additional properties to take affect.
   *
   * @param props the additional properties to set
   */
  public void addConfigs(Properties props) {
    prop.putAll(props);
  }

  public boolean isLeader() {
    return restApp.schemaRegistry().isLeader();
  }

  public void setLeader(SchemaRegistryIdentity schemaRegistryIdentity)
      throws SchemaRegistryException {
    restApp.schemaRegistry().setLeader(schemaRegistryIdentity);
  }

  public SchemaRegistryIdentity myIdentity() {
    return restApp.schemaRegistry().myIdentity();
  }

  public SchemaRegistryIdentity leaderIdentity() {
    return restApp.schemaRegistry().leaderIdentity();
  }
  
  public SchemaRegistry schemaRegistry() {
    return restApp.schemaRegistry();
  }
}
