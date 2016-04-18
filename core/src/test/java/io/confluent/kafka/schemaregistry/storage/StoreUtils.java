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
package io.confluent.kafka.schemaregistry.storage;

import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.rest.RestConfigException;

import static org.junit.Assert.fail;

/**
 * For all store related utility methods.
 */
public class StoreUtils {

  /**
   * Get a new instance of KafkaStore and initialize it.
   */
  public static KafkaStore<String, String> createAndInitKafkaStoreInstance(
      String zkConnect, ZkClient zkClient) {
    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    return createAndInitKafkaStoreInstance(zkConnect, zkClient, inMemoryStore);
  }

  /**
   * Get a new instance of KafkaStore and initialize it.
   */
  public static KafkaStore<String, String> createAndInitKafkaStoreInstance(
      String zkConnect, ZkClient zkClient, Store<String, String> inMemoryStore) {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);

    SchemaRegistryConfig config = null;
    try {
      config = new SchemaRegistryConfig(props);
    } catch (RestConfigException e) {
      fail("Can't initialize configs");
    }

    KafkaStore<String, String> kafkaStore =
        new KafkaStore<String, String>(config, new StringMessageHandler(),
                                       StringSerializer.INSTANCE,
                                       inMemoryStore, new NoopKey().toString());
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
    return kafkaStore;
  }

}
