/**
 * Copyright 2014-2016 Confluent Inc.
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

import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.protocol.SecurityProtocol;

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
    return createAndInitKafkaStoreInstance(zkConnect, zkClient, inMemoryStore,
            new Properties());
  }

  /**
   * Get a new instance of a Kafka and ZooKeeper SASL KafkaStore and initialize it.
   *
   * Because all SASL tests share the same KDC and JAAS file, when testing Kafka
   * or ZooKeeper SASL individually, the other must have SASL enabled.
   */
  public static KafkaStore<String, String> createAndInitSASLStoreInstance(
          String zkConnect, ZkClient zkClient) {
    Properties props = new Properties();

    props.put(KafkaStoreConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG,
            SecurityProtocol.SASL_PLAINTEXT.toString());

    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    return createAndInitKafkaStoreInstance(zkConnect, zkClient, inMemoryStore, props);
  }

  /**
   * Get a new instance of an SSL KafkaStore and initialize it.
   */
  public static KafkaStore<String, String> createAndInitSSLKafkaStoreInstance(
          String zkConnect, ZkClient zkClient, Map<String, Object> sslConfigs,
          boolean requireSSLClientAuth) {
    Properties props = new Properties();

    props.put(KafkaStoreConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.toString());
    props.put(KafkaStoreConfig.KAFKASTORE_SSL_TRUSTSTORE_LOCATION_CONFIG,
        sslConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    props.put(KafkaStoreConfig.KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_CONFIG,
        ((Password) sslConfigs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
    if (requireSSLClientAuth) {
      props.put(KafkaStoreConfig.KAFKASTORE_SSL_KEYSTORE_LOCATION_CONFIG,
          sslConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
      props.put(KafkaStoreConfig.KAFKASTORE_SSL_KEYSTORE_PASSWORD_CONFIG,
          ((Password) sslConfigs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).value());
      props.put(KafkaStoreConfig.KAFKASTORE_SSL_KEY_PASSWORD_CONFIG,
          ((Password) sslConfigs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG)).value());
    }

    Store<String, String> inMemoryStore = new InMemoryStore<String, String>();
    return createAndInitKafkaStoreInstance(zkConnect, zkClient, inMemoryStore, props);
  }

  /**
   * Get a new instance of KafkaStore and initialize it.
   */
  public static KafkaStore<String, String> createAndInitKafkaStoreInstance(
          String zkConnect, ZkClient zkClient, Store<String, String> inMemoryStore,
          Properties props) {
    props.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);
    props.put(KafkaStoreConfig.KAFKASTORE_KAFKA_GROUPID, "test-group");

    KafkaStoreConfig config = new KafkaStoreConfig(props);

    boolean isZkSecurityEnabled = false;

    KafkaStore<String, String> kafkaStore =
            new KafkaStore<>(config, new StringMessageHandler(),
                    StringSerializer.INSTANCE,
                    inMemoryStore, "NoopKey", isZkSecurityEnabled);
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      fail("Kafka store failed to initialize");
    }
    return kafkaStore;
  }

}
