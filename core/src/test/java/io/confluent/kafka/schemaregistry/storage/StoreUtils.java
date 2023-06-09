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
package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.rest.RestConfigException;
import org.apache.kafka.common.security.auth.SecurityProtocol;

/**
 * For all store related utility methods.
 */
public class StoreUtils {

  /**
   * Get a new instance of KafkaStore and initialize it.
   */
  public static KafkaStore<String, String> createAndInitKafkaStoreInstance(String bootstrapServers)
      throws RestConfigException, StoreInitializationException, SchemaRegistryException {
    Store<String, String> inMemoryStore = new InMemoryCache<>(StringSerializer.INSTANCE);
    return createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore);
  }
  /**
   * Get a new instance of KafkaStore and initialize it.
   */
  public static KafkaStore<String, String> createAndInitKafkaStoreInstance(
      String bootstrapServers, Store<String, String> inMemoryStore)
      throws RestConfigException, StoreInitializationException, SchemaRegistryException {
    return createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore,
            new Properties());
  }

  /**
   * Get a new instance of a Kafka and ZooKeeper SASL KafkaStore and initialize it.
   *
   * Because all SASL tests share the same KDC and JAAS file, when testing Kafka
   * or ZooKeeper SASL individually, the other must have SASL enabled.
   */
  public static KafkaStore<String, String> createAndInitSASLStoreInstance(
          String bootstrapServers)
      throws RestConfigException, StoreInitializationException, SchemaRegistryException {
    Properties props = new Properties();

    props.put(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG,
            SecurityProtocol.SASL_PLAINTEXT.toString());

    Store<String, String> inMemoryStore = new InMemoryCache<>(StringSerializer.INSTANCE);
    return createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore, props);
  }

  /**
   * Get a new instance of an SSL KafkaStore and initialize it.
   */
  public static KafkaStore<String, String> createAndInitSSLKafkaStoreInstance(
          String bootstrapServers, Map<String, Object> sslConfigs, boolean requireSSLClientAuth)
      throws RestConfigException, StoreInitializationException, SchemaRegistryException {
    Properties props = new Properties();

    props.put(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG,
            SecurityProtocol.SSL.toString());
    props.put(SchemaRegistryConfig.KAFKASTORE_SSL_TRUSTSTORE_LOCATION_CONFIG,
            sslConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    props.put(SchemaRegistryConfig.KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_CONFIG,
            ((Password)sslConfigs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
    if (requireSSLClientAuth) {
      props.put(SchemaRegistryConfig.KAFKASTORE_SSL_KEYSTORE_LOCATION_CONFIG,
              sslConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
      props.put(SchemaRegistryConfig.KAFKASTORE_SSL_KEYSTORE_PASSWORD_CONFIG,
              ((Password) sslConfigs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).value());
      props.put(SchemaRegistryConfig.KAFKASTORE_SSL_KEY_PASSWORD_CONFIG,
              ((Password) sslConfigs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG)).value());
    }

    Store<String, String> inMemoryStore = new InMemoryCache<>(StringSerializer.INSTANCE);
    return createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore, props);
  }

  /**
   * Get a new instance of KafkaStore and initialize it.
   */
  public static KafkaStore<String, String> createAndInitKafkaStoreInstance(
          String bootstrapServers, Store<String, String> inMemoryStore,
          Properties props)
      throws RestConfigException, StoreInitializationException, SchemaRegistryException {
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);

    SchemaRegistryConfig config = null;
    config = new SchemaRegistryConfig(props);

    KafkaStore<String, String> kafkaStore =
            new KafkaStore<String, String>(config, new StringMessageHandler(),
                    StringSerializer.INSTANCE,
                    inMemoryStore, new NoopKey().toString());
    kafkaStore.init();
    return kafkaStore;
  }
}
