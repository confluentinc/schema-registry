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

import io.confluent.rest.RestConfig;
import kafka.cluster.EndPoint;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class KafkaStore<K, V> implements Store<K, V> {

  private static final Logger log = LoggerFactory.getLogger(KafkaStore.class);
  private static final Set<SecurityProtocol> SUPPORTED_SECURITY_PROTOCOLS = new HashSet<>(
      Arrays
          .asList(SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL, SecurityProtocol.SASL_PLAINTEXT,
                  SecurityProtocol.SASL_SSL)
  );

  private final String kafkaClusterZkUrl;
  private final String topic;
  private final int desiredReplicationFactor;
  private final String groupId;
  private final StoreUpdateHandler<K, V> storeUpdateHandler;
  private final Serializer<K, V> serializer;
  private final Store<K, V> localStore;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final int initTimeout;
  private final int timeout;
  private final String bootstrapBrokers;
  private KafkaProducer<byte[], byte[]> producer;
  private KafkaStoreReaderThread<K, V> kafkaTopicReader;
  // Noop key is only used to help reliably determine last offset; reader thread ignores
  // messages with this key
  private final K noopKey;
  private volatile long lastWrittenOffset = -1L;
  private final SchemaRegistryConfig config;

  public KafkaStore(SchemaRegistryConfig config,
                    StoreUpdateHandler<K, V> storeUpdateHandler,
                    Serializer<K, V> serializer,
                    Store<K, V> localStore,
                    K noopKey) {
    this.kafkaClusterZkUrl =
        config.getString(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG);
    this.topic = config.getString(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG);
    this.desiredReplicationFactor =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG);
    int port = KafkaSchemaRegistry.getPortForIdentity(
        config.getInt(SchemaRegistryConfig.PORT_CONFIG),
            config.getList(RestConfig.LISTENERS_CONFIG));
    this.groupId = config.getString(SchemaRegistryConfig.KAFKASTORE_GROUP_ID_CONFIG).isEmpty()
                   ? String.format("schema-registry-%s-%d",
                                 config.getString(SchemaRegistryConfig.HOST_NAME_CONFIG),
                                 port)
                   : config.getString(SchemaRegistryConfig.KAFKASTORE_GROUP_ID_CONFIG);
    initTimeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
    timeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG);
    this.storeUpdateHandler = storeUpdateHandler;
    this.serializer = serializer;
    this.localStore = localStore;
    this.noopKey = noopKey;
    this.config = config;

    int zkSessionTimeoutMs =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG);

    List<String>
        bootstrapServersConfig =
        config.getList(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG);
    List<String> endpoints;
    if (bootstrapServersConfig.isEmpty()) {
      ZkUtils zkUtils = null;
      try {
        zkUtils = ZkUtils.apply(
            kafkaClusterZkUrl, zkSessionTimeoutMs, zkSessionTimeoutMs,
            KafkaSchemaRegistry.checkZkAclConfig(this.config));
        Seq<Broker> brokerSeq = zkUtils.getAllBrokersInCluster();
        endpoints = brokersToEndpoints(JavaConversions.seqAsJavaList(brokerSeq));
      } finally {
        if (zkUtils != null) {
          zkUtils.close();
        }
        log.debug("Kafka store zookeeper client shut down");
      }
    } else {
      endpoints = bootstrapServersConfig;
    }
    this.bootstrapBrokers =
        endpointsToBootstrapServers(endpoints, config
            .getString(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG));
    log.info("Initializing KafkaStore with broker endpoints: " + this.bootstrapBrokers);
  }

  @Override
  public void init() throws StoreInitializationException {
    if (initialized.get()) {
      throw new StoreInitializationException(
          "Illegal state while initializing store. Store was already initialized");
    }

    createOrVerifySchemaTopic();

    // set the producer properties and initialize a Kafka producer client
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    props.put(ProducerConfig.ACKS_CONFIG, "-1");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.ByteArraySerializer.class);
    props.put(ProducerConfig.RETRIES_CONFIG, 0); // Producer should not retry

    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
              this.config.getString(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG));
    addSecurityConfigsToClientProperties(this.config, props);

    producer = new KafkaProducer<byte[], byte[]>(props);

    // start the background thread that subscribes to the Kafka topic and applies updates.
    // the thread must be created after the schema topic has been created.
    this.kafkaTopicReader =
        new KafkaStoreReaderThread<>(this.bootstrapBrokers, topic, groupId,
                                     this.storeUpdateHandler, serializer, this.localStore,
                                     this.noopKey, this.config);
    this.kafkaTopicReader.start();

    try {
      waitUntilKafkaReaderReachesLastOffset(initTimeout);
    } catch (StoreException e) {
      throw new StoreInitializationException(e);
    }

    boolean isInitialized = initialized.compareAndSet(false, true);
    if (!isInitialized) {
      throw new StoreInitializationException("Illegal state while initializing store. Store "
                                             + "was already initialized");
    }
  }

  public static void addSecurityConfigsToClientProperties(SchemaRegistryConfig config,
                                                          Properties props) {
    addSslConfigsToClientProperties(config, props);
    addSaslConfigsToClientProperties(config, props);
  }

  public static void addSslConfigsToClientProperties(SchemaRegistryConfig config,
                                                     Properties props) {
    if (config.getString(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG).equals(
        SecurityProtocol.SSL.toString())
        || config.getString(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG).equals(
            SecurityProtocol.SASL_SSL.toString())) {
      props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                config.getString(SchemaRegistryConfig.KAFKASTORE_SSL_TRUSTSTORE_LOCATION_CONFIG));
      props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                config.getString(SchemaRegistryConfig.KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_CONFIG));
      props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                config.getString(SchemaRegistryConfig.KAFKASTORE_SSL_TRUSTSTORE_TYPE_CONFIG));
      props.put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                config
                    .getString(SchemaRegistryConfig.KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
      putIfNotEmptyString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                          config.getString(
                              SchemaRegistryConfig.KAFKASTORE_SSL_KEYSTORE_LOCATION_CONFIG), props);
      putIfNotEmptyString(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                          config.getString(
                              SchemaRegistryConfig.KAFKASTORE_SSL_KEYSTORE_PASSWORD_CONFIG), props);
      props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                config.getString(SchemaRegistryConfig.KAFKASTORE_SSL_KEYSTORE_TYPE_CONFIG));
      props.put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
                config.getString(SchemaRegistryConfig.KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_CONFIG));
      putIfNotEmptyString(SslConfigs.SSL_KEY_PASSWORD_CONFIG,
                          config.getString(SchemaRegistryConfig.KAFKASTORE_SSL_KEY_PASSWORD_CONFIG),
                          props);
      putIfNotEmptyString(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                          config.getString(
                              SchemaRegistryConfig.KAFKASTORE_SSL_ENABLED_PROTOCOLS_CONFIG), props);
      props.put(SslConfigs.SSL_PROTOCOL_CONFIG,
                config.getString(SchemaRegistryConfig.KAFKASTORE_SSL_PROTOCOL_CONFIG));
      putIfNotEmptyString(SslConfigs.SSL_PROVIDER_CONFIG,
                          config.getString(SchemaRegistryConfig.KAFKASTORE_SSL_PROVIDER_CONFIG),
                          props);
      putIfNotEmptyString(SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                          config
                              .getString(SchemaRegistryConfig.KAFKASTORE_SSL_CIPHER_SUITES_CONFIG),
                          props);
      putIfNotEmptyString(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                          config.getString(
                              SchemaRegistryConfig
                                  .KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG),
                          props);
    }
  }

  public static void addSaslConfigsToClientProperties(SchemaRegistryConfig config,
                                                      Properties props) {
    if (config.getString(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG).equals(
        SecurityProtocol.SASL_PLAINTEXT.toString())
        || config.getString(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG).equals(
            SecurityProtocol.SASL_SSL.toString())) {
      putIfNotEmptyString(SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
                          config.getString(
                              SchemaRegistryConfig.KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_CONFIG),
                          props);
      props.put(SaslConfigs.SASL_MECHANISM,
                config.getString(SchemaRegistryConfig.KAFKASTORE_SASL_MECHANISM_CONFIG));
      props.put(SaslConfigs.SASL_KERBEROS_KINIT_CMD,
                config.getString(SchemaRegistryConfig.KAFKASTORE_SASL_KERBEROS_KINIT_CMD_CONFIG));
      props.put(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
                config.getLong(
                    SchemaRegistryConfig.KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG));
      props.put(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER,
                config.getDouble(
                    SchemaRegistryConfig.KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG));
      props.put(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,
                config.getDouble(
                    SchemaRegistryConfig.KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG)
      );
    }
  }

  // helper method to only add a property if its not the empty string. This is required
  // because some Kafka client configs expect a null default value, yet ConfigDef doesn't
  // support null default values.
  private static void putIfNotEmptyString(String parameter, String value, Properties props) {
    if (!value.trim().isEmpty()) {
      props.put(parameter, value);
    }
  }

  private void createOrVerifySchemaTopic() throws StoreInitializationException {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG,
              this.config.getString(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG));
    addSecurityConfigsToClientProperties(this.config, props);

    try (AdminClient admin = AdminClient.create(props)) {
      //
      Set<String> allTopics = admin.listTopics().names().get(initTimeout, TimeUnit.MILLISECONDS);
      if (allTopics.contains(topic)) {
        verifySchemaTopic(admin);
      } else {
        createSchemaTopic(admin);
      }
    } catch (TimeoutException e) {
      throw new StoreInitializationException(
          "Timed out trying to create or validate schema topic configuration",
          e
      );
    } catch (InterruptedException | ExecutionException e) {
      throw new StoreInitializationException(
          "Failed trying to create or validate schema topic configuration",
          e
      );
    }
  }

  private void createSchemaTopic(AdminClient admin) throws StoreInitializationException,
                                                           InterruptedException,
                                                           ExecutionException,
                                                           TimeoutException {
    log.info("Creating schemas topic {}", topic);

    int numLiveBrokers = admin.describeCluster().nodes()
        .get(initTimeout, TimeUnit.MILLISECONDS).size();
    if (numLiveBrokers <= 0) {
      throw new StoreInitializationException("No live Kafka brokers");
    }

    int schemaTopicReplicationFactor = Math.min(numLiveBrokers, desiredReplicationFactor);
    if (schemaTopicReplicationFactor < desiredReplicationFactor) {
      log.warn("Creating the schema topic "
               + topic
               + " using a replication factor of "
               + schemaTopicReplicationFactor
               + ", which is less than the desired one of "
               + desiredReplicationFactor + ". If this is a production environment, it's "
               + "crucial to add more brokers and increase the replication factor of the topic.");
    }

    NewTopic schemaTopicRequest = new NewTopic(topic, 1, (short) schemaTopicReplicationFactor);
    schemaTopicRequest.configs(
        Collections.singletonMap(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_COMPACT
        )
    );
    try {
      admin.createTopics(Collections.singleton(schemaTopicRequest)).all()
          .get(initTimeout, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        // This is ok.
      } else {
        throw e;
      }
    }
  }

  private void verifySchemaTopic(AdminClient admin) throws StoreInitializationException,
                                                           InterruptedException,
                                                           ExecutionException,
                                                           TimeoutException {
    log.info("Validating schemas topic {}", topic);

    Set<String> topics = Collections.singleton(topic);
    Map<String, TopicDescription> topicDescription = admin.describeTopics(topics)
        .all().get(initTimeout, TimeUnit.MILLISECONDS);

    TopicDescription description = topicDescription.get(topic);
    final int numPartitions = description.partitions().size();
    if (numPartitions != 1) {
      throw new StoreInitializationException("The schema topic " + topic + " should have only 1 "
                                             + "partition but has " + numPartitions);
    }

    if (description.partitions().get(0).replicas().size() < desiredReplicationFactor) {
      log.warn("The replication factor of the schema topic "
               + topic
               + " is less than the desired one of "
               + desiredReplicationFactor
               + ". If this is a production environment, it's crucial to add more brokers and "
               + "increase the replication factor of the topic.");
    }

    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

    Map<ConfigResource, Config> configs =
        admin.describeConfigs(Collections.singleton(topicResource)).all()
            .get(initTimeout, TimeUnit.MILLISECONDS);
    Config topicConfigs = configs.get(topicResource);
    String retentionPolicy = topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
    if (retentionPolicy == null || !TopicConfig.CLEANUP_POLICY_COMPACT.equals(retentionPolicy)) {
      log.error("The retention policy of the schema topic " + topic + " is incorrect. "
                + "You must configure the topic to 'compact' cleanup policy to avoid Kafka "
                + "deleting your schemas after a week. "
                + "Refer to Kafka documentation for more details on cleanup policies");

      throw new StoreInitializationException("The retention policy of the schema topic " + topic
                                             + " is incorrect. Expected cleanup.policy to be "
                                             + "'compact' but it is " + retentionPolicy);

    }
  }

  static List<String> brokersToEndpoints(List<Broker> brokers) {
    final List<String> endpoints = new LinkedList<>();
    for (Broker broker : brokers) {
      for (EndPoint ep : JavaConversions.asJavaCollection(broker.endPoints())) {
        String
            hostport =
            ep.host() == null ? ":" + ep.port() : Utils.formatAddress(ep.host(), ep.port());
        String endpoint = ep.securityProtocol() + "://" + hostport;

        endpoints.add(endpoint);
      }
    }

    return endpoints;
  }

  static String endpointsToBootstrapServers(List<String> endpoints, String securityProtocol) {
    if (!SUPPORTED_SECURITY_PROTOCOLS.contains(SecurityProtocol.forName(securityProtocol))) {
      throw new ConfigException(
          "Only PLAINTEXT, SSL, SASL_PLAINTEXT, and SASL_SSL Kafka endpoints are supported.");
    }

    final String securityProtocolUrlPrefix = securityProtocol + "://";
    final StringBuilder sb = new StringBuilder();
    for (String endpoint : endpoints) {
      if (!endpoint.startsWith(securityProtocolUrlPrefix)) {
        log.warn(
            "Ignoring Kafka broker endpoint " + endpoint + " that does not match the setting for "
            + SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG + "=" + securityProtocol);
        continue;
      }

      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(endpoint);
    }

    if (sb.length() == 0) {
      throw new ConfigException("No supported Kafka endpoints are configured. Either "
                                + SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG
                                + " must have at least one endpoint matching "
                                + SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG
                                + " or broker endpoints loaded from ZooKeeper "
                                + "must have at least one endpoint matching "
                                + SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG + ".");
    }

    return sb.toString();
  }

  /**
   * Wait until the KafkaStore catches up to the last message in the Kafka topic.
   */
  public void waitUntilKafkaReaderReachesLastOffset(int timeoutMs) throws StoreException {
    long offsetOfLastMessage = getLatestOffset(timeoutMs);
    log.info("Wait to catch up until the offset of the last message at " + offsetOfLastMessage);
    kafkaTopicReader.waitUntilOffset(offsetOfLastMessage, timeoutMs, TimeUnit.MILLISECONDS);
    log.debug("Reached offset at " + offsetOfLastMessage);
  }

  public void markLastWrittenOffsetInvalid() {
    lastWrittenOffset = -1L;
  }

  @Override
  public V get(K key) throws StoreException {
    assertInitialized();
    return localStore.get(key);
  }

  @Override
  public void put(K key, V value) throws StoreTimeoutException, StoreException {
    assertInitialized();
    if (key == null) {
      throw new StoreException("Key should not be null");
    }

    // write to the Kafka topic
    ProducerRecord<byte[], byte[]> producerRecord = null;
    try {
      producerRecord =
          new ProducerRecord<byte[], byte[]>(topic, 0, this.serializer.serializeKey(key),
                                             value == null ? null : this.serializer.serializeValue(
                                                 value));
    } catch (SerializationException e) {
      throw new StoreException("Error serializing schema while creating the Kafka produce "
                               + "record", e);
    }

    boolean knownSuccessfulWrite = false;
    try {
      log.trace("Sending record to KafkaStore topic: " + producerRecord);
      Future<RecordMetadata> ack = producer.send(producerRecord);
      RecordMetadata recordMetadata = ack.get(timeout, TimeUnit.MILLISECONDS);

      log.trace("Waiting for the local store to catch up to offset " + recordMetadata.offset());
      this.lastWrittenOffset = recordMetadata.offset();
      kafkaTopicReader.waitUntilOffset(this.lastWrittenOffset, timeout, TimeUnit.MILLISECONDS);
      knownSuccessfulWrite = true;
    } catch (InterruptedException e) {
      throw new StoreException("Put operation interrupted while waiting for an ack from Kafka", e);
    } catch (ExecutionException e) {
      throw new StoreException("Put operation failed while waiting for an ack from Kafka", e);
    } catch (TimeoutException e) {
      throw new StoreTimeoutException(
          "Put operation timed out while waiting for an ack from Kafka", e);
    } catch (KafkaException ke) {
      throw new StoreException("Put operation to Kafka failed", ke);
    } finally {
      if (!knownSuccessfulWrite) {
        this.lastWrittenOffset = -1L;
      }
    }
  }

  @Override
  public Iterator<V> getAll(K key1, K key2) throws StoreException {
    assertInitialized();
    return localStore.getAll(key1, key2);
  }

  @Override
  public void putAll(Map<K, V> entries) throws StoreException {
    assertInitialized();
    // TODO: write to the Kafka topic as a batch
    for (Map.Entry<K, V> entry : entries.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void delete(K key) throws StoreException {
    assertInitialized();
    // deleteSchemaVersion from the Kafka topic by writing a null value for the key
    put(key, null);
  }

  @Override
  public Iterator<K> getAllKeys() throws StoreException {
    return localStore.getAllKeys();
  }

  @Override
  public void close() {
    kafkaTopicReader.shutdown();
    log.debug("Kafka store reader thread shut down");
    producer.close();
    log.debug("Kafka store producer shut down");
    localStore.close();
    log.debug("Kafka store shut down complete");
  }

  /**
   * For testing.
   */
  KafkaStoreReaderThread<K, V> getKafkaStoreReaderThread() {
    return this.kafkaTopicReader;
  }

  private void assertInitialized() throws StoreException {
    if (!initialized.get()) {
      throw new StoreException("Illegal state. Store not initialized yet");
    }
  }

  /**
   * Return the latest offset of the store topic.
   *
   * <p>The most reliable way to do so in face of potential Kafka broker failure is to produce
   * successfully to the Kafka topic and get the offset of the returned metadata.
   *
   * <p>If the most recent write to Kafka was successful (signaled by lastWrittenOffset >= 0),
   * immediately return that offset. Otherwise write a "Noop key" to Kafka in order to find the
   * latest offset.
   */
  private long getLatestOffset(int timeoutMs) throws StoreException {
    ProducerRecord<byte[], byte[]> producerRecord = null;

    if (this.lastWrittenOffset >= 0) {
      return this.lastWrittenOffset;
    }

    try {
      producerRecord =
          new ProducerRecord<byte[], byte[]>(topic, 0, this.serializer.serializeKey(noopKey), null);
    } catch (SerializationException e) {
      throw new StoreException("Failed to serialize noop key.", e);
    }

    try {
      log.trace("Sending Noop record to KafkaStore to find last offset.");
      Future<RecordMetadata> ack = producer.send(producerRecord);
      RecordMetadata metadata = ack.get(timeoutMs, TimeUnit.MILLISECONDS);
      this.lastWrittenOffset = metadata.offset();
      log.trace("Noop record's offset is " + this.lastWrittenOffset);
      return this.lastWrittenOffset;
    } catch (Exception e) {
      throw new StoreException("Failed to write Noop record to kafka store.", e);
    }
  }
}
