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

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

import io.confluent.kafka.schemaregistry.ClusterTestHarness.AddKraftQuorum;
import io.vavr.Tuple2;
import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import kafka.server.KafkaBroker;
import kafka.server.QuorumTestHarness;
import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.config.DelegationTokenManagerConfigs;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.test.TestSslUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Test harness to run against a real, local Kafka cluster and REST proxy. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined and ported to Java with
 * the addition of the REST proxy. Defaults to a 1-ZK, 3-broker, 1 REST proxy cluster.
 */
@Tag("IntegrationTest")
@DisplayNameGeneration(AddKraftQuorum.class)
public abstract class ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(ClusterTestHarness.class);

  public static final int DEFAULT_NUM_BROKERS = 1;
  public static final int MAX_MESSAGE_SIZE = (2 << 20) * 10; // 10 MiB
  public static final String KAFKASTORE_TOPIC = SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC;
  protected static final Optional<Properties> EMPTY_SASL_PROPERTIES = Optional.empty();

  /**
   * Choose a number of random available ports
   */
  public static int[] choosePorts(int count) {
    try {
      ServerSocket[] sockets = new ServerSocket[count];
      int[] ports = new int[count];
      for (int i = 0; i < count; i++) {
        sockets[i] = new ServerSocket(0, 0, InetAddress.getByName("0.0.0.0"));
        ports[i] = sockets[i].getLocalPort();
      }
      for (int i = 0; i < count; i++) {
        sockets[i].close();
      }
      return ports;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Choose an available port
   */
  public static int choosePort() {
    return choosePorts(1)[0];
  }

  private final int numBrokers;
  private final boolean setupRestApp;
  protected String compatibilityType;

  // Quorum controller
  private TestInfo testInfo;
  private QuorumTestHarness quorumTestHarness;

  // Kafka Config
  protected List<KafkaConfig> configs = null;
  protected List<KafkaBroker> servers = null;
  protected String brokerList = null;

  protected Integer schemaRegistryPort;
  protected RestApp restApp = null;

  public ClusterTestHarness() {
    this(DEFAULT_NUM_BROKERS);
  }

  public ClusterTestHarness(int numBrokers) {
    this(numBrokers, false);
  }

  public ClusterTestHarness(int numBrokers, boolean setupRestApp) {
    this(numBrokers, setupRestApp, CompatibilityLevel.NONE.name);
  }

  public ClusterTestHarness(int numBrokers, boolean setupRestApp, String compatibilityType
  ) {
    this.numBrokers = numBrokers;
    this.setupRestApp = setupRestApp;
    this.compatibilityType = compatibilityType;
  }

  @BeforeEach
  public void setUpTest(TestInfo testInfo) throws Exception {
    this.testInfo = testInfo;
    log.info("Starting setup of {}", getClass().getSimpleName());
    setUp();
    log.info("Completed setup of {}", getClass().getSimpleName());
  }

  protected void setUp() throws Exception {
    checkState(testInfo != null);
    log.info("Starting controller of {}", getClass().getSimpleName());
    // start controller
    this.quorumTestHarness =
        new DefaultQuorumTestHarness(
            overrideKraftControllerSecurityProtocol(), overrideKraftControllerConfig());
    quorumTestHarness.setUp(testInfo);

    // start brokers concurrently
    startBrokersConcurrently(numBrokers);

    brokerList =
        TestUtils.getBrokerListStrFromServers(
            JavaConverters.asScalaBuffer(servers), getBrokerSecurityProtocol());

    setupAcls();

    if (setupRestApp) {
      if (schemaRegistryPort == null)
        schemaRegistryPort = choosePort();
      Properties schemaRegistryProps = getSchemaRegistryProperties();
      if (!schemaRegistryProps.containsKey(SchemaRegistryConfig.LISTENERS_CONFIG)) {
        schemaRegistryProps.put(SchemaRegistryConfig.LISTENERS_CONFIG, getSchemaRegistryProtocol() +
            "://0.0.0.0:"
            + schemaRegistryPort);
      }
      schemaRegistryProps.put(SchemaRegistryConfig.MODE_MUTABILITY, true);
      setupRestApp(schemaRegistryProps);
    }
  }

  protected void setupRestApp(Properties schemaRegistryProps) throws Exception {
    restApp = new RestApp(schemaRegistryPort, null, brokerList, KAFKASTORE_TOPIC,
                          compatibilityType, true, schemaRegistryProps);
    restApp.start();
  }

  protected Properties getSchemaRegistryProperties() throws Exception {
    return new Properties();
  }

  protected void injectProperties(Properties props) {
    // Make sure that broker only role is "broker"
    props.setProperty("process.roles", "broker");
    props.setProperty("message.max.bytes", String.valueOf(MAX_MESSAGE_SIZE));

    props.setProperty("auto.create.topics.enable", "true");
    props.setProperty("num.partitions", "1");
  }

  protected KafkaConfig getKafkaConfig(int brokerId) {
    final Optional<java.io.File> noFile = Optional.empty();
    Properties props = createBrokerConfig(
        brokerId,
        false,
        true,
        TestUtils.RandomPort(),
        Optional.of(getBrokerSecurityProtocol()),
        noFile,
        EMPTY_SASL_PROPERTIES,
        true,
        false,
        TestUtils.RandomPort(),
        false,
        TestUtils.RandomPort(),
        false,
        TestUtils.RandomPort(),
        Optional.empty(),
        false,
        1,
        (short) 1,
        false
    );
    injectProperties(props);
    return KafkaConfig.fromProps(props);
  }

  protected SecurityProtocol getBrokerSecurityProtocol() {
    return SecurityProtocol.PLAINTEXT;
  }

  protected String getSchemaRegistryProtocol() {
    return SchemaRegistryConfig.HTTP;
  }

  protected Time brokerTime(int brokerId) {
    return Time.SYSTEM;
  }

  private void startBrokersConcurrently(int numBrokers) {
    log.info("Starting concurrently {} brokers for {}", numBrokers, getClass().getSimpleName());
    configs =
        IntStream.range(0, numBrokers)
            .mapToObj(this::getKafkaConfig)
            .collect(toList());
    servers =
        allAsList(
                configs.stream()
                    .map(
                        config ->
                            CompletableFuture.supplyAsync(
                                () ->
                                    quorumTestHarness.createBroker(
                                        config,
                                        brokerTime(config.brokerId()),
                                        true,
                                        Option.empty())))
                    .collect(toList()))
            .join();
    log.info("Started all {} brokers for {}", numBrokers, getClass().getSimpleName());
  }

  static <T> CompletableFuture<List<T>> allAsList(List<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(
            none -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  protected void setupAcls() {}

  /** Only applicable in Kraft tests, no effect in Zk tests */
  protected Properties overrideKraftControllerConfig() {
    return new Properties();
  }

  /** Only applicable in Kraft tests, no effect in Zk tests */
  protected SecurityProtocol overrideKraftControllerSecurityProtocol() {
    return SecurityProtocol.PLAINTEXT;
  }

  @AfterEach
  public void tearDown() throws Exception {
    log.info("Starting teardown of {}", getClass().getSimpleName());
    if (restApp != null) {
      restApp.stop();
    }
    tearDownMethod();
    log.info("Completed teardown of {}", getClass().getSimpleName());
  }

  private void tearDownMethod() throws Exception {
    checkState(quorumTestHarness != null);

    TestUtils.shutdownServers(JavaConverters.asScalaBuffer(servers), true);

    log.info("Stopping controller of {}", getClass().getSimpleName());
    quorumTestHarness.tearDown();
  }
  /** A concrete class of QuorumTestHarness so that we can customize for testing purposes */
  static class DefaultQuorumTestHarness extends QuorumTestHarness {
    private final Properties kraftControllerConfig;
    private final SecurityProtocol securityProtocol;

    DefaultQuorumTestHarness(SecurityProtocol securityProtocol, Properties kraftControllerConfig) {
      this.securityProtocol = securityProtocol;
      this.kraftControllerConfig = kraftControllerConfig;
    }

    @Override
    public SecurityProtocol controllerListenerSecurityProtocol() {
      return securityProtocol;
    }

    @Override
    public Seq<Properties> kraftControllerConfigs(TestInfo testInfo) {
      // only one Kraft controller is supported in QuorumTestHarness
      return JavaConverters.asScalaBuffer(Collections.singletonList(kraftControllerConfig));
    }
  }

  public static Properties createBrokerConfig(
      int nodeId,
      boolean enableControlledShutdown,
      boolean enableDeleteTopic,
      int port,
      Optional<SecurityProtocol> interBrokerSecurityProtocol,
      Optional<File> trustStoreFile,
      Optional<Properties> saslProperties,
      boolean enablePlaintext,
      boolean enableSaslPlaintext,
      int saslPlaintextPort,
      boolean enableSsl,
      int sslPort,
      boolean enableSaslSsl,
      int saslSslPort,
      Optional<String> rack,
      boolean enableToken,
      int numPartitions,
      short defaultReplicationFactor,
      boolean enableFetchFromFollower) {

    try {
      Function<SecurityProtocol, Boolean> shouldEnable = (protocol) -> interBrokerSecurityProtocol.map(
          p -> p == protocol).orElse(false);

      List<Tuple2<SecurityProtocol, Integer>> protocolAndPorts = new ArrayList<>();
      if (enablePlaintext || shouldEnable.apply(SecurityProtocol.PLAINTEXT)) {
        protocolAndPorts.add(new Tuple2<>(SecurityProtocol.PLAINTEXT, port));
      }
      if (enableSsl || shouldEnable.apply(SecurityProtocol.SSL)) {
        protocolAndPorts.add(new Tuple2<>(SecurityProtocol.SSL, sslPort));
      }
      if (enableSaslPlaintext || shouldEnable.apply(SecurityProtocol.SASL_PLAINTEXT)) {
        protocolAndPorts.add(new Tuple2<>(SecurityProtocol.SASL_PLAINTEXT, saslPlaintextPort));
      }
      if (enableSaslSsl || shouldEnable.apply(SecurityProtocol.SASL_SSL)) {
        protocolAndPorts.add(new Tuple2<>(SecurityProtocol.SASL_SSL, saslSslPort));
      }

      String listeners = protocolAndPorts.stream()
          .map(p -> p._1.name() + "://localhost:" + p._2)
          .reduce((a, b) -> a + "," + b)
          .get();
      String protocolMap = protocolAndPorts.stream()
          .map(p -> p._1.name() + ":" + p._1)
          .reduce((a, b) -> a + "," + b)
          .get();

      Properties props = new Properties();
      props.put(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true");
      props.put(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true");
      props.setProperty(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG,
          String.valueOf(TimeUnit.MINUTES.toMillis(10)));
      props.put(KRaftConfigs.NODE_ID_CONFIG, String.valueOf(nodeId));
      props.put(ServerConfigs.BROKER_ID_CONFIG, String.valueOf(nodeId));
      props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listeners);
      props.put(SocketServerConfigs.LISTENERS_CONFIG, listeners);
      props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");
      props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
          protocolMap + ",CONTROLLER:PLAINTEXT");

      props.put(ServerLogConfigs.LOG_DIR_CONFIG, TestUtils.tempDir().getAbsolutePath());
      props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
      // Note: this is just a placeholder value for controller.quorum.voters. JUnit
      // tests use random port assignment, so the controller ports are not known ahead of
      // time. Therefore, we ignore controller.quorum.voters and use
      // controllerQuorumVotersFuture instead.
      props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "1000@localhost:0");
      props.put(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG, "1500");
      props.put(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG, "1500");
      props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG,
          String.valueOf(enableControlledShutdown));
      props.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, String.valueOf(enableDeleteTopic));
      props.put(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, "1000");
      props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "2097152");
      props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
      props.put(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, "100");
      if (!props.containsKey(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG)) {
        props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "5");
      }
      if (!props.containsKey(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG)) {
        props.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0");
      }
      rack.ifPresent(r -> props.put(ServerConfigs.BROKER_RACK_CONFIG, r));
      // Reduce number of threads per broker
      props.put(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, "2");
      props.put(ServerConfigs.BACKGROUND_THREADS_CONFIG, "2");

      if (protocolAndPorts.stream().anyMatch(p -> usesSslTransportLayer(p._1))) {
        props.putAll(sslConfigs(ConnectionMode.SERVER, false, trustStoreFile,
            "server" + nodeId));
      }

      if (protocolAndPorts.stream().anyMatch(p -> usesSaslAuthentication(p._1))) {
        props.putAll(saslConfigs(saslProperties));
      }

      interBrokerSecurityProtocol.ifPresent(protocol ->
          props.put(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, protocol.name)
      );

      if (enableToken)
        props.put(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, "secretkey");

      props.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, String.valueOf(numPartitions));
      props.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG,
          String.valueOf(defaultReplicationFactor));

      if (enableFetchFromFollower) {
        props.put(ServerConfigs.BROKER_RACK_CONFIG, String.valueOf(nodeId));
        props.put(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG,
            "org.apache.kafka.common.replica.RackAwareReplicaSelector");
      }
      return props;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean usesSslTransportLayer(SecurityProtocol securityProtocol) {
    switch (securityProtocol) {
      case SSL:
      case SASL_SSL:
        return true;
      default:
        return false;
    }
  }

  public static boolean usesSaslAuthentication(SecurityProtocol securityProtocol) {
    switch (securityProtocol) {
      case SASL_SSL:
      case SASL_PLAINTEXT:
        return true;
      default:
        return false;
    }
  }

  public static Properties sslConfigs(ConnectionMode mode, boolean clientCert, Optional<File> trustStoreFile, String certAlias) throws Exception {
    return sslConfigs(mode, clientCert, trustStoreFile, certAlias, "localhost", "TLSv1.3");
  }

  public static Properties sslConfigs(ConnectionMode mode, boolean clientCert, Optional<File> trustStoreFile, String certAlias, String certCn, String tlsProtocol) throws Exception {
    File trustStore = trustStoreFile.orElseThrow(() -> new Exception("SSL enabled but no trustStoreFile provided"));
    Properties sslProps = new Properties();
    sslProps.putAll((new TestSslUtils.SslConfigsBuilder(mode)).useClientCert(clientCert).createNewTrustStore(trustStore).certAlias(certAlias).cn(certCn).tlsProtocol(tlsProtocol).build());
    return sslProps;
  }

  private static final boolean IS_IBM_SECURITY = Java.isIbmJdk() && !Java.isIbmJdkSemeru();

  public static Properties saslConfigs(Optional<Properties> saslProperties) {
    Properties result = saslProperties.orElse(new Properties());
    if (IS_IBM_SECURITY && !result.containsKey("sasl.kerberos.service.name")) {
      result.put("sasl.kerberos.service.name", "kafka");
    }

    return result;
  }

  static class AddKraftQuorum extends DisplayNameGenerator.Standard {
    @Override
    public String generateDisplayNameForClass(Class<?> testClass) {
      return addKraftQuorum(super.generateDisplayNameForClass(testClass));
    }

    @Override
    public String generateDisplayNameForNestedClass(Class<?> nestedClass) {
      return addKraftQuorum(super.generateDisplayNameForNestedClass(nestedClass));
    }

    @Override
    public String generateDisplayNameForMethod(Class<?> testClass, Method testMethod) {
      return this.addKraftQuorum(super.generateDisplayNameForMethod(testClass, testMethod));
    }

    String addKraftQuorum(String name) {
      return name + ",quorum=kraft";
    }
  }
}
