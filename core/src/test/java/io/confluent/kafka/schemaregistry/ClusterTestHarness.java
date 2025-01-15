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

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import kafka.server.KafkaBroker;
import kafka.server.QuorumTestHarness;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Option$;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Test harness to run against a real, local Kafka cluster and REST proxy. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined and ported to Java with
 * the addition of the REST proxy. Defaults to a 1-ZK, 3-broker, 1 REST proxy cluster.
 */
@Tag("IntegrationTest")
public abstract class ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(ClusterTestHarness.class);

  public static final int DEFAULT_NUM_BROKERS = 1;
  public static final int MAX_MESSAGE_SIZE = (2 << 20) * 10; // 10 MiB
  public static final String KAFKASTORE_TOPIC = SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC;
  protected static final Option<Properties> EMPTY_SASL_PROPERTIES = Option$.MODULE$.<Properties>empty();

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
      schemaRegistryProps.put(SchemaRegistryConfig.LISTENERS_CONFIG, getSchemaRegistryProtocol() +
                                                                     "://0.0.0.0:"
                                                                     + schemaRegistryPort);
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
    props.setProperty("auto.create.topics.enable", "false");
    props.setProperty("message.max.bytes", String.valueOf(MAX_MESSAGE_SIZE));

    props.setProperty("auto.create.topics.enable", "true");
    props.setProperty("num.partitions", "1");
  }

  protected KafkaConfig getKafkaConfig(int brokerId) {
    final Option<java.io.File> noFile = scala.Option.apply(null);
    Properties props = TestUtils.createBrokerConfig(
        brokerId,
        false,
        true,
        TestUtils.RandomPort(),
        Option.apply(getBrokerSecurityProtocol()),
        noFile,
        EMPTY_SASL_PROPERTIES,
        true,
        false,
        TestUtils.RandomPort(),
        false,
        TestUtils.RandomPort(),
        false,
        TestUtils.RandomPort(),
        Option.<String>empty(),
        1,
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
}
