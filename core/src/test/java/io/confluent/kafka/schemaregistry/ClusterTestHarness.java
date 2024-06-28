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

import io.confluent.common.utils.IntegrationTest;
import kafka.utils.CoreUtils;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import scala.Option;
import scala.Option$;
import scala.collection.JavaConverters;

/**
 * Test harness to run against a real, local Kafka cluster and REST proxy. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined and ported to Java with
 * the addition of the REST proxy. Defaults to a 1-ZK, 3-broker, 1 REST proxy cluster.
 */
@Category(IntegrationTest.class)
public abstract class ClusterTestHarness {

  public static final int DEFAULT_NUM_BROKERS = 1;
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

  private int numBrokers;
  private boolean setupRestApp;
  protected String compatibilityType;

  // ZK Config
  protected EmbeddedZookeeper zookeeper;
  protected String zkConnect;

  // Kafka Config
  protected List<KafkaConfig> configs = null;
  protected List<KafkaServer> servers = null;
  protected String brokerList = null;
  protected String bootstrapServers = null;

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

  @Before
  public void setUp() throws Exception {
    zookeeper = new EmbeddedZookeeper();
    zkConnect = String.format("localhost:%d", zookeeper.port());
    configs = new Vector<>();
    servers = new Vector<>();
    for (int i = 0; i < numBrokers; i++) {
      KafkaConfig config = getKafkaConfig(i);
      configs.add(config);

      KafkaServer server = TestUtils.createServer(config, Time.SYSTEM);
      servers.add(server);
    }

    ListenerName listenerType = ListenerName.forSecurityProtocol(getSecurityProtocol());
    brokerList = TestUtils.bootstrapServers(JavaConverters.asScalaBuffer(servers), listenerType);

    // Initialize the rest app ourselves so we can ensure we don't pass any info about the Kafka
    // zookeeper. The format for this config includes the security protocol scheme in the URLs so
    // we can't use the pre-generated server list.
    String[] serverUrls = new String[servers.size()];
    for(int i = 0; i < servers.size(); i++) {
      serverUrls[i] = getSecurityProtocol() + "://" +
                      Utils.formatAddress(
                          servers.get(i).config().effectiveAdvertisedBrokerListeners().head().host(),
                          servers.get(i).boundPort(listenerType)
                      );
    }
    bootstrapServers = String.join(",", serverUrls);

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
    restApp = new RestApp(schemaRegistryPort, zkConnect, bootstrapServers, KAFKASTORE_TOPIC,
                          compatibilityType, true, schemaRegistryProps);
    restApp.start();
  }

  protected Properties getSchemaRegistryProperties() throws Exception {
    return new Properties();
  }

  protected void injectProperties(Properties props) {
    props.setProperty("auto.create.topics.enable", "true");
    props.setProperty("num.partitions", "1");
  }

  protected KafkaConfig getKafkaConfig(int brokerId) {

    final Option<java.io.File> noFile = scala.Option.apply(null);
    final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
    Properties props = TestUtils.createBrokerConfig(
        brokerId,
        zkConnect,
        false,
        true,
        TestUtils.RandomPort(),
        noInterBrokerSecurityProtocol,
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

  protected SecurityProtocol getSecurityProtocol() {
    return SecurityProtocol.PLAINTEXT;
  }

  protected String getSchemaRegistryProtocol() {
    return SchemaRegistryConfig.HTTP;
  }

  @After
  public void tearDown() throws Exception {
    if (restApp != null) {
      restApp.stop();
    }

    if (servers != null) {
      for (KafkaServer server : servers) {
        server.shutdown();
      }

      // Remove any persistent data
      for (KafkaServer server : servers) {
        CoreUtils.delete(server.config().logDirs());
      }
    }

    if (zookeeper != null) {
      zookeeper.shutdown();
    }
  }
}
