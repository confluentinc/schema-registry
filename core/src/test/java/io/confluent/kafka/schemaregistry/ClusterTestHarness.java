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

import io.confluent.common.utils.IntegrationTest;
import kafka.utils.CoreUtils;
import org.I0Itec.zkclient.ZkClient;
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

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import scala.Option;
import scala.Option$;
import scala.collection.JavaConversions;

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
  protected ZkClient zkClient;
  protected ZkUtils zkUtils;
  protected int zkConnectionTimeout = 30000; // a larger connection timeout is required for SASL tests
                                             // because SASL connections tend to take longer.
  protected int zkSessionTimeout = 6000;

  // Kafka Config
  protected List<KafkaConfig> configs = null;
  protected List<KafkaServer> servers = null;
  protected String brokerList = null;
  protected String bootstrapServers = null;

  protected int schemaRegistryPort;
  protected RestApp restApp = null;

  public ClusterTestHarness() {
    this(DEFAULT_NUM_BROKERS);
  }

  public ClusterTestHarness(int numBrokers) {
    this(numBrokers, false);
  }

  public ClusterTestHarness(int numBrokers, boolean setupRestApp) {
    this(numBrokers, setupRestApp, AvroCompatibilityLevel.NONE.name);
  }

  public ClusterTestHarness(int numBrokers, boolean setupRestApp, String compatibilityType
  ) {
    this.numBrokers = numBrokers;
    this.setupRestApp = setupRestApp;
    this.compatibilityType = compatibilityType;
  }

  protected boolean setZkAcls() {
    return getSecurityProtocol() == SecurityProtocol.SASL_PLAINTEXT ||
           getSecurityProtocol() == SecurityProtocol.SASL_SSL;
  }

  @Before
  public void setUp() throws Exception {
    zookeeper = new EmbeddedZookeeper();
    zkConnect = String.format("localhost:%d", zookeeper.port());
    zkUtils = ZkUtils.apply(
        zkConnect, zkSessionTimeout, zkConnectionTimeout,
        setZkAcls()
    ); // true or false doesn't matter because the schema registry Kafka principal is the same as the
    // Kafka broker principal, so ACLs won't make any difference. The principals are the same because
    // ZooKeeper, Kafka, and the Schema Registry are run in the same process during testing and hence share
    // the same JAAS configuration file. Read comments in ASLClusterTestHarness.java for more details.
    zkClient = zkUtils.zkClient();

    configs = new Vector<>();
    servers = new Vector<>();
    for (int i = 0; i < numBrokers; i++) {
      KafkaConfig config = getKafkaConfig(i);
      configs.add(config);

      KafkaServer server = TestUtils.createServer(config, Time.SYSTEM);
      servers.add(server);
    }

    brokerList =
        TestUtils.getBrokerListStrFromServers(
            JavaConversions.asScalaBuffer(servers),
            getSecurityProtocol()
        );

    // Initialize the rest app ourselves so we can ensure we don't pass any info about the Kafka
    // zookeeper. The format for this config includes the security protocol scheme in the URLs so
    // we can't use the pre-generated server list.
    String[] serverUrls = new String[servers.size()];
    ListenerName listenerType = ListenerName.forSecurityProtocol(getSecurityProtocol());
    for(int i = 0; i < servers.size(); i++) {
      serverUrls[i] = getSecurityProtocol() + "://" +
                      Utils.formatAddress(
                          servers.get(i).config().advertisedListeners().head().host(),
                          servers.get(i).boundPort(listenerType)
                      );
    }
    bootstrapServers = Utils.join(serverUrls, ",");

    if (setupRestApp) {
      schemaRegistryPort = choosePort();
      Properties schemaRegistryProps = getSchemaRegistryProperties();
      schemaRegistryProps.put(SchemaRegistryConfig.LISTENERS_CONFIG, getSchemaRegistryProtocol() +
                                                                     "://0.0.0.0:"
                                                                     + schemaRegistryPort);
      restApp = new RestApp(schemaRegistryPort, zkConnect, null, KAFKASTORE_TOPIC,
                            compatibilityType, true, schemaRegistryProps);
      restApp.start();

    }
  }

  protected Properties getSchemaRegistryProperties() {
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
        false,
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
        1
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

    if (zkUtils != null) {
      zkUtils.close();
    }

    if (zookeeper != null) {
      zookeeper.shutdown();
    }
  }
}
