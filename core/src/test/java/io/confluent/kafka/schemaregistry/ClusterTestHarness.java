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

import kafka.utils.CoreUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.After;
import org.junit.Before;

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
import kafka.utils.SystemTime$;
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
public abstract class ClusterTestHarness {

  public static final int DEFAULT_NUM_BROKERS = 1;
  public static final String KAFKASTORE_TOPIC = SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC;
  protected static final Option<Properties> SASL_PROPERTIES = Option$.MODULE$.<Properties>empty();

  private int numBrokers;
  private boolean setupRestApp;
  private String compatibilityType;

  // ZK Config
  protected EmbeddedZookeeper zookeeper;
  protected String zkConnect;
  protected ZkClient zkClient;
  protected ZkUtils zkUtils;
  protected int zkConnectionTimeout = 6000;
  protected int zkSessionTimeout = 6000;

  // Kafka Config
  protected List<KafkaConfig> configs = null;
  protected List<KafkaServer> servers = null;
  protected String brokerList = null;

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

  public ClusterTestHarness(int numBrokers, boolean setupRestApp, String compatibilityType) {
    this.numBrokers = numBrokers;
    this.setupRestApp = setupRestApp;
    this.compatibilityType = compatibilityType;
  }

  @Before
  public void setUp() throws Exception {
    zookeeper = new EmbeddedZookeeper();
    zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    zkUtils = ZkUtils.apply(
        zkConnect, zkSessionTimeout, zkConnectionTimeout,
        JaasUtils.isZkSecurityEnabled());
    zkClient = zkUtils.zkClient();

    configs = new Vector<>();
    servers = new Vector<>();
    for (int i = 0; i < numBrokers; i++) {
      KafkaConfig config = getKafkaConfig(i);
      configs.add(config);

      KafkaServer server = TestUtils.createServer(config, SystemTime$.MODULE$);
      servers.add(server);
    }

    brokerList =
        TestUtils.getBrokerListStrFromServers(JavaConversions.asScalaBuffer(servers),
                                              getSecurityProtocol());

    if (setupRestApp) {
      restApp = new RestApp(zkConnect, KAFKASTORE_TOPIC, compatibilityType);
      restApp.start();
    }
  }

  protected KafkaConfig getKafkaConfig(int brokerId) {
    final Option<java.io.File> noFile = scala.Option.apply(null);
    final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
    Properties props = TestUtils.createBrokerConfig(
            brokerId, zkConnect, false, false, TestUtils.RandomPort(), noInterBrokerSecurityProtocol,
            noFile, SASL_PROPERTIES, true, false, TestUtils.RandomPort(), false, TestUtils.RandomPort(), false,
            TestUtils.RandomPort(), Option.<String>empty());
    props.setProperty("auto.create.topics.enable", "true");
    props.setProperty("num.partitions", "1");

    // We *must* override this to use the ZooKeeper port chosen in this test harness, instead of the
    // port chosen by TestUtils.createBrokerConfig().
    props.setProperty("zookeeper.connect", this.zkConnect);
    return KafkaConfig.fromProps(props);
  }

  protected SecurityProtocol getSecurityProtocol() {
    return SecurityProtocol.PLAINTEXT;
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
