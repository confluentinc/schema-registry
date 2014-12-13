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

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.Utils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import scala.collection.JavaConversions;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Test harness to run against a real, local Kafka cluster and REST proxy. This is essentially Kafka's
 * ZookeeperTestHarness and KafkaServerTestHarness traits combined and ported to Java with the addition
 * of the REST proxy. Defaults to a 1-ZK, 3-broker, 1 REST proxy cluster.
 */
public abstract class ClusterTestHarness {
    public static final int DEFAULT_NUM_BROKERS = 3;

    // Shared config
    protected Queue<Integer> ports;

    // ZK Config
    protected int zkPort;
    protected String zkConnect;
    protected EmbeddedZookeeper zookeeper;
    protected ZkClient zkClient;
    protected int zkConnectionTimeout = 6000;
    protected int zkSessionTimeout = 6000;

    // Kafka Config
    protected List<KafkaConfig> configs = null;
    protected List<KafkaServer> servers = null;
    protected String brokerList = null;

    protected String bootstrapServers = null;

    public ClusterTestHarness() {
        this(DEFAULT_NUM_BROKERS);
    }

    public ClusterTestHarness(int numBrokers) {
        // 1 port per broker + ZK + REST server
        this(numBrokers, numBrokers + 2);
    }

    public ClusterTestHarness(int numBrokers, int numPorts) {
        ports = new ArrayDeque<Integer>();
        for(Object portObj : JavaConversions.asJavaList(TestUtils.choosePorts(numPorts)))
            ports.add((Integer)portObj);
        zkPort = ports.remove();
        zkConnect = String.format("localhost:%d", zkPort);

        configs = new Vector<KafkaConfig>();
        bootstrapServers = "";
        for(int i = 0; i < numBrokers; i++) {
            int port = ports.remove();
            Properties props = TestUtils.createBrokerConfig(i, port, false);
            // Turn auto creation *off*, unlike the default. This lets us test errors that should be generated when
            // brokers are configured that way.
            props.setProperty("auto.create.topics.enable", "true");
            props.setProperty("num.partitions", "1");
            // We *must* override this to use the port we allocated (Kafka currently allocates one port that it always
            // uses for ZK
            props.setProperty("zookeeper.connect", this.zkConnect);
            configs.add(new KafkaConfig(props));

            if (bootstrapServers.length() > 0)
                bootstrapServers += ",";
            bootstrapServers = bootstrapServers + "localhost:" + ((Integer)port).toString();
        }
    }

    @Before
    public void setUp() throws Exception {
        zookeeper = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zookeeper.connectString(), zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer$.MODULE$);

        if(configs == null || configs.size() <= 0)
            throw new RuntimeException("Must supply at least one server config.");
        brokerList = TestUtils.getBrokerListStrFromConfigs(JavaConversions.asScalaIterable(configs).toSeq());
        servers = new Vector<KafkaServer>(configs.size());
        for(KafkaConfig config : configs) {
            KafkaServer server = TestUtils.createServer(config, SystemTime$.MODULE$);
            servers.add(server);
        }
    }

    @After
    public void tearDown() throws Exception {
        for(KafkaServer server: servers)
            server.shutdown();
        for(KafkaServer server: servers)
            for (String logDir : JavaConversions.asJavaCollection(server.config().logDirs()))
                Utils.rm(logDir);

        zkClient.close();
        zookeeper.shutdown();
    }
}
