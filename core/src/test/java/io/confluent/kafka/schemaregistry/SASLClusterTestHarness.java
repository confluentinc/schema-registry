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

import kafka.security.minikdc.MiniKdc;
import kafka.server.KafkaConfig;
import kafka.utils.JaasTestUtils;
import kafka.utils.TestUtils;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.List;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import javax.security.auth.login.Configuration;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

// sets up SASL for ZooKeeper and Kafka. Much of this was borrowed from kafka.api.SaslSetup in Kafka.
public class SASLClusterTestHarness extends ClusterTestHarness {
  public static final String JAAS_CONF = "java.security.auth.login.config";
  public static final String ZK_AUTH_PROVIDER = "zookeeper.authProvider.1";

  private MiniKdc kdc = null;
  private File kdcHome = TestUtils.tempDir();
  private Properties kdcProps = MiniKdc.createConfig();

  private static final Logger log = LoggerFactory.getLogger(SASLClusterTestHarness.class);

  public SASLClusterTestHarness() {
    super(DEFAULT_NUM_BROKERS);
  }

  @Override
  protected SecurityProtocol getSecurityProtocol() {
    return SecurityProtocol.SASL_PLAINTEXT;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    // Important if tests leak consumers, producers or brokers.
    LoginManager.closeAll();

    File serverKeytab = File.createTempFile("server-", ".keytab");
    File clientKeytab = File.createTempFile("client-", ".keytab");

    // create a JAAS file.
    Option<File> serverKeytabOption = Option.apply(serverKeytab);
    Option<File> clientKeytabOption = Option.apply(clientKeytab);
    List<String> serverSaslMechanisms = JavaConverters.asScalaBuffer(Arrays.asList("GSSAPI")).toList();
    Option<String> clientSaslMechanism = Option.apply("GSSAPI");

    java.util.List<JaasTestUtils.JaasSection> jaasSections = new ArrayList<>();
    jaasSections.add(JaasTestUtils.kafkaServerSection(JaasTestUtils.KafkaServerContextName(), serverSaslMechanisms, serverKeytabOption));
    jaasSections.add(JaasTestUtils.kafkaClientSection(clientSaslMechanism, clientKeytabOption));
    jaasSections.addAll(CollectionConverters.asJavaCollection(JaasTestUtils.zkSections()));
    String jaasFilePath = JaasTestUtils.writeJaasContextsToFile(JavaConverters.asScalaBuffer(jaasSections).toSeq()).getAbsolutePath();

    log.info("Using KDC home: " + kdcHome.getAbsolutePath());
    kdc = new MiniKdc(kdcProps, kdcHome);
    kdc.start();

    createPrincipal(serverKeytab, "kafka/localhost");
    createPrincipal(clientKeytab, "client");
    createPrincipal(clientKeytab, "client2");

    // This will cause a reload of the Configuration singleton when `getConfiguration` is called.
    Configuration.setConfiguration(null);

    System.setProperty(JAAS_CONF, jaasFilePath);
    System.setProperty(ZK_AUTH_PROVIDER, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
    super.setUp();
  }

  private void createPrincipal(File keytab, String principalNoRealm) throws Exception {
    Seq<String> principals = JavaConverters.asScalaBuffer(
            Arrays.asList(principalNoRealm)
    ).toList();
    kdc.createPrincipal(keytab, principals);
  }

  @Override
  protected KafkaConfig getKafkaConfig(int brokerId) {
    final Option<File> trustStoreFileOption = scala.Option.apply(null);
    final Option<SecurityProtocol> saslInterBrokerSecurityProtocol =
            scala.Option.apply(SecurityProtocol.SASL_PLAINTEXT);
    Properties props = TestUtils.createBrokerConfig(
            brokerId, zkConnect, false, false, TestUtils.RandomPort(), saslInterBrokerSecurityProtocol,
            trustStoreFileOption, EMPTY_SASL_PROPERTIES, false, true, TestUtils.RandomPort(),
            false, TestUtils.RandomPort(),
            false, TestUtils.RandomPort(), Option.<String>empty(), 1, false, 1, (short) 1);

    injectProperties(props);
    props.setProperty("zookeeper.connection.timeout.ms", "30000");
    props.setProperty("sasl.mechanism.inter.broker.protocol", "GSSAPI");
    props.setProperty(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, "GSSAPI");

    return KafkaConfig.fromProps(props);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (kdc != null) {
      kdc.stop();
    }
    LoginManager.closeAll();
    System.clearProperty(JAAS_CONF);
    System.clearProperty(ZK_AUTH_PROVIDER);
    Configuration.setConfiguration(null);
    super.tearDown();
  }
}
