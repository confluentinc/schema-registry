/**
 * Copyright 2016 Confluent Inc.
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

import kafka.security.minikdc.MiniKdc;
import kafka.server.KafkaConfig;
import kafka.utils.JaasTestUtils;
import kafka.utils.TestUtils;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;

import javax.security.auth.login.Configuration;
import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

// sets up SASL for ZooKeeper and Kafka.
public class SASLClusterTestHarness extends ClusterTestHarness {
  public static final String JAAS_CONF = "java.security.auth.login.config";
  public static final String KRB5_CONF = "java.security.krb5.conf";
  public static final String ZK_AUTH_PROVIDER = "zookeeper.authProvider.1";

  private static MiniKdc kdc;
  private static File kdcHome;
  private static String krb5ConfPath;
  private static File jaasFile;

  private static final Logger log = LoggerFactory.getLogger(SASLClusterTestHarness.class);

  public SASLClusterTestHarness() {
    super(DEFAULT_NUM_BROKERS);
  }

  @Override
  protected SecurityProtocol getSecurityProtocol() {
    return SecurityProtocol.SASL_PLAINTEXT;
  }

  @BeforeClass
  public static void setUpKdc() throws Exception {
    destroySaslHelper();
    File zkServerKeytab = File.createTempFile("zookeeper-", ".keytab");
    File kafkaKeytab = File.createTempFile("kafka-", ".keytab");
    File schemaRegistryKeytab = File.createTempFile("schema-registry-", ".keytab");

    // build and write the JAAS file.
    JaasTestUtils.JaasSection serverSection = createJaasSection(zkServerKeytab,
            "zookeeper/localhost@EXAMPLE.COM", "Server", "zookeeper");
    // NOTE: there is only one `Client` section in the Jaas configuraiton file. Both the internal embedded Kafka
    // cluster and the schema registry share the same principal. This is required because within the same JVM (eg
    // these tests) one cannot have two sections, each with its own ZooKeeper client SASL credentials.
    JaasTestUtils.JaasSection clientSection = createJaasSection(kafkaKeytab,
            "kafka/localhost@EXAMPLE.COM", "Client", "zookeeper");
    JaasTestUtils.JaasSection kafkaServerSection = createJaasSection(kafkaKeytab,
            "kafka/localhost@EXAMPLE.COM", "KafkaServer", "kafka");
    JaasTestUtils.JaasSection kafkaClientSection = createJaasSection(schemaRegistryKeytab,
            "schema-registry/localhost@EXAMPLE.COM", "KafkaClient", "kafka");
    jaasFile = File.createTempFile("schema_registry_tests", "_jaas.conf");
    PrintWriter out = new PrintWriter(jaasFile);
    out.println(serverSection.toString());
    out.println(clientSection.toString());
    out.println(kafkaServerSection.toString());
    out.println(kafkaClientSection.toString());
    out.close();

    Properties kdcProps = MiniKdc.createConfig();
    kdcHome = Files.createTempDirectory("mini-kdc").toFile();
    log.info("Using KDC home: " + kdcHome.getAbsolutePath());
    kdc = new MiniKdc(kdcProps, kdcHome);
    kdc.start();

    krb5ConfPath = System.getProperty("java.security.krb5.conf");

    createPrincipal(zkServerKeytab, "zookeeper/localhost");
    createPrincipal(kafkaKeytab, "kafka/localhost");
    createPrincipal(schemaRegistryKeytab, "schema-registry/localhost");

    // destroy SASL settings so all tests don't have SASL enabled. SASL is re-enabled in a @Before method below.
    destroySaslHelper();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    destroySaslHelper();
    System.setProperty(JAAS_CONF, jaasFile.getAbsolutePath());
    System.setProperty(KRB5_CONF, krb5ConfPath);
    System.setProperty(ZK_AUTH_PROVIDER, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
    super.setUp();
  }

  private static void createPrincipal(File keytab, String principalNoRealm) throws Exception {
    Seq<String> principals = scala.collection.JavaConversions.asScalaBuffer(
            Arrays.asList(principalNoRealm)
    ).seq();
    kdc.createPrincipal(keytab, principals);
  }

  private static JaasTestUtils.JaasSection createJaasSection(File keytab, String principalWithRealm,
                                                      String jaasContextName, String serviceName) {
    final scala.Option<String> serviceNameOption = scala.Option.apply(serviceName);
    JaasTestUtils.Krb5LoginModule krbModule = new JaasTestUtils.Krb5LoginModule(true, true,
            keytab.getAbsolutePath(), principalWithRealm, true, serviceNameOption);
    Seq<JaasTestUtils.JaasModule> jaasModules = scala.collection.JavaConversions.asScalaBuffer(
            Arrays.asList(krbModule.toJaasModule())
    ).seq();
    return new JaasTestUtils.JaasSection(jaasContextName, jaasModules);
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
            false, TestUtils.RandomPort(), Option.<String>empty());

    injectProperties(props);
    props.setProperty("zookeeper.connection.timeout.ms", "30000");
    props.setProperty("sasl.mechanism.inter.broker.protocol", "GSSAPI");
    props.setProperty(SaslConfigs.SASL_ENABLED_MECHANISMS, "GSSAPI");

    return KafkaConfig.fromProps(props);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    destroySaslHelper();
    super.tearDown();
  }

  @AfterClass
  public static void tearDownKdc() throws Exception {
    if (kdc != null) {
      kdc.stop();
    }
    if (kdcHome != null && !kdcHome.delete()) {
      log.warn("Could not delete the KDC directory.");
    }
    if (jaasFile != null && !jaasFile.delete()) {
      log.warn("Could not delete the JAAS file.");
    }
    destroySaslHelper();
  }

  private static void destroySaslHelper() {
    LoginManager.closeAll();
    System.clearProperty(JAAS_CONF);
    System.clearProperty(ZK_AUTH_PROVIDER);
    System.clearProperty(KRB5_CONF);
    Configuration.setConfiguration(null);
  }
}
