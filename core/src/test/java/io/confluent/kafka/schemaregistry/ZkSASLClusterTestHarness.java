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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class ZkSASLClusterTestHarness extends ClusterTestHarness {
  public static final String JAAS_CONF = "java.security.auth.login.config";

  private static MiniKdc kdc;
  private static File kdcHome;

  private static final Logger log = LoggerFactory.getLogger(ZkSASLClusterTestHarness.class);

  public ZkSASLClusterTestHarness() {
    super(DEFAULT_NUM_BROKERS);
  }

  @BeforeClass
  public static void setUpKdc() throws Exception {
    Properties kdcProps = MiniKdc.createConfig();
    kdcHome = Files.createTempDirectory("mini-kdc").toFile();
    log.info("Using KDC home: " + kdcHome.getAbsolutePath());
    kdc = new MiniKdc(kdcProps, kdcHome);
    kdc.start();

    // create client and server principals in the KDC.
    File zkServerKeytab = createPrincipal("zookeeper-", "zookeeper/localhost");
    File kafkaKeytab = createPrincipal("kafka-", "kafka/localhost");

    // build and write the JAAS file.
    JaasTestUtils.JaasSection serverSection = createJaasSection(zkServerKeytab,
            "zookeeper/localhost@EXAMPLE.COM", "Server");
    JaasTestUtils.JaasSection clientSection = createJaasSection(kafkaKeytab,
            "kafka/localhost@EXAMPLE.COM", "Client");
    // IMPORTANT: there is only one `Client` section in the Jaas configuraiton file. Both the internal embedded Kafka
    // cluster and the schema registry share the same principal. This is required because within the same JVM (eg
    // these tests) one cannot have two sections, each with its own ZooKeeper client SASL credentials.
    File zkJaas = File.createTempFile("schema_registry_tests", "_jaas.conf");
    PrintWriter out = new PrintWriter(zkJaas);
    out.println(serverSection.toString());
    out.println(clientSection.toString());
    out.close();

    // don't need to set java.security.krb5.conf because the MiniKdc does it.

    // Once this is set, it can't be changed, for example by a subsequent test. As such, the KDC and keytab file are
    // setup only once for all tests. This can't be changed most likely because the ZooKeeper client library
    // stores the original value and doesn't react to changes to it.
    System.setProperty(JAAS_CONF, zkJaas.getAbsolutePath());

    // we *must* set a SASL provider. Frankly, it's unclear why. Perhaps because the ZooKeeper and Kafka test
    // utils don't set this properly? Kafka's tests manually set this, too.
    System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
  }

  @Override
  protected void injectProperties(Properties props) {
    props.setProperty("zookeeper.connection.timeout.ms", "30000");
    super.injectProperties(props);
  }

  @Override
  protected KafkaConfig getKafkaConfig(int brokerId) {
    KafkaConfig conf = super.getKafkaConfig(brokerId);
    System.out.println("--------------------");
    System.out.println(conf.zkEnableSecureAcls());
    System.out.println("---------------");
    return conf;
  }

  private static File createPrincipal(String pathPrefix, String principalNoRealm) throws Exception {
    File keytab = File.createTempFile(pathPrefix, ".keytab");
    Seq<String> principals = scala.collection.JavaConversions.asScalaBuffer(
            Arrays.asList(principalNoRealm)
    ).seq();
    kdc.createPrincipal(keytab, principals);
    return keytab;
  }

  private static JaasTestUtils.JaasSection createJaasSection(File keytab, String principalWithRealm, String jaasContextName) {
    final scala.Option<String> emptyOption = scala.Option.apply(null);
    JaasTestUtils.Krb5LoginModule krbModule = new JaasTestUtils.Krb5LoginModule(true, true,
            keytab.getAbsolutePath(), principalWithRealm, false, emptyOption);
    Seq<JaasTestUtils.JaasModule> jaasModules = scala.collection.JavaConversions.asScalaBuffer(
            Arrays.asList(krbModule.toJaasModule())
    ).seq();
    return new JaasTestUtils.JaasSection(jaasContextName, jaasModules);
  }

  @AfterClass
  public static void tearDownKdc() throws Exception {
    kdc.stop();
    if (!kdcHome.delete()) {
      log.warn("Could not delete the KDC directory.");
    }

    System.clearProperty(JAAS_CONF);
  }
}
