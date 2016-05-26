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

import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.test.TestSslUtils;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class SSLClusterTestHarness extends ClusterTestHarness {
  public Map<String, Object> clientSslConfigs;

  public SSLClusterTestHarness() {
    super(DEFAULT_NUM_BROKERS);
  }

  protected SecurityProtocol getSecurityProtocol() {
    return SecurityProtocol.SSL;
  }

  protected KafkaConfig getKafkaConfig(int brokerId) {
    File trustStoreFile;
    try {
      trustStoreFile = File.createTempFile("SSLClusterTestHarness-truststore", ".jks");
    } catch (IOException ioe) {
      throw new RuntimeException("Unable to create temporary file for the truststore.");
    }
    final Option<File> trustStoreFileOption = scala.Option.apply(trustStoreFile);
    final Option<SecurityProtocol> sslInterBrokerSecurityProtocol = scala.Option.apply(SecurityProtocol.SSL);
    Properties props = TestUtils.createBrokerConfig(
            brokerId, zkConnect, false, false, TestUtils.RandomPort(), sslInterBrokerSecurityProtocol,
            trustStoreFileOption, SASL_PROPERTIES, false, false, TestUtils.RandomPort(), true, TestUtils.RandomPort(),
            false, TestUtils.RandomPort(), Option.<String>empty());

    // setup client SSL. Needs to happen before the broker is initialized, because the client's cert
    // needs to be added to the broker's trust store.
    Map<String, Object> sslConfigs;
    try {
      this.clientSslConfigs = TestSslUtils.createSslConfig(true, true, Mode.CLIENT,
              trustStoreFile, "client", "localhost");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    injectProperties(props);
    if (requireSSLClientAuth()) {
      props.setProperty("ssl.client.auth", "required");
    }
    // We *must* override this to use the port we allocated (Kafka currently allocates one port
    // that it always uses for ZK
    props.setProperty("zookeeper.connect", this.zkConnect);

    return KafkaConfig.fromProps(props);
  }

  protected boolean requireSSLClientAuth() {
    return true;
  }
}
