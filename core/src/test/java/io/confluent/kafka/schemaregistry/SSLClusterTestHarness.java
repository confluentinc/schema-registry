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

import java.util.Optional;
import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.security.auth.SecurityProtocol;
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

  @Override
  protected SecurityProtocol getBrokerSecurityProtocol() {
    return SecurityProtocol.SSL;
  }

  @Override
  protected KafkaConfig getKafkaConfig(int brokerId) {
    File trustStoreFile;
    try {
      trustStoreFile = File.createTempFile("SSLClusterTestHarness-truststore", ".jks");
    } catch (IOException ioe) {
      throw new RuntimeException("Unable to create temporary file for the truststore.");
    }
    final Optional<File> trustStoreFileOption = Optional.of(trustStoreFile);
    final Optional<SecurityProtocol> sslInterBrokerSecurityProtocol = Optional.of(SecurityProtocol.SSL);
    Properties props = createBrokerConfig(
            brokerId, false, false, TestUtils.RandomPort(), sslInterBrokerSecurityProtocol,
            trustStoreFileOption, EMPTY_SASL_PROPERTIES, false, false, TestUtils.RandomPort(),
            true, TestUtils.RandomPort(), false, TestUtils.RandomPort(), Optional.empty(), false,
            1, (short) 1, false);

    // setup client SSL. Needs to happen before the broker is initialized, because the client's cert
    // needs to be added to the broker's trust store.
    try {
      this.clientSslConfigs = TestSslUtils.createSslConfig(true, true, ConnectionMode.CLIENT,
              trustStoreFile, "client", "localhost");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    injectProperties(props);
    if (requireSSLClientAuth()) {
      props.setProperty("ssl.client.auth", "required");
    }

    return KafkaConfig.fromProps(props);
  }

  protected boolean requireSSLClientAuth() {
    return true;
  }
}
