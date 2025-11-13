/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.SchemaRegistryTestHarness;
import org.apache.kafka.common.config.types.Password;

import javax.security.auth.login.Configuration;
import java.io.File;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * ClusterTestHarness implementation of SSL REST API integration tests.
 */
public class RestApiSslTest extends ClusterTestHarness implements RestApiSslTestSuite {

  private Properties sslProps = new Properties();

  public RestApiSslTest() {
    super(1, true, CompatibilityLevel.BACKWARD.name);
  }

  @Override
  public SchemaRegistryTestHarness getHarness() {
    return this;
  }

  @Override
  public String getSchemaRegistryProtocol() {
    return "https";
  }

  @Override
  public Properties getSslProperties() {
    return sslProps;
  }

  @Override
  public Properties getSchemaRegistryProperties() {
    Configuration.setConfiguration(null);
    sslProps.put(
        SchemaRegistryConfig.SCHEMAREGISTRY_INTER_INSTANCE_PROTOCOL_CONFIG,
        "https"
    );
    sslProps.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    try {
      File trustStoreFile = File.createTempFile("truststore", ".jks");
      trustStoreFile.deleteOnExit();
      List<X509Certificate> clientCerts = new ArrayList<>();

      List<KeyPair> keyPairs = new ArrayList<>();
      sslProps.putAll(
          SecureTestUtils.clientSslConfigsWithKeyStore(1, trustStoreFile, new Password
                  ("TrustPassword"), clientCerts,
              keyPairs
          ));
      sslProps.put(SchemaRegistryConfig.SSL_CLIENT_AUTHENTICATION_CONFIG, SchemaRegistryConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED);

    } catch (Exception e) {
      throw new RuntimeException("Exception creation SSL properties ", e);
    }

    // Use localhost instead of 0.0.0.0 to avoid 400 Invalid SNI
    sslProps.put(SchemaRegistryConfig.LISTENERS_CONFIG, "https://localhost:" + getHarness().getSchemaRegistryPort());

    return sslProps;
  }
}
