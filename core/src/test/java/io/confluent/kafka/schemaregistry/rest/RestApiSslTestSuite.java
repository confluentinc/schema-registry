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

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.SchemaRegistryTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.apache.avro.Schema;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Interface for SSL REST API integration tests with default test implementations.
 * Implementing classes must provide test harness implementation.
 */
public interface RestApiSslTestSuite {

  /**
   * Get the test harness.
   */
  SchemaRegistryTestHarness getHarness();

  /**
   * Helper method to get the RestApp from the harness.
   */
  default RestApp restApp() {
    return getHarness().getRestApp();
  }

  /**
   * Get SSL properties for schema registry.
   * Override to provide SSL-specific configuration.
   */
  Properties getSslProperties();

  @Test
  default void testRegisterWithClientSecurity() throws Exception {
    setupHostNameVerifier();

    String subject = "testSubject";
    Schema schema = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").rawSchema();

    int expectedIdSchema1 = 1;

    Properties sslProps = getSslProperties();
    Map clientsslConfigs = new HashMap();
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_PROTOCOL_CONFIG,
        "TLS");
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_PASSWORD_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_KEYSTORE_PASSWORD_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEY_PASSWORD_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_KEYSTORE_PASSWORD_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_TYPE_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_KEYSTORE_TYPE_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_LOCATION_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_TRUSTSTORE_LOCATION_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_TYPE_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_TRUSTSTORE_TYPE_CONFIG));
    CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restApp().restClient, 10, clientsslConfigs);

    assertEquals(
        expectedIdSchema1,
        schemaRegistryClient.register(subject, new AvroSchema(schema)),
        "Registering should succeed"
    );

  }


  @Test
  default void testRegisterWithClientSecurityWithMinimalProperties() throws Exception {

    setupHostNameVerifier();

    String subject = "testSubject";
    Schema schema = AvroUtils.parseSchema(
        "{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":\"f2\"}]}").rawSchema();

    int expectedIdSchema1 = 1;

    Properties sslProps = getSslProperties();
    Map clientsslConfigs = new HashMap();
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_PASSWORD_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_KEYSTORE_PASSWORD_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_TYPE_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_KEYSTORE_TYPE_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_LOCATION_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_TRUSTSTORE_LOCATION_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_TYPE_CONFIG,
        sslProps.get(SchemaRegistryConfig.SSL_TRUSTSTORE_TYPE_CONFIG));
    CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restApp().restClient, 10, clientsslConfigs);

    assertEquals(
        expectedIdSchema1,
        schemaRegistryClient.register(subject, new AvroSchema(schema)),
        "Registering should succeed"
    );

  }

  default void setupHostNameVerifier() {
      // Create all-trusting host name verifier
      HostnameVerifier allHostsValid = new HostnameVerifier() {
        public boolean verify(String hostname, SSLSession session) {
          return true;
        }
      };
      // Install the all-trusting host verifier
      HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
  }

}
