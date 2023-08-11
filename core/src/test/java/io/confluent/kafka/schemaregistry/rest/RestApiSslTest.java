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
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.apache.avro.Schema;
import org.apache.kafka.common.config.types.Password;
import org.junit.Test;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.security.auth.login.Configuration;
import java.io.File;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class RestApiSslTest extends ClusterTestHarness {

  Properties props = new Properties();

  public RestApiSslTest() {
    super(1, true, AvroCompatibilityLevel.BACKWARD.name);
  }


  @Test
  public void testRegisterWithClientSecurity() throws Exception {

    setupHostNameVerifier();

    String subject = "testSubject";
    Schema schema = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").rawSchema();

    int expectedIdSchema1 = 1;

    Map clientsslConfigs = new HashMap();
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_PROTOCOL_CONFIG,
        "TLS");
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG,
        props.get(SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_PASSWORD_CONFIG,
        props.get(SchemaRegistryConfig.SSL_KEYSTORE_PASSWORD_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEY_PASSWORD_CONFIG,
        props.get(SchemaRegistryConfig.SSL_KEYSTORE_PASSWORD_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_TYPE_CONFIG,
        props.get(SchemaRegistryConfig.SSL_KEYSTORE_TYPE_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_LOCATION_CONFIG,
        props.get(SchemaRegistryConfig.SSL_TRUSTSTORE_LOCATION_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG,
        props.get(SchemaRegistryConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_TYPE_CONFIG,
        props.get(SchemaRegistryConfig.SSL_TRUSTSTORE_TYPE_CONFIG));
    CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restApp.restClient, 10, clientsslConfigs);

    assertEquals(
        "Registering should succeed",
        expectedIdSchema1,
        schemaRegistryClient.register(subject, schema)
    );

  }


  @Test
  public void testRegisterWithClientSecurityWithMinimalProperties() throws Exception {

    setupHostNameVerifier();

    String subject = "testSubject";
    Schema schema = AvroUtils.parseSchema(
        "{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":\"f2\"}]}").rawSchema();

    int expectedIdSchema1 = 1;

    Map clientsslConfigs = new HashMap();
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG,
        props.get(SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_PASSWORD_CONFIG,
        props.get(SchemaRegistryConfig.SSL_KEYSTORE_PASSWORD_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_KEYSTORE_TYPE_CONFIG,
        props.get(SchemaRegistryConfig.SSL_KEYSTORE_TYPE_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_LOCATION_CONFIG,
        props.get(SchemaRegistryConfig.SSL_TRUSTSTORE_LOCATION_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG,
        props.get(SchemaRegistryConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    clientsslConfigs.put(
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SchemaRegistryConfig.SSL_TRUSTSTORE_TYPE_CONFIG,
        props.get(SchemaRegistryConfig.SSL_TRUSTSTORE_TYPE_CONFIG));
    CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restApp.restClient, 10, clientsslConfigs);

    assertEquals(
        "Registering should succeed",
        expectedIdSchema1,
        schemaRegistryClient.register(subject, schema)
    );

  }


  @Override
  protected Properties getSchemaRegistryProperties() {
    Configuration.setConfiguration(null);
    props.put(
        SchemaRegistryConfig.SCHEMAREGISTRY_INTER_INSTANCE_PROTOCOL_CONFIG,
        "https"
    );
    props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    try {
      File trustStoreFile = File.createTempFile("truststore", ".jks");
      trustStoreFile.deleteOnExit();
      List<X509Certificate> clientCerts = new ArrayList<>();

      List<KeyPair> keyPairs = new ArrayList<>();
      props.putAll(
          SecureTestUtils.clientSslConfigsWithKeyStore(1, trustStoreFile, new Password
                  ("TrustPassword"), clientCerts,
              keyPairs
          ));
      props.put(SchemaRegistryConfig.SSL_CLIENT_AUTHENTICATION_CONFIG, SchemaRegistryConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED);

    } catch (Exception e) {
      throw new RuntimeException("Exception creation SSL properties ", e);
    }
    return props;
  }

  @Override
  protected String getSchemaRegistryProtocol() {
    return "https";
  }

  private void setupHostNameVerifier() {
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
