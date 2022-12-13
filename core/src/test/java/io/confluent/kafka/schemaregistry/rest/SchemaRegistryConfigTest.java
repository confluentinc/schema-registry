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

package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.rest.NamedURI;
import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;
import kafka.Kafka;
import kafka.cluster.Broker;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class SchemaRegistryConfigTest {

  @Test
  public void testFilterBrokerEndpointsSinglePlaintext() {
    String endpoint = "PLAINTEXT://hostname:1234";
    List<String> endpointsList = new ArrayList<String>();
    endpointsList.add(endpoint);
    assertEquals("Expected one PLAINTEXT endpoint for localhost", endpoint,
        SchemaRegistryConfig
            .endpointsToBootstrapServers(endpointsList, SecurityProtocol.PLAINTEXT.toString())
    );
  }

  @Test(expected = ConfigException.class)
  public void testGetBrokerEndpointsEmpty() {
    SchemaRegistryConfig.endpointsToBootstrapServers(
        new ArrayList<String>(),
        SecurityProtocol.PLAINTEXT.toString()
    );
  }

  @Test(expected = ConfigException.class)
  public void testGetBrokerEndpointsNoSecurityProtocolMatches() {
    SchemaRegistryConfig.endpointsToBootstrapServers(
        Collections.singletonList("SSL://localhost:1234"),
        SecurityProtocol.PLAINTEXT.toString()
    );
  }

  @Test(expected = ConfigException.class)
  public void testGetBrokerEndpointsUnsupportedSecurityProtocol() {
    SchemaRegistryConfig
        .endpointsToBootstrapServers(Collections.singletonList("TRACE://localhost:1234"), "TRACE");
  }

  @Test
  public void testGetBrokerEndpointsMixed() throws IOException {
    List<String> endpointsList = new ArrayList<String>(4);
    endpointsList.add("PLAINTEXT://localhost0:1234");
    endpointsList.add("PLAINTEXT://localhost1:1234");
    endpointsList.add("SASL_PLAINTEXT://localhost1:1235");
    endpointsList.add("SSL://localhost1:1236");
    endpointsList.add("SASL_SSL://localhost2:1234");
    endpointsList.add("TRACE://localhost3:1234");

    assertEquals(
        "PLAINTEXT://localhost0:1234,PLAINTEXT://localhost1:1234",
        SchemaRegistryConfig
            .endpointsToBootstrapServers(endpointsList, SecurityProtocol.PLAINTEXT.toString())
    );

    assertEquals(
        "SASL_PLAINTEXT://localhost1:1235",
        SchemaRegistryConfig
            .endpointsToBootstrapServers(endpointsList, SecurityProtocol.SASL_PLAINTEXT.toString())
    );

    assertEquals(
        "SSL://localhost1:1236",
        SchemaRegistryConfig
            .endpointsToBootstrapServers(endpointsList, SecurityProtocol.SSL.toString())
    );

    assertEquals(
        "SASL_SSL://localhost2:1234",
        SchemaRegistryConfig
            .endpointsToBootstrapServers(endpointsList, SecurityProtocol.SASL_SSL.toString())
    );

    List<String> noprotocolEndpointsList = new ArrayList<String>();
    noprotocolEndpointsList.add("localhost0:1234");
    noprotocolEndpointsList.add("localhost1:1234");

    assertEquals(
        "PLAINTEXT://localhost0:1234,PLAINTEXT://localhost1:1234",
        SchemaRegistryConfig
            .endpointsToBootstrapServers(noprotocolEndpointsList, SecurityProtocol.PLAINTEXT.toString())
    );
  }

  @Test
  public void testBrokersToEndpoints() {
    List<Broker> brokersList = new ArrayList<Broker>(4);
    brokersList
        .add(new Broker(0, "localhost", 1, new ListenerName("CLIENT"), SecurityProtocol.PLAINTEXT));
    brokersList.add(new Broker(1,
        "localhost1",
        12,
        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
        SecurityProtocol.PLAINTEXT
    ));
    brokersList.add(new Broker(2,
        "localhost2",
        123,
        new ListenerName("SECURE_REPLICATION"),
        SecurityProtocol.SASL_PLAINTEXT
    ));
    brokersList.add(new Broker(2,
        "localhost2",
        123,
        ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT),
        SecurityProtocol.SASL_PLAINTEXT
    ));
    brokersList.add(new Broker(3,
        "localhost3",
        1234,
        ListenerName.forSecurityProtocol(SecurityProtocol.SSL),
        SecurityProtocol.SSL
    ));
    List<String> endpointsList = SchemaRegistryConfig.brokersToEndpoints((brokersList));

    List<String> expected = new ArrayList<String>(4);
    expected.add("PLAINTEXT://localhost:1");
    expected.add("PLAINTEXT://localhost1:12");
    expected.add("SASL_PLAINTEXT://localhost2:123");
    expected.add("SASL_PLAINTEXT://localhost2:123");
    expected.add("SSL://localhost3:1234");

    assertEquals("Expected the same size list.", expected.size(), endpointsList.size());

    for (int i = 0; i < endpointsList.size(); i++) {
      assertEquals("Expected a different endpoint", expected.get(i), endpointsList.get(i));
    }
  }

  @Test
  public void useDeprecatedInterInstanceProtocolIfDefined() throws RestConfigException {
    Properties props = new Properties();
    // We should obey the deprecated value if it is defined, even if the new one is defined as well
    props.setProperty(SchemaRegistryConfig.SCHEMAREGISTRY_INTER_INSTANCE_PROTOCOL_CONFIG, "https");
    props.setProperty(SchemaRegistryConfig.INTER_INSTANCE_PROTOCOL_CONFIG, "foo");
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    assertEquals("https", config.interInstanceProtocol());
  }

  @Test
  public void unprefixedInterInstanceProtocol() throws RestConfigException {
    Properties props = new Properties();
    // Validate that setting the new config name only works
    props.setProperty(SchemaRegistryConfig.INTER_INSTANCE_PROTOCOL_CONFIG, "https");
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    assertEquals("https", config.interInstanceProtocol());
  }

  @Test
  public void defaultInterInstanceProtocol() throws RestConfigException {
    Properties props = new Properties();
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    assertEquals("http", config.interInstanceProtocol());
  }

  @Test
  public void defaultMutabilityMode() throws RestConfigException {
    Properties props = new Properties();
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    assertEquals(true, config.getBoolean(SchemaRegistryConfig.MODE_MUTABILITY));
  }

  @Test
  public void testSslConfigOverride() throws RestConfigException, SchemaRegistryException {
    Properties props = new Properties();

    props.setProperty(RestConfig.LISTENERS_CONFIG, "alice://localhost:123, bob://localhost:456, https://localhost:789");
    props.setProperty(SchemaRegistryConfig.INTER_INSTANCE_LISTENER_NAME_CONFIG, "alice");
    props.setProperty(RestConfig.LISTENER_PROTOCOL_MAP_CONFIG, "alice:https, bob:https");
    props.setProperty(RestConfig.SSL_KEYSTORE_LOCATION_CONFIG, "/mnt/keystore/keystore.jks");
    props.setProperty("listener.name.alice." + RestConfig.SSL_KEYSTORE_LOCATION_CONFIG , "/mnt/keystore/internal/keystore.jks");
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);

    NamedURI internalListener = KafkaSchemaRegistry.getInterInstanceListener(config.getListeners(),
      config.getString(SchemaRegistryConfig.INTER_INSTANCE_LISTENER_NAME_CONFIG),
      SchemaRegistryConfig.HTTPS);
    Map<String, Object> overrides = config.getOverriddenSslConfigs(internalListener);
    assertEquals("/mnt/keystore/internal/keystore.jks", overrides.get(SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG));

    NamedURI externalListener = config.getListeners().stream()
        .filter(l -> Optional.ofNullable(l.getName()).orElse("").equalsIgnoreCase("bob"))
        .collect(Collectors.toList()).get(0);
    overrides = config.getOverriddenSslConfigs(externalListener);
    assertEquals("/mnt/keystore/keystore.jks", overrides.get(SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG));

    NamedURI unnamedListener = config.getListeners().stream().filter(l -> l.getName() == null).collect(Collectors.toList()).get(0);
    overrides = config.getOverriddenSslConfigs(unnamedListener);
    assertEquals("/mnt/keystore/keystore.jks", overrides.get(SchemaRegistryConfig.SSL_KEYSTORE_LOCATION_CONFIG));

  }

}
