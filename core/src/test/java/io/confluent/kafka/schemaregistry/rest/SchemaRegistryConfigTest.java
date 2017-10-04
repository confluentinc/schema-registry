/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import io.confluent.common.config.ConfigException;
import kafka.cluster.Broker;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
}
