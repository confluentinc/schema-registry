/**
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
 **/

package io.confluent.kafka.schemaregistry.storage;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.junit.Before;
import org.junit.Test;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.utils.TestUtils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SchemaRegistryDedicatedZkTest extends ClusterTestHarness {

  public SchemaRegistryDedicatedZkTest() {
    super(1, true, false, AvroCompatibilityLevel.BACKWARD.name);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    // Initialize the rest app ourselves so we can ensure we don't pass any info about the Kafka
    // zookeeper. The format for this config includes the security protocol scheme in the URLs so
    // we can't use the pre-generated server list.
    String[] serverUrls = new String[servers.size()];
    ListenerName listenerType = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
    for(int i = 0; i < servers.size(); i++) {
      serverUrls[i] = "PLAINTEXT://" +
                      Utils.formatAddress(
                          servers.get(i).config().advertisedListeners().head().host(),
                          servers.get(i).boundPort(listenerType)
                      );
    }
    String bootstrapServers = Utils.join(serverUrls, ",");

    restApp = new RestApp(choosePort(), srZkConnect, null, bootstrapServers, KAFKASTORE_TOPIC,
                          compatibilityType, true, null);
    restApp.start();
  }

  @Test
  public void testDedicatedZk() throws Exception {
    restApp.restClient.registerSchema(TestUtils.getRandomCanonicalAvroString(1).get(0), "foo");

    // Schema registry Zookeeper shouldn't have Kafka-related paths, should have SR nodes
    assertFalse(srZkClient.exists("/controller"));
    assertFalse(srZkClient.exists("/cluster"));
    assertTrue(srZkClient.exists("/schema_registry/schema_id_counter"));
    assertTrue(srZkClient.exists("/schema_registry/schema_registry_master"));

    // Kafka Zookeeper should have Kafka-related paths, should not have SR nodes
    assertTrue(zkClient.exists("/controller"));
    assertTrue(zkClient.exists("/cluster"));
    assertFalse(zkClient.exists("/schema_registry/schema_id_counter"));
    assertFalse(zkClient.exists("/schema_registry/schema_registry_master"));
  }
}
