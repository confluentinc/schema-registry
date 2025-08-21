/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.client;

import static io.confluent.kafka.schemaregistry.storage.SchemaRegistry.DEFAULT_TENANT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryDeployment;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryServerVersion;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.utils.Props;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LocalSchemaRegistryClientTest {

  @Mock
  private KafkaSchemaRegistry mockSchemaRegistry;

  @Mock
  private SchemaProvider mockSchemaProvider;

  @Mock
  private AvroSchemaProvider mockAvroProvider;

  private LocalSchemaRegistryClient client;
  private String testSchemaString;

  @Before
  public void setUp() {
    when(mockSchemaRegistry.tenant()).thenReturn(DEFAULT_TENANT);
    client = new LocalSchemaRegistryClient(mockSchemaRegistry);
  }

  @Test
  public void testGetSchemaRegistryDeployment() throws Exception {
    Map<String, Object> props = new HashMap<>();
    List<String> deploymentAttributes = new ArrayList<String>(Collections.singleton("deploymentScope:opensource"));
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, deploymentAttributes);
    when(mockSchemaRegistry.properties()).thenReturn(props);

    SchemaRegistryDeployment deployment = client.getSchemaRegistryDeployment();

    assertNotNull(deployment);
    assertEquals(deployment.getAttributes(),
        new ArrayList<String>(Collections.singleton("deploymentScope:opensource"))
    );
  }

  @Test
  public void testGetSchemaRegistryServerVersion() throws Exception {
    SchemaRegistryServerVersion version = client.getSchemaRegistryServerVersion();

    assertNotNull(version);
    assertNotNull(version.getVersion());
    assertNotNull(version.getCommitId());
  }
}
