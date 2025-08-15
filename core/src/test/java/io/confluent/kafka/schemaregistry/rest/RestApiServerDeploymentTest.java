/*
 * Copyright 2025 Confluent Inc.
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

import static org.junit.Assert.assertEquals;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryDeployment;
import org.junit.Test;

public class RestApiServerDeploymentTest extends ClusterTestHarness {

  public RestApiServerDeploymentTest() {
    super(1, true);
  }


  @Test
  public void testGetSchemaRegistryServerDeploymentWithCustomProperty() throws Exception {
    SchemaRegistryDeployment srDeployment = restApp.restClient.getSchemaRegistryDeployment();
    assertEquals("opensource", srDeployment.getAttributes().get(0));
  }
}
