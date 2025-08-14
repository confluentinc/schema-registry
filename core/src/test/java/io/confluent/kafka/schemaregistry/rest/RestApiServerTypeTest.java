/*
 * Copyright 2022 Confluent Inc.
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
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryType;
import io.confluent.kafka.schemaregistry.utils.Props;
import java.util.Properties;
import org.junit.Test;

public class RestApiServerTypeTest extends ClusterTestHarness {

  public RestApiServerTypeTest() {
    super(1, true);
  }

  @Override
  protected Properties getSchemaRegistryProperties() throws Exception {
    Properties props = new Properties();
    props.setProperty(Props.PROPERTY_SCHEMA_REGISTRY_TYPE_ATTRIBUTES, "enterprise");
    return props;
  }

  @Test
  public void testGetSchemaRegistryServerTypeWithCustomProperty() throws Exception {
    SchemaRegistryType srType = restApp.restClient.getSchemaRegistryType();
    assertEquals("enterprise", srType.getAttributes().get(0));
  }
}
