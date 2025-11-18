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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.SchemaRegistryTestHarness;

import java.util.Properties;

/**
 * ClusterTestHarness implementation of REST API integration tests.
 */
public class RestApiClusterTest extends ClusterTestHarness implements RestApiTest {

  public RestApiClusterTest() {
    super(1, true);
  }

  @Override
  public SchemaRegistryTestHarness getHarness() {
    return this;
  }

  @Override
  public Properties getSchemaRegistryProperties() throws Exception {
    Properties schemaRegistryProps = super.getSchemaRegistryProperties();
    schemaRegistryProps.put("response.http.headers.config",
            "add X-XSS-Protection: 1; mode=block, \"add Cache-Control: no-cache, no-store, must-revalidate\"");
    schemaRegistryProps.put("schema.providers.avro.validate.defaults", "true");
    return schemaRegistryProps;
  }
}
