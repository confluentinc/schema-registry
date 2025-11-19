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

import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * ClusterTestHarness implementation of REST API integration tests.
 */
public class RestApiClusterTest extends RestApiTest {

  private ClusterTestHarness harness;

  public RestApiClusterTest() {
    this.harness = new LocalClusterTestHarness(1, true);
  }

  @BeforeEach
  public void setUpTest(TestInfo testInfo) throws Exception {
    harness.setUpTest(testInfo);
    setUpTest(harness.getRestApp());
  }

  @AfterEach
  public void tearDown() throws Exception {
    harness.tearDown();
  }

  static class LocalClusterTestHarness extends ClusterTestHarness {
    public LocalClusterTestHarness(int numBrokers, boolean setupRestApp) {
      super(numBrokers, setupRestApp);
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
}
