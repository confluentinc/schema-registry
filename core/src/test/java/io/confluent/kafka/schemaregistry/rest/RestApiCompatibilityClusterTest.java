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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.storage.RuleSetHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public class RestApiCompatibilityClusterTest extends RestApiCompatibilityTest {

  protected ClusterTestHarness harness;

  public RestApiCompatibilityClusterTest() {
    this.harness = new ClusterTestHarness(1, true, CompatibilityLevel.BACKWARD.name);
  }

  @BeforeEach
  public void setUpTest(TestInfo testInfo) throws Exception {
    harness.setUpTest(testInfo);
    setRestApp(harness.getRestApp());
    
    // Set up RuleSetHandler
    harness.getRestApp().schemaRegistry().setRuleSetHandler(new RuleSetHandler() {
      public void handle(String subject, ConfigUpdateRequest request) {
      }

      public void handle(String subject, boolean normalize, RegisterSchemaRequest request) {
      }

      public io.confluent.kafka.schemaregistry.storage.RuleSet transform(RuleSet ruleSet) {
        return ruleSet != null
            ? new io.confluent.kafka.schemaregistry.storage.RuleSet(ruleSet)
            : null;
      }
    });
  }

  @AfterEach
  public void tearDown() throws Exception {
    harness.tearDown();
  }
}

