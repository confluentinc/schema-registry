/*
 * Copyright 2021 Confluent Inc.
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * ClusterTestHarness implementation of context REST API integration tests.
 */
public class RestApiContextTest extends ClusterTestHarness implements RestApiContextTestSuite {

  @BeforeEach
  public void setUpTest(TestInfo testInfo) throws Exception {
    super.setUp();
  }

  @Override
  public SchemaRegistryTestHarness getHarness() {
    return this;
  }
}
