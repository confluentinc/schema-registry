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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.SchemaRegistryTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.util.Properties;

/**
 * Kafka-based implementation of REST API schema-too-large integration tests.
 */
public class RestApiSchemaTooLargeTest extends AbstractRestApiSchemaTooLargeTest {

  private ClusterTestHarness harness;

  @BeforeEach
  public void setUpTest(TestInfo testInfo) throws Exception {
    harness = new ClusterTestHarness(1, true){
      @Override
      public Properties getSchemaRegistryProperties() throws Exception {
        return RestApiSchemaTooLargeTest.this.getSchemaRegistryProperties();
      }
      
      @Override
      public void injectProperties(Properties props) {
        RestApiSchemaTooLargeTest.this.injectProperties(props);
      }
    };
    harness.setUpTest(testInfo);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (harness != null) {
      harness.tearDown();
    }
  }

  @Override
  protected SchemaRegistryTestHarness getHarness() {
    return harness;
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    return new Properties();
  }

  @Override
  protected void injectProperties(Properties props) {
    // Lower the message max bytes to induce schema too large exception
    props.setProperty("message.max.bytes", "900");
  }
}
