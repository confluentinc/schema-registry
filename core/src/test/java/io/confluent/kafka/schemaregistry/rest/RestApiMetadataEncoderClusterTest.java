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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.util.Properties;

public class RestApiMetadataEncoderClusterTest extends RestApiMetadataEncoderTest {

  protected ClusterTestHarness harness;

  public RestApiMetadataEncoderClusterTest() {
    this.harness = new ClusterTestHarness(1, true, CompatibilityLevel.BACKWARD.name);
    this.harness.injectSchemaRegistryProperties(getSchemaRegistryProperties());
  }

  @BeforeEach
  public void setUpTest(TestInfo testInfo) throws Exception {
    harness.setUpTest(testInfo);
    setRestApp(harness.getRestApp());
  }

  @AfterEach
  public void tearDown() throws Exception {
    harness.tearDown();
  }

  public Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG, "mysecret");
    return props;
  }
}

