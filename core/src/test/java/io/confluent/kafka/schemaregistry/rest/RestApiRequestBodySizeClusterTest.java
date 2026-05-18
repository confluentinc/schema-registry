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

package io.confluent.kafka.schemaregistry.rest;

import static io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.MAX_REQUEST_BODY_SIZE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Properties;

@Tag("IntegrationTest")
public class RestApiRequestBodySizeClusterTest {

  protected ClusterTestHarness harness;
  protected RestApp restApp;

  public RestApiRequestBodySizeClusterTest() {
    this.harness = new ClusterTestHarness(1, true);
    Properties props = new Properties();
    props.setProperty(MAX_REQUEST_BODY_SIZE_CONFIG, "1024"); // 1KB limit
    this.harness.injectSchemaRegistryProperties(props);  // Pass props, not new Properties()!
  }

  @BeforeEach
  public void setUpTest(TestInfo testInfo) throws Exception {
    harness.setUpTest(testInfo);
    this.restApp = harness.getRestApp();
  }


  @AfterEach
  public void tearDown() throws Exception {
    harness.tearDown();
  }

  /**
   * Generate a large Avro schema with a very long documentation string.
   */
  private String generateLargeSchema(int targetSizeBytes) {
    StringBuilder doc = new StringBuilder();
    // Fill doc string to reach target size
    while (doc.length() < targetSizeBytes) {
      doc.append("This is a very long documentation string. ");
    }

    return "{\"namespace\": \"namespace\",\n"
        + " \"type\": \"record\",\n"
        + " \"name\": \"LargeSchemaTest\",\n"
        + " \"fields\": [\n"
        + "     {\"name\": \"field1\", \"type\": \"string\", \"doc\": \"" + doc + "\"}\n"
        + "]\n"
        + "}";
  }

  /**
   * Generate a small schema that is well under the limit
   */
  private String generateSmallSchema() {
    return "{\"namespace\": \"namespace\",\n"
        + " \"type\": \"record\",\n"
        + " \"name\": \"SmallSchemaTest\",\n"
        + " \"fields\": [\n"
        + "     {\"name\": \"field1\", \"type\": \"string\"},\n"
        + "     {\"name\": \"field2\", \"type\": \"int\"},\n"
        + "     {\"name\": \"field3\", \"type\": \"boolean\"}\n"
        + "]\n"
        + "}";
  }

  @Test
  public void testRegisterSchemaExceedsBodySizeLimit() throws Exception {
    // Generate a schema larger than the configured limit (1KB)
    String largeSchema = generateLargeSchema(2048); // 2KB - exceeds 1KB limit
    String subject = "testLargeRequestBody";

    try {
      restApp.restClient.registerSchema(largeSchema, subject);
      fail("Registering a schema with body size exceeding limit should fail");
    } catch (RestClientException e) {
      // The SizeLimitHandler returns HTTP 413 (Payload Too Large)
      assertEquals(413, e.getStatus(),
          "Expected HTTP 413 for request body exceeding size limit");
    }
  }

  @Test
  public void testRegisterSchemaWithinBodySizeLimit() throws Exception {
    // Generate a small schema well under the 1KB limit
    String smallSchema = generateSmallSchema();
    String subject = "testSmallRequestBody";

    try {
      // This should succeed
      int schemaId = restApp.restClient.registerSchema(smallSchema, subject);
      assertTrue(schemaId > 0,
          "Schema registration should succeed and return positive schema ID");
    } catch (RestClientException e) {
      fail("Registering a small schema should not fail with error: " + e.getMessage());
    }
  }
}
