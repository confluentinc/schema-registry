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

import static io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.MAX_REQ_BODY_SIZE_CONFIG;
import static io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.SIZE_LIMIT_HANDLER_ENABLED_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.Test;


import java.util.Properties;

public class RestApiRequestBodySizeClusterTest extends ClusterTestHarness {

  public RestApiRequestBodySizeClusterTest() {
    super(1, true); // 1 broker, setup restApp
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.setProperty(SIZE_LIMIT_HANDLER_ENABLED_CONFIG, "true"); // 1KB limit
    props.setProperty(MAX_REQ_BODY_SIZE_CONFIG, "1024"); // 1KB limit
    return props;
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
      assertEquals("Expected HTTP 413 for request body exceeding size limit",
          413, e.getStatus());
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
      assertTrue("Schema registration should succeed and return positive schema ID",
          schemaId > 0);
    } catch (RestClientException e) {
      fail("Registering a small schema should not fail with error: " + e.getMessage());
    }
  }
}
