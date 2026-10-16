/*
 * Copyright 2026 Confluent Inc.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("IntegrationTest")
public abstract class RestApiReferenceVersionsStrictTest {

  protected RestApp restApp;

  public void setRestApp(RestApp restApp) {
    this.restApp = restApp;
  }

  private static final String INNER_SCHEMA_V1 =
      "{\"type\":\"record\",\"name\":\"Inner\","
      + "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";

  private static final String INNER_SCHEMA_V2 =
      "{\"type\":\"record\",\"name\":\"Inner\","
      + "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"},"
      + "{\"name\":\"f2\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

  private static final String MIDDLE_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Middle\","
      + "\"fields\":[{\"name\":\"m1\",\"type\":\"string\"}]}";

  private static final String ROOT_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Root\","
      + "\"fields\":[{\"name\":\"r1\",\"type\":\"string\"}]}";

  @Test
  public void testConflictingReferenceVersionsRejected() throws Exception {
    // Register inner schema with two versions
    restApp.restClient.registerSchema(INNER_SCHEMA_V1, "inner");
    restApp.restClient.registerSchema(INNER_SCHEMA_V2, "inner");

    // Register middle schema referencing inner v2
    restApp.restClient.registerSchema(
        MIDDLE_SCHEMA, "AVRO",
        Collections.singletonList(new SchemaReference("Inner", "inner", 2)),
        "middle");

    // Register root referencing inner v1 directly AND middle (which refs inner v2)
    try {
      restApp.restClient.registerSchema(
          ROOT_SCHEMA, "AVRO",
          Arrays.asList(
              new SchemaReference("Inner", "inner", 1),
              new SchemaReference("Middle", "middle", 1)),
          "root");
      fail("Should reject schema with conflicting reference versions");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
      assertTrue(rce.getMessage().contains("Conflicting reference versions"));
    }
  }

  @Test
  public void testConsistentReferenceVersionsAllowed() throws Exception {
    // Register inner schema
    restApp.restClient.registerSchema(INNER_SCHEMA_V1, "innerOk");

    // Register middle schema referencing inner v1
    restApp.restClient.registerSchema(
        MIDDLE_SCHEMA, "AVRO",
        Collections.singletonList(new SchemaReference("InnerOk", "innerOk", 1)),
        "middleOk");

    // Root references inner v1 directly AND middle (which also refs inner v1) — no clash
    restApp.restClient.registerSchema(
        ROOT_SCHEMA, "AVRO",
        Arrays.asList(
            new SchemaReference("InnerOk", "innerOk", 1),
            new SchemaReference("MiddleOk", "middleOk", 1)),
        "rootOk");
  }
}
