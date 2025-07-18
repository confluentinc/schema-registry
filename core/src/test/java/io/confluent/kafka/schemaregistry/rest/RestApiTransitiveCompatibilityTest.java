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
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestIncompatibleSchemaException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class RestApiTransitiveCompatibilityTest extends ClusterTestHarness {

  String baseSchema = AvroUtils.parseSchema("{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
  
  String baseSchemaWithColumnWithDefault = AvroUtils.parseSchema("{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"},"
      + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}").canonicalString();
  
  String baseSchemaWithColumnNoDefault = AvroUtils.parseSchema("{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"},"
      + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString();
  
  
  public RestApiTransitiveCompatibilityTest() {
    super(1, true, CompatibilityLevel.BACKWARD_TRANSITIVE.name);
  }

  /* Confirm that removing a default in from a column that was added earlier is not compatible. */
  @Test
  public void testCompatibility() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(baseSchema, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(baseSchemaWithColumnWithDefault, subject),
        "Registering a compatible schema should succeed"
    );

    // register an incompatible avro
    String incompatibleSchemaString = baseSchemaWithColumnNoDefault;
    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
      fail("Registering an incompatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
    }
  }
  
  /* Confirm that removing a default in isolation is compatible. */
  @Test
  public void validateTransitiveEffect() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(baseSchemaWithColumnWithDefault, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(baseSchemaWithColumnNoDefault, subject),
        "Registering a compatible schema should succeed"
    );
  }
}
