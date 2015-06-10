/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestIncompatibleAvroSchemaException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidSchemaException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RestApiCompatibilityTest extends ClusterTestHarness {

  public RestApiCompatibilityTest() {
    super(1, true, AvroCompatibilityLevel.BACKWARD.name);
  }

  @Test
  public void testCompatibility() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
        .canonicalString;
    int expectedIdSchema1 = 1;
    assertEquals("Registering should succeed",
            expectedIdSchema1,
            restApp.restClient.registerSchema(schemaString1, subject));

    // register an incompatible avro
    String incompatibleSchemaString = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}"
    ).canonicalString;
    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
      fail("Registering an incompatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Should get a conflict status",
                   RestIncompatibleAvroSchemaException.DEFAULT_ERROR_CODE,
                   e.getStatus());
    }

    // register a non-avro
    String nonAvroSchemaString = "non-avro schema string";
    try {
      restApp.restClient.registerSchema(nonAvroSchemaString, subject);
      fail("Registering a non-avro schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Should get a bad request status",
                   RestInvalidSchemaException.ERROR_CODE,
                   e.getErrorCode());
    }

    // register a backward compatible avro
    String schemaString2 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}"
    ).canonicalString;
    int expectedIdSchema2 = 2;
    assertEquals("Registering a compatible schema should succeed",
                 expectedIdSchema2,
                 restApp.restClient.registerSchema(schemaString2, subject));
  }

  @Test
  public void testCompatibilityLevelChangeToNone() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
        .canonicalString;
    int expectedIdSchema1 = 1;
    assertEquals("Registering should succeed",
            expectedIdSchema1,
            restApp.restClient.registerSchema(schemaString1, subject));

    // register an incompatible avro
    String incompatibleSchemaString = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}"
    ).canonicalString;
    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
      fail("Registering an incompatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Should get a conflict status",
                   RestIncompatibleAvroSchemaException.DEFAULT_ERROR_CODE,
                   e.getStatus());
    }

    // change compatibility level to none and try again
    assertEquals("Changing compatibility level should succeed",
            AvroCompatibilityLevel.NONE.name,
            restApp.restClient
                    .updateCompatibility(AvroCompatibilityLevel.NONE.name, null)
                    .getCompatibilityLevel());

    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
    } catch (RestClientException e) {
      fail("Registering an incompatible schema should succeed after bumping down the compatibility "
           + "level to none");
    }
  }

  @Test
  public void testCompatibilityLevelChangeToBackward() throws Exception {
    String subject = "testSubject";

    String schemaString1 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString;
    int expectedIdSchema1 = 1;
    assertEquals("Registering should succeed",
            expectedIdSchema1,
            restApp.restClient.registerSchema(schemaString1, subject));
    // verify that default compatibility level is backward
    assertEquals("Default compatibility level should be backward",
            AvroCompatibilityLevel.BACKWARD.name,
            restApp.restClient.getConfig(null).getCompatibilityLevel());
    // change it to forward
    assertEquals("Changing compatibility level should succeed",
            AvroCompatibilityLevel.FORWARD.name,
            restApp.restClient
                    .updateCompatibility(AvroCompatibilityLevel.FORWARD.name, null)
                    .getCompatibilityLevel());

    // verify that new compatibility level is forward
    assertEquals("New compatibility level should be forward",
            AvroCompatibilityLevel.FORWARD.name,
            restApp.restClient.getConfig(null).getCompatibilityLevel());

    // register schema that is forward compatible with schemaString1
    String schemaString2 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString;
    int expectedIdSchema2 = 2;
    assertEquals("Registering should succeed",
                 expectedIdSchema2,
                 restApp.restClient.registerSchema(schemaString2, subject));

    // change compatibility to backward
    assertEquals("Changing compatibility level should succeed",
            AvroCompatibilityLevel.BACKWARD.name,
            restApp.restClient.updateCompatibility(AvroCompatibilityLevel.BACKWARD.name,
                    null).getCompatibilityLevel());

    // verify that new compatibility level is backward
    assertEquals("Updated compatibility level should be backward",
            AvroCompatibilityLevel.BACKWARD.name,
            restApp.restClient.getConfig(null).getCompatibilityLevel());

            // register forward compatible schema, which should fail
            String schemaString3 = AvroUtils.parseSchema(
            "{\"type\":\"record\","
                    + "\"name\":\"myrecord\","
                    + "\"fields\":"
                    + "[{\"type\":\"string\",\"name\":\"f1\"},"
                    + " {\"type\":\"string\",\"name\":\"f2\"},"
                    + " {\"type\":\"string\",\"name\":\"f3\"}]}").canonicalString;
    try {
      restApp.restClient.registerSchema(schemaString3, subject);
      fail("Registering a forward compatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Should get a conflict status",
                   RestIncompatibleAvroSchemaException.DEFAULT_ERROR_CODE,
                   e.getStatus());
    }

    // now try registering a backward compatible schema (add a field with a default)
    String schemaString4 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\", \"default\": \"foo\"}]}").canonicalString;
    int expectedIdSchema4 = 3;
    assertEquals("Registering should succeed with backwards compatible schema",
            expectedIdSchema4,
            restApp.restClient.registerSchema(schemaString4, subject));
  }
}
