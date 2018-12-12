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

import org.junit.Test;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestIncompatibleAvroSchemaException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidSchemaException;
import io.confluent.rest.exceptions.RestConstraintViolationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RestApiModeTest extends ClusterTestHarness {

  public RestApiModeTest() {
    super(1, true, AvroCompatibilityLevel.BACKWARD.name);
  }

  @Test
  public void testReadOnlyMode() throws Exception {
    String subject = "testSubject";
    String mode = "READONLY";

    // set mode to read only
    assertEquals(
        mode,
        restApp.restClient.setMode(mode, subject, false).getMode());

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
        .canonicalString;
    try {
      restApp.restClient.registerSchema(schemaString1, subject);
      fail("Registering during read-only mode should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Should get a constraint violation",
          RestConstraintViolationException.DEFAULT_ERROR_CODE,
          e.getStatus());
    }
  }

  @Test
  public void testReadWriteMode() throws Exception {
    String subject = "testSubject";
    String mode = "READWRITE";

    // set mode to read write
    assertEquals(
        mode,
        restApp.restClient.setMode(mode, subject, false).getMode());

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
        .canonicalString;
    try {
      restApp.restClient.registerSchema(schemaString1, subject, 1);
      fail("Registering an incompatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Should get a constraint violation",
          RestConstraintViolationException.DEFAULT_ERROR_CODE,
          e.getStatus());
    }

    int expectedIdSchema1 = 1;
    assertEquals("Registering without id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject));
  }

  @Test
  public void testImportMode() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp.restClient.setMode(mode, subject, false).getMode());

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
        .canonicalString;
    int expectedIdSchema1 = 1;
    assertEquals("Registering without id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject, expectedIdSchema1));
  }

  @Test
  public void testInvalidImportMode() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
        .canonicalString;
    int expectedIdSchema1 = 1;
    assertEquals("Registering without id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject));

    try {
      restApp.restClient.setMode(mode, subject, false).getMode();
      fail("Setting import mode should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Should get a constraint violation",
          RestConstraintViolationException.DEFAULT_ERROR_CODE,
          e.getStatus());
    }
  }
}
