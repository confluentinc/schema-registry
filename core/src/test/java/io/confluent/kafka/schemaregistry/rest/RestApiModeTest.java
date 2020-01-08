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

import org.junit.Test;

import java.util.Collections;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestIncompatibleAvroSchemaException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidSchemaException;
import io.confluent.rest.exceptions.RestConstraintViolationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RestApiModeTest extends ClusterTestHarness {

  private static String SCHEMA_STRING = AvroUtils.parseSchema(
      "{\"type\":\"record\","
          + "\"name\":\"myrecord\","
          + "\"fields\":"
          + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
      .canonicalString();

  public RestApiModeTest() {
    super(1, true, CompatibilityLevel.BACKWARD.name);
  }

  @Test
  public void testReadOnlyMode() throws Exception {
    String subject = "testSubject";
    String mode = "READONLY";

    // set mode to read only
    assertEquals(
        mode,
        restApp.restClient.setMode(mode).getMode());

    // register a valid avro schema
    try {
      restApp.restClient.registerSchema(SCHEMA_STRING, subject);
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
        restApp.restClient.setMode(mode).getMode());

    // register a valid avro schema
    try {
      restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, 1);
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
        restApp.restClient.registerSchema(SCHEMA_STRING, subject));
  }

  @Test
  public void testImportMode() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp.restClient.setMode(mode).getMode());

    // register a valid avro schema
    int expectedIdSchema1 = 1;
    assertEquals("Registering with id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1));
  }

  @Test
  public void testImportModeWithoutId() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp.restClient.setMode(mode).getMode());

    // register a schema without id
    try {
      restApp.restClient.registerSchema(SCHEMA_STRING, subject);
      fail("Registering a schema without ID should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Should get a constraint violation",
          RestConstraintViolationException.DEFAULT_ERROR_CODE,
          e.getStatus());
    }
  }

  @Test
  public void testInvalidImportMode() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // register a valid avro schema
    int expectedIdSchema1 = 1;
    assertEquals("Registering without id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject));

    try {
      restApp.restClient.setMode(mode).getMode();
      fail("Setting import mode should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Should get a constraint violation",
          RestConstraintViolationException.DEFAULT_ERROR_CODE,
          e.getStatus());
    }
  }

  @Test
  public void testRegisterSchemaWithDifferentIdAfterImport() throws Exception {
    String subject = "testSubject";
    String mode = "READWRITE";

    // set mode to read write
    assertEquals(
        mode,
        restApp.restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 1;
    assertEquals("Registering without id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject));

    // delete subject so we can switch to import mode
    restApp.restClient.deleteSubject(Collections.emptyMap(), subject);

    mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp.restClient.setMode(mode).getMode());

    // register same schema with different id
    expectedIdSchema1 = 2;
    assertEquals("Registering with id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1));
  }
}
