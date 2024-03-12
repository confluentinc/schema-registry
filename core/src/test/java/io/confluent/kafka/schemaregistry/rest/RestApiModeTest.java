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

  private static String SCHEMA2_STRING = AvroUtils.parseSchema(
          "{\"type\":\"record\","
              + "\"name\":\"myrecord\","
              + "\"fields\":"
              + "[{\"type\":\"int\",\"name\":\"f1\"}]}")
      .canonicalString();

  private static String SCHEMA_WITH_DECIMAL = AvroUtils.parseSchema(
          "{\n"
              + "    \"type\": \"record\",\n"
              + "    \"name\": \"MyRecord\",\n"
              + "    \"fields\": [\n"
              + "        {\n"
              + "            \"name\": \"field1\",\n"
              + "            \"type\": [\n"
              + "                \"null\",\n"
              + "                {\n"
              + "                    \"type\": \"bytes\",\n"
              + "                    \"scale\": 4,\n"
              + "                    \"precision\": 17,\n"
              + "                    \"logicalType\": \"decimal\"\n"
              + "                }\n"
              + "            ],\n"
              + "            \"default\": null\n"
              + "        }\n"
              + "    ]\n"
              + "}")
      .canonicalString();

  private static String SCHEMA_WITH_DECIMAL2 = AvroUtils.parseSchema(
          "{\n"
              + "    \"type\": \"record\",\n"
              + "    \"name\": \"MyRecord\",\n"
              + "    \"fields\": [\n"
              + "        {\n"
              + "            \"name\": \"field1\",\n"
              + "            \"type\": [\n"
              + "                \"null\",\n"
              + "                {\n"
              + "                    \"type\": \"bytes\",\n"
              + "                    \"logicalType\": \"decimal\",\n"
              + "                    \"precision\": 17,\n"
              + "                    \"scale\": 4\n"
              + "                }\n"
              + "            ],\n"
              + "            \"default\": null\n"
              + "        }\n"
              + "    ]\n"
              + "}")
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

    // set subject mode to import with force=true
    assertEquals(
        mode,
        restApp.restClient.setMode(mode, subject, true).getMode());

    // set global mode to import with force=true
    assertEquals(
        mode,
        restApp.restClient.setMode(mode, null, true).getMode());
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

  @Test
  public void testRegisterSchemaWithSameIdAfterImport() throws Exception {
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

    // register same schema with same id
    expectedIdSchema1 = 1;
    assertEquals("Registering with id should succeed",
            expectedIdSchema1,
            restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1));

    // delete subject again
    restApp.restClient.deleteSubject(Collections.emptyMap(), subject);

    // register same schema with same id
    expectedIdSchema1 = 1;
    assertEquals("Registering with id should succeed",
            expectedIdSchema1,
            restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1));

    assertEquals("Getting schema by id should succeed",
            SCHEMA_STRING,
            restApp.restClient.getVersion(subject, 1).getSchema());
  }

  @Test
  public void testRegisterSchemaWithSameIdButWithMetadataAfterImport() throws Exception {
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

    // register same schema with same id but with metadata
    expectedIdSchema1 = 1;
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(SCHEMA_STRING);
    request.setMetadata(new Metadata(null, ImmutableMap.of("foo", "bar"), null));
    request.setVersion(1);
    request.setId(expectedIdSchema1);
    assertEquals("Registering with id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request, subject, false).getId());

    assertEquals("Getting schema by id should succeed",
        SCHEMA_STRING,
        restApp.restClient.getVersion(subject, 1).getSchema());

    // delete subject again
    restApp.restClient.deleteSubject(Collections.emptyMap(), subject);

    // register same schema with same id but with metadata
    expectedIdSchema1 = 1;
    request = new RegisterSchemaRequest();
    request.setSchema(SCHEMA_STRING);
    request.setMetadata(new Metadata(null, ImmutableMap.of("foo", "bar"), null));
    request.setVersion(1);
    request.setId(expectedIdSchema1);
    assertEquals("Registering with id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request, subject, false).getId());

    assertEquals("Getting schema by id should succeed",
        SCHEMA_STRING,
        restApp.restClient.getVersion(subject, 1).getSchema());
  }

  @Test
  public void testImportModeWithEquivalentSchemaDifferentId() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp.restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 100;
    assertEquals("Registering with id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 1, expectedIdSchema1));

    assertEquals("Getting schema by id should succeed",
        SCHEMA_WITH_DECIMAL,
        restApp.restClient.getVersion(subject, 1).getSchema());

    // register equivalent schema with different id
    expectedIdSchema1 = 200;
    assertEquals("Registering with id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_WITH_DECIMAL2, subject, 2, expectedIdSchema1));

    assertEquals("Getting schema by id should succeed",
        SCHEMA_WITH_DECIMAL2,
        restApp.restClient.getVersion(subject, 2).getSchema());
  }

  @Test
  public void testImportModeWithSameSchemaDifferentId() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp.restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 100;
    assertEquals("Registering with id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 1, expectedIdSchema1));

    assertEquals("Getting schema by id should succeed",
        SCHEMA_WITH_DECIMAL,
        restApp.restClient.getVersion(subject, 1).getSchema());

    // register equivalent schema with different id
    expectedIdSchema1 = 200;
    assertEquals("Registering with id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 2, expectedIdSchema1));

    assertEquals("Getting schema by id should succeed",
        SCHEMA_WITH_DECIMAL,
        restApp.restClient.getVersion(subject, 2).getSchema());
  }

  @Test
  public void testRegisterIncompatibleSchemaDuringImport() throws Exception {
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

    // register same schema with same id
    expectedIdSchema1 = 1;
    assertEquals("Registering with id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1));

    // register same schema with same id
    expectedIdSchema1 = 2;
    assertEquals("Registering with id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA2_STRING, subject, 2, expectedIdSchema1));

    assertEquals("Getting schema by id should succeed",
        SCHEMA2_STRING,
        restApp.restClient.getVersion(subject, 2).getSchema());
  }
}
