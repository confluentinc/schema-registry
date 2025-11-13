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

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Mode;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;

import java.util.Collections;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.SchemaRegistryTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import org.junit.jupiter.api.Test;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Abstract base class for REST API mode integration tests.
 * Concrete subclasses provide the specific test harness implementation.
 */
public abstract class AbstractRestApiModeTest {

  protected abstract SchemaRegistryTestHarness getHarness();
  protected abstract Properties getSchemaRegistryProperties() throws Exception;
  
  protected RestApp restApp() {
    return getHarness().getRestApp();
  }

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

  @Test
  public void testReadOnlyMode() throws Exception {
    String subject = "testSubject";
    String mode = "READONLY";

    // set mode to read only
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    // register a valid avro schema
    try {
      restApp().restClient.registerSchema(SCHEMA_STRING, subject);
      fail("Registering during read-only mode should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestConstraintViolationException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a constraint violation"
      );
    }
  }

  @Test
  public void testReadWriteMode() throws Exception {
    String subject = "testSubject";
    String mode = "READWRITE";

    // set mode to read write
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    // register a valid avro schema
    try {
      restApp().restClient.registerSchema(SCHEMA_STRING, subject, 1, 1);
      fail("Registering an incompatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestConstraintViolationException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a constraint violation"
      );
    }

    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );
  }

  @Test
  public void testImportMode() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    // register a valid avro schema
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );
  }

  @Test
  public void testImportModeWithoutId() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    // register a schema without id
    try {
      restApp().restClient.registerSchema(SCHEMA_STRING, subject);
      fail("Registering a schema without ID should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestConstraintViolationException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a constraint violation"
      );
    }
  }

  @Test
  public void testClearMode() throws Exception {
    String mode = "READONLY";

    // set mode to read only
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    // clear mode
    assertEquals(
        null,
        restApp().restClient.setMode(null).getMode());

    // get mode
    assertEquals(
        "READWRITE",
        restApp().restClient.getMode().getMode());
  }

  @Test
  public void testInvalidImportMode() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // register a valid avro schema
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );

    try {
      restApp().restClient.setMode(mode).getMode();
      fail("Setting import mode should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestConstraintViolationException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a constraint violation"
      );
    }

    // set subject mode to import with force=true
    assertEquals(
        mode,
        restApp().restClient.setMode(mode, subject, true).getMode());

    // set global mode to import with force=true
    assertEquals(
        mode,
        restApp().restClient.setMode(mode, null, true).getMode());
  }

  @Test
  public void testRegisterSchemaWithNoIdAfterImport() throws Exception {
    // Represents a production use case where auto-registering clients
    // do not fail when SR is in import mode and the schema already exists
    String subject = "testSubject";
    String mode = "READWRITE";

    // set mode to read write
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    // register same schema with no id
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );
  }

  @Test
  public void testRegisterSchemaWithDifferentIdAfterImport() throws Exception {
    String subject = "testSubject";
    String mode = "READWRITE";

    // set mode to read write
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );

    // delete subject so we can switch to import mode
    restApp().restClient.deleteSubject(Collections.emptyMap(), subject);

    mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    // register same schema with different id
    expectedIdSchema1 = 2;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );
  }

  @Test
  public void testRegisterSchemaWithSameIdAfterImport() throws Exception {
    String subject = "testSubject";
    String mode = "READWRITE";

    // set mode to read write
    assertEquals(
            mode,
            restApp().restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );

    // delete subject so we can switch to import mode
    restApp().restClient.deleteSubject(Collections.emptyMap(), subject);

    mode = "IMPORT";

    // set mode to import
    assertEquals(
            mode,
            restApp().restClient.setMode(mode).getMode());

    // register same schema with same id
    expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    // delete subject again
    restApp().restClient.deleteSubject(Collections.emptyMap(), subject);

    // register same schema with same id
    expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_STRING,
        restApp().restClient.getVersion(subject, 1).getSchema(),
        "Getting schema by id should succeed"
    );
  }

  @Test
  public void testRegisterSchemaWithSameIdButWithMetadataAfterImport() throws Exception {
    String subject = "testSubject";
    String mode = "READWRITE";

    // set mode to read write
    assertEquals(
            mode,
            restApp().restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );

    // delete subject so we can switch to import mode
    restApp().restClient.deleteSubject(Collections.emptyMap(), subject);

    mode = "IMPORT";

    // set mode to import
    assertEquals(
            mode,
            restApp().restClient.setMode(mode).getMode());

    // register same schema with same id but with metadata
    expectedIdSchema1 = 1;
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(SCHEMA_STRING);
    request.setMetadata(new Metadata(null, ImmutableMap.of("foo", "bar"), null));
    request.setVersion(1);
    request.setId(expectedIdSchema1);
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(request, subject, false).getId(),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_STRING,
        restApp().restClient.getVersion(subject, 1).getSchema(),
        "Getting schema by id should succeed"
    );

    // delete subject again
    restApp().restClient.deleteSubject(Collections.emptyMap(), subject);

    // register same schema with same id but with metadata
    expectedIdSchema1 = 1;
    request = new RegisterSchemaRequest();
    request.setSchema(SCHEMA_STRING);
    request.setMetadata(new Metadata(null, ImmutableMap.of("foo", "bar"), null));
    request.setVersion(1);
    request.setId(expectedIdSchema1);
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(request, subject, false).getId(),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_STRING,
        restApp().restClient.getVersion(subject, 1).getSchema(),
        "Getting schema by id should succeed"
    );
  }

  @Test
  public void testImportModeWithEquivalentSchemaDifferentId() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 100;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_WITH_DECIMAL,
        restApp().restClient.getVersion(subject, 1).getSchema(),
        "Getting schema by id should succeed"
    );

    // register equivalent schema with different id
    expectedIdSchema1 = 200;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_WITH_DECIMAL2, subject, 2, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_WITH_DECIMAL2,
        restApp().restClient.getVersion(subject, 2).getSchema(),
        "Getting schema by id should succeed"
    );
  }

  @Test
  public void testImportModeWithSameSchemaDifferentId() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 100;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_WITH_DECIMAL,
        restApp().restClient.getVersion(subject, 1).getSchema(),
        "Getting schema by id should succeed"
    );

    // register equivalent schema with different id
    expectedIdSchema1 = 200;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 2, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_WITH_DECIMAL,
        restApp().restClient.getVersion(subject, 2).getSchema(),
        "Getting schema by id should succeed"
    );
  }

  @Test
  public void testImportModeWithSameSchemaDifferentIdAndSubject() throws Exception {
    String subject = "testSubject";
    String subject2 = "testSubject2";
    String mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 100;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 1, expectedIdSchema1));

    assertEquals(
        SCHEMA_WITH_DECIMAL,
        restApp().restClient.getVersion(subject, 1).getSchema());

    int versionOfRegisteredSubject1 =
        restApp().restClient.lookUpSubjectVersion(SCHEMA_WITH_DECIMAL, subject).getVersion();
    assertEquals(1, versionOfRegisteredSubject1);

    // register equivalent schema with different id
    expectedIdSchema1 = 200;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject2, 1, expectedIdSchema1));

    assertEquals(
        SCHEMA_WITH_DECIMAL,
        restApp().restClient.getVersion(subject2, 1).getSchema());

    versionOfRegisteredSubject1 =
        restApp().restClient.lookUpSubjectVersion(SCHEMA_WITH_DECIMAL, subject).getVersion();
    assertEquals(1, versionOfRegisteredSubject1);

    int versionOfRegisteredSubject2 =
        restApp().restClient.lookUpSubjectVersion(SCHEMA_WITH_DECIMAL, subject2).getVersion();
    assertEquals(1, versionOfRegisteredSubject2);
  }

  @Test
  public void testRegisterIncompatibleSchemaDuringImport() throws Exception {
    String subject = "testSubject";
    String mode = "READWRITE";

    // set mode to read write
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );

    // delete subject so we can switch to import mode
    restApp().restClient.deleteSubject(Collections.emptyMap(), subject);

    mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp().restClient.setMode(mode).getMode());

    // register same schema with same id
    expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    // register same schema with same id
    expectedIdSchema1 = 2;
    assertEquals(
        expectedIdSchema1,
        restApp().restClient.registerSchema(SCHEMA2_STRING, subject, 2, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA2_STRING,
        restApp().restClient.getVersion(subject, 2).getSchema(),
        "Getting schema by id should succeed"
    );
  }

  @Test
  public void testGlobalContextWithReadOnlyMode() throws Exception {
    String subject = "testSubject";
    String mode = "READONLY";

    // set mode in global context to read only
    assertEquals(
        mode,
        restApp().restClient.setMode(mode, ":.__GLOBAL:").getMode());

    Mode mode1 = restApp().restClient.getMode(null, true);
    assertEquals("readonly", mode1.getMode().toLowerCase());

    // register a valid avro schema
    try {
      restApp().restClient.registerSchema(SCHEMA_STRING, subject);
      fail("Registering during read-only mode should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestConstraintViolationException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a constraint violation"
      );
    }
  }

  @Test
  public void testDeleteGlobalMode() throws Exception {
    String mode = "READONLY";

    // set global mode to read only
    assertEquals(
            mode,
            restApp().restClient.setMode(mode).getMode());

    // verify mode is set
    assertEquals(
            mode,
            restApp().restClient.getMode().getMode());

    // delete global mode - should succeed and return the old mode
    Mode deletedMode = restApp().restClient.deleteSubjectMode(null);
    assertEquals(
            mode,
            deletedMode.getMode(),
            "Deleted mode should return the old global mode");

    // verify global mode is now reset to default (READWRITE)
    assertEquals(
            "READWRITE",
            restApp().restClient.getMode().getMode(),
            "Global mode should revert to default READWRITE");
  }

  @Test
  public void testDeleteSubjectModeAfterGlobalMode() throws Exception {
    String subject = "testSubject";
    String globalMode = "READONLY";
    String subjectMode = "READWRITE";

    // set global mode to read only
    assertEquals(
            globalMode,
            restApp().restClient.setMode(globalMode).getMode());

    // set subject mode to read write
    assertEquals(
            subjectMode,
            restApp().restClient.setMode(subjectMode, subject).getMode());

    // verify subject mode is set
    assertEquals(
            subjectMode,
            restApp().restClient.getMode(subject, false).getMode());

    // delete subject mode
    Mode deletedMode = restApp().restClient.deleteSubjectMode(subject);
    assertEquals(
            subjectMode,
            deletedMode.getMode(),
            "Deleted mode should return the old mode");

    // verify subject mode falls back to global mode
    assertEquals(
            globalMode,
            restApp().restClient.getMode(subject, true).getMode());
  }

  @Test
  public void testRecursiveDeleteContextMode() throws Exception {
    String context = ":.mycontext:";
    String subject1 = ":.mycontext:subject1";
    String subject2 = ":.mycontext:subject2";
    String subject3 = ":.mycontext:subject3";
    String mode = "READONLY";

    // set mode for context
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, context).getMode());

    // set mode for subjects under the context
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject1).getMode());
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject2).getMode());
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject3).getMode());

    // verify modes are set
    assertEquals(
            mode,
            restApp().restClient.getMode(context, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject1, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject2, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject3, false).getMode());

    // recursive delete context mode
    Mode deletedMode = restApp().restClient.deleteSubjectMode(Collections.emptyMap(), context, true);
    assertEquals(
            mode,
            deletedMode.getMode(),
            "Deleted mode should return the old context mode");

    // verify context mode is deleted
    try {
      restApp().restClient.getMode(context, false);
      fail("Should throw exception for deleted context mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    // verify all subject modes under the context are also deleted
    try {
      restApp().restClient.getMode(subject1, false);
      fail("Should throw exception for deleted subject1 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    try {
      restApp().restClient.getMode(subject2, false);
      fail("Should throw exception for deleted subject2 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    try {
      restApp().restClient.getMode(subject3, false);
      fail("Should throw exception for deleted subject3 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }
  }

  @Test
  public void testNonRecursiveDeleteContextModeDoesNotAffectSubjects() throws Exception {
    String context = ":.mycontext:";
    String subject1 = ":.mycontext:subject1";
    String subject2 = ":.mycontext:subject2";
    String mode = "READONLY";

    // set mode for context
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, context).getMode());

    // set mode for subjects under the context
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject1).getMode());
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject2).getMode());

    // verify modes are set
    assertEquals(
            mode,
            restApp().restClient.getMode(context, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject1, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject2, false).getMode());

    // non-recursive delete context mode (recursive = false)
    Mode deletedMode = restApp().restClient.deleteSubjectMode(Collections.emptyMap(), context, false);
    assertEquals(
            mode,
            deletedMode.getMode(),
            "Deleted mode should return the old context mode");

    // verify context mode is deleted
    try {
      restApp().restClient.getMode(context, false);
      fail("Should throw exception for deleted context mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    // verify subject modes are NOT deleted (still exist)
    assertEquals(
            mode,
            restApp().restClient.getMode(subject1, false).getMode(),
            "Subject1 mode should still exist");
    assertEquals(
            mode,
            restApp().restClient.getMode(subject2, false).getMode(),
            "Subject2 mode should still exist");
  }

  @Test
  public void testRecursiveDeleteGlobalMode() throws Exception {
    String subject1 = "subject1";
    String subject2 = "subject2";
    String subject3 = "subject3";
    String mode = "READONLY";

    // set global mode
    assertEquals(
            mode,
            restApp().restClient.setMode(mode).getMode());

    // set mode for subjects in default context
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject1).getMode());
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject2).getMode());
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject3).getMode());

    // verify modes are set
    assertEquals(
            mode,
            restApp().restClient.getMode(null, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject1, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject2, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject3, false).getMode());

    // recursive delete global mode
    Mode deletedMode = restApp().restClient.deleteSubjectMode(Collections.emptyMap(), null, true);
    assertEquals(
            mode,
            deletedMode.getMode(),
            "Deleted mode should return the old global mode");

    // verify global mode is deleted (reverts to default)
    assertEquals(
            "READWRITE",
            restApp().restClient.getMode(null, false).getMode(),
            "Global mode should revert to default READWRITE");

    // verify all subject modes in default context are also deleted
    try {
      restApp().restClient.getMode(subject1, false);
      fail("Should throw exception for deleted subject1 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    try {
      restApp().restClient.getMode(subject2, false);
      fail("Should throw exception for deleted subject2 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    try {
      restApp().restClient.getMode(subject3, false);
      fail("Should throw exception for deleted subject3 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }
  }

  @Test
  public void testRecursiveDeleteContextModeWhenContextModeNotSet() throws Exception {
    String context = ":.mycontext:";
    String subject1 = ":.mycontext:subject1";
    String subject2 = ":.mycontext:subject2";
    String mode = "READONLY";

    // set mode for subjects under the context (but NOT for the context itself)
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject1).getMode());
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject2).getMode());

    // verify subject modes are set
    assertEquals(
            mode,
            restApp().restClient.getMode(subject1, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject2, false).getMode());

    // verify context mode is not set
    try {
      restApp().restClient.getMode(context, false);
      fail("Context mode should not be set");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    // recursive delete context mode (should succeed even though context mode is not set)
    // and should delete modes for subjects under the context
    Mode deletedMode = restApp().restClient.deleteSubjectMode(Collections.emptyMap(), context, true);
    assertEquals(
            null,
            deletedMode.getMode(),
            "Deleted mode should return null as context mode was not set");

    // verify subject modes are deleted
    try {
      restApp().restClient.getMode(subject1, false);
      fail("Should throw exception for deleted subject1 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    try {
      restApp().restClient.getMode(subject2, false);
      fail("Should throw exception for deleted subject2 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }
  }

  @Test
  public void testRecursiveDeleteMixedContextAndSubjects() throws Exception {
    String context = ":.mycontext:";
    String subject1 = ":.mycontext:subject1";
    String subject2 = ":.mycontext:subject2";
    String subject3 = ":.mycontext:subject3";
    String contextMode = "IMPORT";
    String subject1Mode = "READONLY";
    String subject2Mode = "READWRITE";
    // subject3 has no mode set

    // set mode for context
    assertEquals(
            contextMode,
            restApp().restClient.setMode(contextMode, context).getMode());

    // set different modes for subjects
    assertEquals(
            subject1Mode,
            restApp().restClient.setMode(subject1Mode, subject1).getMode());
    assertEquals(
            subject2Mode,
            restApp().restClient.setMode(subject2Mode, subject2).getMode());
    // subject3 has no explicit mode

    // verify modes are set
    assertEquals(
            contextMode,
            restApp().restClient.getMode(context, false).getMode());
    assertEquals(
            subject1Mode,
            restApp().restClient.getMode(subject1, false).getMode());
    assertEquals(
            subject2Mode,
            restApp().restClient.getMode(subject2, false).getMode());

    // recursive delete context mode
    Mode deletedMode = restApp().restClient.deleteSubjectMode(Collections.emptyMap(), context, true);
    assertEquals(
            contextMode,
            deletedMode.getMode(),
            "Deleted mode should return the old context mode");

    // verify all modes are deleted
    try {
      restApp().restClient.getMode(context, false);
      fail("Should throw exception for deleted context mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    try {
      restApp().restClient.getMode(subject1, false);
      fail("Should throw exception for deleted subject1 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    try {
      restApp().restClient.getMode(subject2, false);
      fail("Should throw exception for deleted subject2 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    // subject3 still has no mode (should not be affected)
    try {
      restApp().restClient.getMode(subject3, false);
      fail("Subject3 should still have no mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }
  }

  @Test
  public void testRecursiveDeleteDoesNotAffectOtherContexts() throws Exception {
    String context1 = ":.context1:";
    String context2 = ":.context2:";
    String subject1 = ":.context1:subject1";
    String subject2 = ":.context2:subject2";
    String mode = "READONLY";

    // set mode for both contexts and subjects
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, context1).getMode());
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, context2).getMode());
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject1).getMode());
    assertEquals(
            mode,
            restApp().restClient.setMode(mode, subject2).getMode());

    // verify modes are set
    assertEquals(
            mode,
            restApp().restClient.getMode(context1, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(context2, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject1, false).getMode());
    assertEquals(
            mode,
            restApp().restClient.getMode(subject2, false).getMode());

    // recursive delete context1 mode
    Mode deletedMode = restApp().restClient.deleteSubjectMode(Collections.emptyMap(), context1, true);
    assertEquals(
            mode,
            deletedMode.getMode(),
            "Deleted mode should return the old context1 mode");

    // verify context1 and subject1 modes are deleted
    try {
      restApp().restClient.getMode(context1, false);
      fail("Should throw exception for deleted context1 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    try {
      restApp().restClient.getMode(subject1, false);
      fail("Should throw exception for deleted subject1 mode");
    } catch (RestClientException e) {
      assertEquals(40409, e.getErrorCode(),
              "Should get mode not configured error");
    }

    // verify context2 and subject2 modes are NOT affected
    assertEquals(
            mode,
            restApp().restClient.getMode(context2, false).getMode(),
            "Context2 mode should still exist");
    assertEquals(
            mode,
            restApp().restClient.getMode(subject2, false).getMode(),
            "Subject2 mode should still exist");
  }
}
