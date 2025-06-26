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
import java.util.Properties;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidModeException;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.Mode;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import jakarta.ws.rs.core.Configurable;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.Callback;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

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
        restApp.restClient.setMode(mode).getMode());

    // register a valid avro schema
    try {
      restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, 1);
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
        restApp.restClient.registerSchema(SCHEMA_STRING, subject),
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
        restApp.restClient.setMode(mode).getMode());

    // register a valid avro schema
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
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
        restApp.restClient.setMode(mode).getMode());

    // register a schema without id
    try {
      restApp.restClient.registerSchema(SCHEMA_STRING, subject);
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
        restApp.restClient.setMode(mode).getMode());

    // clear mode
    assertEquals(
        null,
        restApp.restClient.setMode(null).getMode());

    // get mode
    assertEquals(
        "READWRITE",
        restApp.restClient.getMode().getMode());
  }

  @Test
  public void testInvalidImportMode() throws Exception {
    String subject = "testSubject";
    String mode = "IMPORT";

    // register a valid avro schema
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );

    try {
      restApp.restClient.setMode(mode).getMode();
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
        restApp.restClient.setMode(mode, subject, true).getMode());

    // set global mode to import with force=true
    assertEquals(
        mode,
        restApp.restClient.setMode(mode, null, true).getMode());
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
        restApp.restClient.setMode(mode).getMode());

    mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp.restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    // register same schema with no id
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject),
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
        restApp.restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );

    // delete subject so we can switch to import mode
    restApp.restClient.deleteSubject(Collections.emptyMap(), subject);

    mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp.restClient.setMode(mode).getMode());

    // register same schema with different id
    expectedIdSchema1 = 2;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
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
            restApp.restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );

    // delete subject so we can switch to import mode
    restApp.restClient.deleteSubject(Collections.emptyMap(), subject);

    mode = "IMPORT";

    // set mode to import
    assertEquals(
            mode,
            restApp.restClient.setMode(mode).getMode());

    // register same schema with same id
    expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    // delete subject again
    restApp.restClient.deleteSubject(Collections.emptyMap(), subject);

    // register same schema with same id
    expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_STRING,
        restApp.restClient.getVersion(subject, 1).getSchema(),
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
            restApp.restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );

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
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request, subject, false).getId(),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_STRING,
        restApp.restClient.getVersion(subject, 1).getSchema(),
        "Getting schema by id should succeed"
    );

    // delete subject again
    restApp.restClient.deleteSubject(Collections.emptyMap(), subject);

    // register same schema with same id but with metadata
    expectedIdSchema1 = 1;
    request = new RegisterSchemaRequest();
    request.setSchema(SCHEMA_STRING);
    request.setMetadata(new Metadata(null, ImmutableMap.of("foo", "bar"), null));
    request.setVersion(1);
    request.setId(expectedIdSchema1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request, subject, false).getId(),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_STRING,
        restApp.restClient.getVersion(subject, 1).getSchema(),
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
        restApp.restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 100;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_WITH_DECIMAL,
        restApp.restClient.getVersion(subject, 1).getSchema(),
        "Getting schema by id should succeed"
    );

    // register equivalent schema with different id
    expectedIdSchema1 = 200;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_WITH_DECIMAL2, subject, 2, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_WITH_DECIMAL2,
        restApp.restClient.getVersion(subject, 2).getSchema(),
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
        restApp.restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 100;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_WITH_DECIMAL,
        restApp.restClient.getVersion(subject, 1).getSchema(),
        "Getting schema by id should succeed"
    );

    // register equivalent schema with different id
    expectedIdSchema1 = 200;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 2, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA_WITH_DECIMAL,
        restApp.restClient.getVersion(subject, 2).getSchema(),
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
        restApp.restClient.setMode(mode).getMode());

    int expectedIdSchema1 = 100;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject, 1, expectedIdSchema1));

    assertEquals(
        SCHEMA_WITH_DECIMAL,
        restApp.restClient.getVersion(subject, 1).getSchema());

    int versionOfRegisteredSubject1 =
        restApp.restClient.lookUpSubjectVersion(SCHEMA_WITH_DECIMAL, subject).getVersion();
    assertEquals(1, versionOfRegisteredSubject1);

    // register equivalent schema with different id
    expectedIdSchema1 = 200;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_WITH_DECIMAL, subject2, 1, expectedIdSchema1));

    assertEquals(
        SCHEMA_WITH_DECIMAL,
        restApp.restClient.getVersion(subject2, 1).getSchema());

    versionOfRegisteredSubject1 =
        restApp.restClient.lookUpSubjectVersion(SCHEMA_WITH_DECIMAL, subject).getVersion();
    assertEquals(1, versionOfRegisteredSubject1);

    int versionOfRegisteredSubject2 =
        restApp.restClient.lookUpSubjectVersion(SCHEMA_WITH_DECIMAL, subject2).getVersion();
    assertEquals(1, versionOfRegisteredSubject2);
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
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject),
        "Registering without id should succeed"
    );

    // delete subject so we can switch to import mode
    restApp.restClient.deleteSubject(Collections.emptyMap(), subject);

    mode = "IMPORT";

    // set mode to import
    assertEquals(
        mode,
        restApp.restClient.setMode(mode).getMode());

    // register same schema with same id
    expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA_STRING, subject, 1, expectedIdSchema1),
        "Registering with id should succeed"
    );

    // register same schema with same id
    expectedIdSchema1 = 2;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(SCHEMA2_STRING, subject, 2, expectedIdSchema1),
        "Registering with id should succeed"
    );

    assertEquals(
        SCHEMA2_STRING,
        restApp.restClient.getVersion(subject, 2).getSchema(),
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
        restApp.restClient.setMode(mode, ":.__GLOBAL:").getMode());

    Mode mode1 = restApp.restClient.getMode(null, true);
    assertEquals("readonly", mode1.getMode().toLowerCase());

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
  public void testSetForwardMode() throws Exception {
    String subject = "testSubject";
    String mode = Mode.FORWARD.toString();

    try {
      restApp.restClient.setMode(Mode.FORWARD.toString(), subject);
    } catch (RestClientException e) {
      assertEquals(42204, e.getErrorCode());
      assertEquals("Forward mode only supported on global level; error code: 42204", e.getMessage());
    }
    assertEquals(
            mode,
            restApp.restClient.setMode(mode).getMode());
  }
}
