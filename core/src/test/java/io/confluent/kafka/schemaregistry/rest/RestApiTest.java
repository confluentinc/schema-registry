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

import static io.confluent.kafka.schemaregistry.CompatibilityLevel.BACKWARD;
import static io.confluent.kafka.schemaregistry.CompatibilityLevel.FORWARD;
import static io.confluent.kafka.schemaregistry.CompatibilityLevel.FORWARD_TRANSITIVE;
import static io.confluent.kafka.schemaregistry.CompatibilityLevel.NONE;
import static io.confluent.kafka.schemaregistry.storage.Mode.IMPORT;
import static io.confluent.kafka.schemaregistry.storage.Mode.READONLY;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.ContextId;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryServerVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.ServerClusterId;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidSubjectException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidVersionException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.utils.AppInfoParser;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaParseException;
import org.junit.jupiter.api.Test;

public class RestApiTest extends ClusterTestHarness {

  public RestApiTest() {
    super(1, true);
  }

  @Test
  public void testBasic() throws Exception {
    String subject1 = "testTopic1";
    String subject2 = "testTopic2";
    int schemasInSubject1 = 10;
    List<Integer> allVersionsInSubject1 = new ArrayList<>();
    List<String> allSchemasInSubject1 = TestUtils.getRandomCanonicalAvroString(schemasInSubject1);
    int schemasInSubject2 = 5;
    List<Integer> allVersionsInSubject2 = new ArrayList<>();
    List<String> allSchemasInSubject2 = TestUtils.getRandomCanonicalAvroString(schemasInSubject2);
    List<String> allSubjects = new ArrayList<>();

    // test getAllVersions with no existing data
    try {
      restApp.restClient.getAllVersions(subject1);
      fail("Getting all versions from non-existing subject1 should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " (subject not found)");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Should get a 404 status for non-existing subject"
      );
    }

    // test getAllContexts
    assertEquals(
        Collections.singletonList(DEFAULT_CONTEXT),
        restApp.restClient.getAllContexts(),
        "Getting all subjects should return default context"
    );

    // test getAllContextsWithPagination limit=1 offset=0
    assertEquals(
        Collections.singletonList(DEFAULT_CONTEXT),
        restApp.restClient.getAllContextsWithPagination(1, 0),
        "Getting all subjects should return default context"
    );

    // test getAllContextsWithPagination limit=1 offset=0
    assertEquals(
        Collections.emptyList(),
        restApp.restClient.getAllContextsWithPagination(1, 1),
        "Getting all subjects should return default context"
    );

    // test getAllSubjects with no existing data
    assertEquals(
        allSubjects,
        restApp.restClient.getAllSubjects(),
        "Getting all subjects should return empty"
    );

    // test registering and verifying new schemas in subject1
    int schemaIdCounter = 1;
    for (int i = 0; i < schemasInSubject1; i++) {
      String schema = allSchemasInSubject1.get(i);
      int expectedVersion = i + 1;
      TestUtils.registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter,
                                        subject1);
      schemaIdCounter++;
      allVersionsInSubject1.add(expectedVersion);
    }
    allSubjects.add(subject1);

    // test re-registering existing schemas
    for (int i = 0; i < schemasInSubject1; i++) {
      int expectedId = i + 1;
      String schemaString = allSchemasInSubject1.get(i);
      int foundId = restApp.restClient.registerSchema(schemaString, subject1);
      assertEquals(
          expectedId, foundId,
          "Re-registering an existing schema should return the existing version"
      );
    }

    // test registering schemas in subject2
    for (int i = 0; i < schemasInSubject2; i++) {
      String schema = allSchemasInSubject2.get(i);
      int expectedVersion = i + 1;
      TestUtils.registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter,
                                        subject2);
      schemaIdCounter++;
      allVersionsInSubject2.add(expectedVersion);
    }
    allSubjects.add(subject2);

    // test getAllVersions with existing data
    assertEquals(
        allVersionsInSubject1,
        restApp.restClient.getAllVersions(subject1),
        "Getting all versions from subject1 should match all registered versions"
    );
    assertEquals(
        allVersionsInSubject2,
        restApp.restClient.getAllVersions(subject2),
        "Getting all versions from subject2 should match all registered versions"
    );

    // test getAllSubjects with existing data
    assertEquals(
        allSubjects,
        restApp.restClient.getAllSubjects(),
        "Getting all subjects should match all registered subjects"
    );

    // test pagination with limit of 1
    assertEquals(
        ImmutableList.of(allSubjects.get(0)),
        restApp.restClient.getAllSubjectsWithPagination(0, 1),
        "Getting all subjects with pagination offset=0, limit=1 should return first registered subject"
    );
    assertEquals(
        ImmutableList.of(allSubjects.get(1)),
        restApp.restClient.getAllSubjectsWithPagination(1, 1),
        "Getting all subjects with pagination offset=1, limit=1 should return second registered subject"
    );

    List<Schema> latestSchemas = restApp.restClient.getSchemas(null, false, true);
    assertEquals(
        2,
        latestSchemas.size(),
        "Getting latest schemas should return two schemas"
    );
    assertEquals(Integer.valueOf(10), latestSchemas.get(0).getVersion());
    assertEquals(Integer.valueOf(5), latestSchemas.get(1).getVersion());

    SchemaString schemaString = restApp.restClient.getId(
        RestService.DEFAULT_REQUEST_PROPERTIES, 1, null, null, null, false);
    assertNotNull(schemaString.getTimestamp());
    assertFalse(schemaString.getDeleted());

    SchemaString schemaString2 = restApp.restClient.getByGuid(
        RestService.DEFAULT_REQUEST_PROPERTIES, schemaString.getGuid(), null);
    assertEquals(schemaString.getGuid(), schemaString2.getGuid());
    assertNotNull(schemaString.getTimestamp());
    assertFalse(schemaString.getDeleted());

    List<ContextId> contextId = restApp.restClient.getAllContextIds(
        RestService.DEFAULT_REQUEST_PROPERTIES, schemaString.getGuid());
    assertEquals(1, contextId.size());
    assertEquals(DEFAULT_CONTEXT, contextId.get(0).getContext());
    assertEquals(1, contextId.get(0).getId());
  }

  @Test
  public void testRegisterSameSchemaOnDifferentSubject() throws Exception {
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    int id1 = restApp.restClient.registerSchema(schema, "subject1");
    int id2 = restApp.restClient.registerSchema(schema, "subject2");
    assertEquals(
        id1, id2,
        "Registering the same schema under different subjects should return the same id"
    );
  }

  @Test
  public void testRegisterBadDefault() throws Exception {
    String subject = "testSubject";

    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"default\":null,\"name\":"
        + "\"f" + "\"}]}";
    String schema = AvroUtils.parseSchema(schemaString).canonicalString();

    try {
      restApp.restClient.testCompatibility(schema, subject, "latest");
      fail("Testing compatibility for schema with invalid default should fail with "
          + Errors.INVALID_SCHEMA_ERROR_CODE
          + " (invalid schema)");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }

    try {
      restApp.restClient.registerSchema(schema, subject);
      fail("Registering schema with invalid default should fail with "
          + Errors.INVALID_SCHEMA_ERROR_CODE
          + " (invalid schema)");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }
  }

  @Test
  public void testRegisterInvalidSchemaBadType() throws Exception {
    String subject = "testSubject";

    //Invalid Field Type 'str'
    String badSchemaString = "{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"str\",\"name\":\"field1\"}]}";

    String expectedErrorMessage = null;
    try {
        new Parser().parse(badSchemaString);
        fail("Parsing invalid schema string should fail with SchemaParseException");
    } catch (SchemaParseException spe) {
        expectedErrorMessage = spe.getMessage();
    }

    try {
        restApp.restClient.registerSchema(badSchemaString, subject);
        fail("Registering schema with invalid field type should fail with "
                + Errors.INVALID_SCHEMA_ERROR_CODE
                + " (invalid schema)");
    } catch (RestClientException rce) {
        assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
        assertTrue(rce.getMessage().contains(expectedErrorMessage), "Verify error message verbosity");
    }
  }

  @Test
  public void testRegisterInvalidSchemaBadReference() throws Exception {
    String subject = "testSubject";

    //Invalid Reference
    SchemaReference invalidReference = new SchemaReference("invalid.schema", "badSubject", 1);
    String schemaString = "{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":\"field1\"}]}";

    try {
      restApp.restClient.registerSchema(schemaString, "AVRO",
              Collections.singletonList(invalidReference), subject);
      fail("Registering schema with invalid reference should fail with "
              + Errors.INVALID_SCHEMA_ERROR_CODE
              + " (invalid schema)");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }
  }

  @Test
  public void testRegisterBadReferenceInContext() throws Exception {
    List<String> avroSchemas = TestUtils.getRandomCanonicalAvroString(2);

    String subject = "testSubject";
    String avroSchema = avroSchemas.get(0);

    int id1 = restApp.restClient.registerSchema(avroSchema, subject);
    assertEquals(1, id1, "1st schema registered in first context should have id 1");

    // Create another context so that lookup will search it
    String subject2 = ":.ctx:testFoo";
    String avroSchema2 = avroSchemas.get(1);

    int id2 = restApp.restClient.registerSchema(avroSchema2, subject2);
    assertEquals(1, id2, "2nd schema registered in second context should have id 1");

    SchemaReference reference = new SchemaReference("testSubject", "testSubject", 1);
    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"somerecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"field1\"}]}";

    try {
      restApp.restClient.lookUpSubjectVersion(schemaString, "AVRO",
          Collections.singletonList(reference), subject, false);
      fail(String.format("Subject %s should not be found", subject));
    } catch (RestClientException rce) {
      assertEquals(Errors.SCHEMA_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
  }

  @Test
  public void testRegisterDiffSchemaType() throws Exception {
    String subject = "testSubject";
    String avroSchema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    String jsonSchema = io.confluent.kafka.schemaregistry.rest.json.RestApiTest.getRandomJsonSchemas(1).get(0);
    String protobufSchema = io.confluent.kafka.schemaregistry.rest.protobuf.RestApiTest.getRandomProtobufSchemas(1).get(0);

    restApp.restClient.updateCompatibility(NONE.name, subject);

    int id1 = restApp.restClient.registerSchema(avroSchema, subject);
    assertEquals(1, id1, "1st schema registered globally should have id 1");

    boolean isCompatible = restApp.restClient.testCompatibility(jsonSchema, "JSON", null, subject, "latest", false).isEmpty();
    assertTrue(isCompatible, "Different schema type is allowed when compatibility is NONE");

    int id2 = restApp.restClient.registerSchema(jsonSchema, "JSON", null, subject).getId();
    assertEquals(2, id2, "2nd schema registered globally should have id 2");

    isCompatible = restApp.restClient.testCompatibility(protobufSchema, "PROTOBUF", null, subject, "latest", false).isEmpty();
    assertTrue(isCompatible, "Different schema type is allowed when compatibility is NONE");

    int id3 = restApp.restClient.registerSchema(protobufSchema, "PROTOBUF", null, subject).getId();
    assertEquals(3, id3, "3rd schema registered globally should have id 3");
  }

  @Test
  public void testRegisterDiffContext() throws Exception {
    List<String> avroSchemas = TestUtils.getRandomCanonicalAvroString(2);

    String subject = "testSubject";
    String avroSchema = avroSchemas.get(0);

    int id1 = restApp.restClient.registerSchema(avroSchema, subject);
    assertEquals(1, id1, "1st schema registered in first context should have id 1");

    String subject2 = ":.ctx:testSubject";
    String avroSchema2 = avroSchemas.get(1);

    int id2 = restApp.restClient.registerSchema(avroSchema2, subject2);
    assertEquals(1, id2, "2nd schema registered in second context should have id 1");

    List<String> subjects = restApp.restClient.getAllSubjects("", false);
    assertEquals(Collections.singletonList(subject), subjects);

    List<Schema> schemas = restApp.restClient.getSchemas(null, false, false);
    assertEquals(avroSchema, schemas.get(0).getSchema());

    List<String> subjects2 = restApp.restClient.getAllSubjects(":.ctx:", false);
    assertEquals(Collections.singletonList(subject2), subjects2);

    List<Schema> schemas2 = restApp.restClient.getSchemas(":.ctx:", false, false);
    assertEquals(avroSchema2, schemas2.get(0).getSchema());

    int id3 = restApp.restClient.registerSchema(avroSchema, subject2);
    assertEquals(2, id3, "3nd schema registered in second context should have id 2");

    SchemaString schemaString = restApp.restClient.getId(
        RestService.DEFAULT_REQUEST_PROPERTIES, 1, subject, null, null, false);
    SchemaString schemaString2 = restApp.restClient.getId(
        RestService.DEFAULT_REQUEST_PROPERTIES, 2, subject2, null, null, false);
    assertEquals(schemaString.getGuid(), schemaString2.getGuid());
    assertNotNull(schemaString.getTimestamp());
  }

  @Test
  public void testImportDifferentSchemaOnSameID() throws Exception {
    String schema1 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"field1\"}]}";

    String schema2 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"field1\"},"
        + "{\"type\":\"int\",\"name\":\"field2\",\"default\":0}]}";

    restApp.restClient.setMode("IMPORT");

    try {
      int id1 = restApp.restClient.registerSchema(schema1, "subject1", 1, 1);
      assertEquals(1, id1);
      int id2 = restApp.restClient.registerSchema(schema2, "subject1", 2, 1);
      fail(String.format("Schema2 is registered with id %s, should receive error here", id2));
    } catch (RestClientException e) {
      assertEquals(
          Errors.OPERATION_NOT_PERMITTED_ERROR_CODE, e.getErrorCode(),
          "Overwrite schema for the same ID is not permitted."
      );
    }

    try {
      int id1 = restApp.restClient.registerSchema(schema1, "subject1", 1, 1);
      assertEquals(1, id1);
      int id2 = restApp.restClient.registerSchema(schema2, "subject2", 1, 1);
      fail(String.format("Schema2 is registered with id %s, should receive error here", id2));
    } catch (RestClientException e) {
      assertEquals(
          Errors.OPERATION_NOT_PERMITTED_ERROR_CODE, e.getErrorCode(),
          "Overwrite schema for the same ID is not permitted."
      );
    }
  }

  @Test
  public void testImportSameSchemaDifferentVersion() throws Exception {
    String schema = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"field1\"}]}";

    restApp.restClient.setMode("IMPORT");

    int id1 = restApp.restClient.registerSchema(schema, "subject1", 1, 1);
    assertEquals(1, id1);
    id1 = restApp.restClient.registerSchema(schema, "subject1", 2, 1);
    assertEquals(1, id1);
    id1 = restApp.restClient.registerSchema(schema, "subject1", 1, 1);
    assertEquals(1, id1);
  }

  @Test
  public void testCompatibleSchemaLookupBySubject() throws Exception {
    String subject = "testSubject";
    int numRegisteredSchemas = 0;
    int numSchemas = 10;

    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(numSchemas);
    restApp.restClient.updateCompatibility(NONE.name, subject);

    restApp.restClient.registerSchema(allSchemas.get(0), subject);
    numRegisteredSchemas++;

    // test compatibility of this schema against the latest version under the subject
    String schema1 = allSchemas.get(0);
    boolean isCompatible =
        restApp.restClient.testCompatibility(schema1, subject, "latest").isEmpty();
    assertTrue(isCompatible, "First schema registered should be compatible");

    for (int i = 0; i < numSchemas; i++) {
      // Test that compatibility check doesn't change the number of versions
      String schema = allSchemas.get(i);
      isCompatible = restApp.restClient.testCompatibility(schema, subject, "latest").isEmpty();
      TestUtils.checkNumberOfVersions(restApp.restClient, numRegisteredSchemas, subject);
    }
  }

  @Test
  public void testIncompatibleSchemaLookupBySubject() throws Exception {
    String subject = "testSubject";

    // Make two incompatible schemas - field 'f' has different types
    String schema1String = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":"
                           + "\"f" + "\"}]}";
    String schema1 = AvroUtils.parseSchema(schema1String).canonicalString();

    String schema2String = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"int\",\"name\":"
                           + "\"f" + "\"}]}";
    String schema2 = AvroUtils.parseSchema(schema2String).canonicalString();

    // ensure registering incompatible schemas will raise an error
    restApp.restClient.updateCompatibility(
        CompatibilityLevel.FULL.name, subject);

    // test that compatibility check for incompatible schema returns false and the appropriate
    // error response from Avro
    restApp.restClient.registerSchema(schema1, subject);
    int versionOfRegisteredSchema =
        restApp.restClient.lookUpSubjectVersion(schema1, subject).getVersion();
    boolean isCompatible = restApp.restClient.testCompatibility(schema2, subject,
                                                                String.valueOf(
                                                                    versionOfRegisteredSchema))
                                              .isEmpty();
    assertFalse(isCompatible, "Schema should be incompatible with specified version");
  }

  @Test
  public void testIncompatibleSchemaBySubject() throws Exception {
    String subject = "testSubject";

    String schema1String = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},{\"type\":\"string\",\"name\":\"f2\"}]}";
    String schema1 = AvroUtils.parseSchema(schema1String).canonicalString();

    String schema2String = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}";
    String schema2 = AvroUtils.parseSchema(schema2String).canonicalString();

    String schema3String = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},{\"type\":\"string\",\"name\":\"f3\"}]}";
    String schema3 = AvroUtils.parseSchema(schema3String).canonicalString();

    restApp.restClient.registerSchema(schema1, subject);
    restApp.restClient.registerSchema(schema2, subject);

    restApp.restClient.updateCompatibility(CompatibilityLevel.FORWARD_TRANSITIVE.name, subject);

    //schema3 is compatible with schema2, but not compatible with schema1
    boolean isCompatible = restApp.restClient.testCompatibility(schema3, subject, "latest").isEmpty();
    assertTrue(isCompatible, "Schema is compatible with the latest version");
    isCompatible = restApp.restClient.testCompatibility(schema3, subject, null).isEmpty();
    assertFalse(isCompatible, "Schema should be incompatible with FORWARD_TRANSITIVE setting");
    try {
      restApp.restClient.registerSchema(schema3String, subject);
      fail("Schema register should fail since schema is incompatible");
    } catch (RestClientException e) {
      assertEquals(Errors.INCOMPATIBLE_SCHEMA_ERROR_CODE, e.getErrorCode());
      assertFalse(e.getMessage().isEmpty());
      assertTrue(e.getMessage().contains("oldSchemaVersion:"));
      assertTrue(e.getMessage().contains("oldSchema:"));
      assertTrue(e.getMessage().contains("compatibility:"));
    }
  }

  @Test
  public void testBadFormat() throws Exception {
    String subject = "testSubject";

    String schema1String = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":"
        + "\"f" + "\"}]}";
    String schema1 = AvroUtils.parseSchema(schema1String).canonicalString();

    restApp.restClient.registerSchema(schema1, subject);

    SchemaString schemaString = restApp.restClient.getId(
        RestService.DEFAULT_REQUEST_PROPERTIES, 1, null, "bad-format", null, false);
    assertEquals(
        schema1,
        schemaString.getSchemaString(),
        "Registered schema should be found"
    );
  }

  @Test
  public void testSchemaRegistrationUnderDiffSubjects() throws Exception {
    String subject1 = "testSubject1";
    String subject2 = "testSubject2";

    // Make two incompatible schemas - field 'f' has different types
    String schemaString1 = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":"
                           + "\"f" + "\"}]}";
    String schema1 = AvroUtils.parseSchema(schemaString1).canonicalString();
    String schemaString2 = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"int\",\"name\":"
                           + "\"foo" + "\"}]}";
    String schema2 = AvroUtils.parseSchema(schemaString2).canonicalString();

    restApp.restClient.updateCompatibility(
        CompatibilityLevel.NONE.name, subject1);
    restApp.restClient.updateCompatibility(
        CompatibilityLevel.NONE.name, subject2);

    int idOfRegisteredSchema1Subject1 =
        restApp.restClient.registerSchema(schema1, subject1);
    int versionOfRegisteredSchema1Subject1 =
        restApp.restClient.lookUpSubjectVersion(schema1, subject1).getVersion();
    assertEquals(
        1, versionOfRegisteredSchema1Subject1,
        "1st schema under subject1 should have version 1"
    );
    assertEquals(
        1, idOfRegisteredSchema1Subject1,
        "1st schema registered globally should have id 1"
    );

    int idOfRegisteredSchema2Subject1 =
        restApp.restClient.registerSchema(schema2, subject1);
    int versionOfRegisteredSchema2Subject1 =
        restApp.restClient.lookUpSubjectVersion(schema2, subject1).getVersion();
    assertEquals(
        2, versionOfRegisteredSchema2Subject1,
        "2nd schema under subject1 should have version 2"
    );
    assertEquals(
        2, idOfRegisteredSchema2Subject1,
        "2nd schema registered globally should have id 2"
    );

    int idOfRegisteredSchema2Subject2 =
        restApp.restClient.registerSchema(schema2, subject2);
    int versionOfRegisteredSchema2Subject2 =
        restApp.restClient.lookUpSubjectVersion(schema2, subject2).getVersion();
    assertEquals(
        1,
        versionOfRegisteredSchema2Subject2,
        "2nd schema under subject1 should still have version 1 as the first schema under subject2"
    );
    assertEquals(
        2,
        idOfRegisteredSchema2Subject2,
        "Since schema is globally registered but not under subject2, id should not change"
    );
  }

  @Test
  public void testConfigDefaults() throws Exception {
    assertEquals(
        NONE.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be none for this test instance"
    );

    // change it to forward
    restApp.restClient.updateCompatibility(CompatibilityLevel.FORWARD.name, null);

    assertEquals(
        FORWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "New compatibility level should be forward for this test instance"
    );
  }

  @Test
  public void testNonExistentSubjectConfigChange() throws Exception {
    String subject = "testSubject";
    try {
      restApp.restClient.updateCompatibility(CompatibilityLevel.FORWARD.name, subject);
    } catch (RestClientException e) {
      fail("Changing config for an invalid subject should succeed");
    }
    assertEquals(
        FORWARD.name,
        restApp.restClient.getConfig(subject).getCompatibilityLevel(),
        "New compatibility level for this subject should be forward"
    );
  }

  @Test
  public void testSubjectConfigChange() throws Exception {
    String subject = "testSubject";
    assertEquals(
        NONE.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be none for this test instance"
    );

    // change subject compatibility to forward
    restApp.restClient.updateCompatibility(CompatibilityLevel.FORWARD.name, subject);

    assertEquals(
        NONE.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Global compatibility level should remain none for this test instance"
    );

    assertEquals(
        FORWARD.name,
        restApp.restClient.getConfig(subject).getCompatibilityLevel(),
        "New compatibility level for this subject should be forward"
    );

    // delete subject compatibility
    restApp.restClient.deleteConfig(subject);

    assertEquals(
        NONE.name,
        restApp.restClient
            .getConfig(RestService.DEFAULT_REQUEST_PROPERTIES, subject, true)
            .getCompatibilityLevel(),
        "Compatibility level for this subject should be reverted to none"
    );
  }

  @Test
  public void testGlobalConfigChange() throws Exception{
    assertEquals(
        NONE.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be none for this test instance"
    );

    // change subject compatibility to forward
    restApp.restClient.updateCompatibility(CompatibilityLevel.FORWARD.name, null);
    assertEquals(
        FORWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "New Global compatibility level should be forward"
    );

    // change subject compatibility to backward
    restApp.restClient.updateCompatibility(BACKWARD.name, null);
    assertEquals(
        BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "New Global compatibility level should be backward"
    );

    // delete Global compatibility
    restApp.restClient.deleteConfig(null);
    assertEquals(
        NONE.name,
        restApp.restClient
            .getConfig(RestService.DEFAULT_REQUEST_PROPERTIES, null, true)
            .getCompatibilityLevel(),
        "Global compatibility level should be reverted to none"
    );
  }

  @Test
  public void testDefaultContextConfigAndMode() throws Exception {
    String defaultContext = ":.:";

    ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest();
    configUpdateRequest.setCompatibilityLevel(FORWARD.name());
    assertEquals(
        configUpdateRequest,
        restApp.restClient.updateConfig(configUpdateRequest, null));

    assertEquals(
        FORWARD.name,
        restApp.restClient
            .getConfig(RestService.DEFAULT_REQUEST_PROPERTIES, defaultContext, true)
            .getCompatibilityLevel()
    );

    configUpdateRequest = new ConfigUpdateRequest();
    configUpdateRequest.setCompatibilityLevel(FORWARD_TRANSITIVE.name());
    assertEquals(
        configUpdateRequest,
        restApp.restClient.updateConfig(configUpdateRequest, defaultContext));

    assertEquals(
        FORWARD_TRANSITIVE.name,
        restApp.restClient
            .getConfig(RestService.DEFAULT_REQUEST_PROPERTIES, null, true)
            .getCompatibilityLevel()
    );

    restApp.restClient.deleteConfig(defaultContext);

    assertEquals(
        NONE.name,
        restApp.restClient
            .getConfig(RestService.DEFAULT_REQUEST_PROPERTIES, null, true)
            .getCompatibilityLevel()
    );

    ModeUpdateRequest modeUpdateRequest = new ModeUpdateRequest(IMPORT.name());
    assertEquals(
        modeUpdateRequest,
        restApp.restClient.setMode(IMPORT.name(), null));

    assertEquals(
        IMPORT.name(),
        restApp.restClient
            .getMode(defaultContext, true)
            .getMode()
    );

    modeUpdateRequest = new ModeUpdateRequest(READONLY.name());
    assertEquals(
        modeUpdateRequest,
        restApp.restClient.setMode(READONLY.name(), defaultContext));

    assertEquals(
        READONLY.name(),
        restApp.restClient
            .getMode(null, true)
            .getMode()
    );

    // We don't support deleting the mode for the default context
    assertThrows(
        RestClientException.class,
        () -> restApp.restClient.deleteSubjectMode(defaultContext)
    );
  }

  @Test
  public void testGetSchemaNonExistingId() throws Exception {
    try {
      restApp.restClient.getId(100);
      fail("Schema lookup by missing id should fail with "
           + Errors.SCHEMA_NOT_FOUND_ERROR_CODE
           + " (schema not found)");
    } catch (RestClientException rce) {
      // this is expected.
      assertEquals(
          Errors.SCHEMA_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Should get a 404 status for non-existing id"
      );
    }
  }

  @Test
  public void testGetSchemaWithFetchMaxId() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(3);
    int latestId = 0;

    for (String schema : schemas) {
      latestId = restApp.restClient.registerSchema(schema, "subject");
    }

    // if fetchMaxId is not provided then the maxId is null
    assertNull(restApp.restClient.getId(1).getMaxId());
    assertEquals(Integer.valueOf(latestId), restApp.restClient.getId(1, null, true).getMaxId());
  }

  @Test
  public void testGetSchemaTypes() throws Exception {
    assertEquals(new HashSet<>(Arrays.asList("AVRO", "JSON", "PROTOBUF")),
        new HashSet<>(restApp.restClient.getSchemaTypes()));
  }

  @Test
  public void testListVersionsNonExistingSubject() throws Exception {
    try {
      restApp.restClient.getAllVersions("Invalid");
      fail("Getting all versions of missing subject should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " (subject not found)");
    } catch (RestClientException rce) {
      // this is expected.
      assertEquals(
          Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Should get a 404 status for non-existing subject"
      );
    }
  }

  @Test
  public void testGetVersionNonExistentSubject() throws Exception {
    // test getVersion on a non-existing subject
    try {
      restApp.restClient.getVersion("non-existing-subject", 1);
      fail("Getting version of missing subject should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " (subject not found)");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
          e.getErrorCode(),
          "Unregistered subject shouldn't be found in getVersion()"
      );
    }
  }

  @Test
  public void testGetNonExistingVersion() throws Exception {
    // test getVersion on a non-existing version
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject);
    try {
      restApp.restClient.getVersion(subject, 200);
      fail("Getting unregistered version should fail with "
           + Errors.VERSION_NOT_FOUND_ERROR_CODE
           + " (version not found)");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          Errors.VERSION_NOT_FOUND_ERROR_CODE, e.getErrorCode(),
          "Unregistered version shouldn't be found"
      );
    }
  }

  @Test
  public void testGetInvalidVersion() throws Exception {
    // test getVersion on a non-existing version
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject);
    try {
      restApp.restClient.getVersion(subject, 0);
      fail("Getting invalid version should fail with "
           + RestInvalidVersionException.ERROR_CODE
           + " (invalid version)");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidVersionException.ERROR_CODE,
          e.getErrorCode(),
          "Invalid version shouldn't be found"
      );
    }
  }

  @Test
  public void testRegisterInvalidSubject() throws Exception {
    // test invalid subject
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    String subject = "\rbad\nsubject\t";
    try {
      TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject);
      fail("Registering invalid subject should fail with 400");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          400,
          e.getStatus(),
          "Invalid subject shouldn't be registered"
      );
    }
  }

  @Test
  public void testGetVersion() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals(
        schemas.get(0),
        restApp.restClient.getVersion(subject, 1).getSchema(),
        "Version 1 schema should match"
    );

    assertEquals(
        schemas.get(1),
        restApp.restClient.getVersion(subject, 2).getSchema(),
        "Version 2 schema should match"
    );
    assertEquals(
        schemas.get(1),
        restApp.restClient.getLatestVersion(subject).getSchema(),
        "Latest schema should be the same as version 2"
    );
  }

  @Test
  public void testGetOnlySchemaById() throws Exception {
    String schema = String.valueOf(TestUtils.getRandomCanonicalAvroString(1));
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject);
    assertEquals(
        schema,
        restApp.restClient.getOnlySchemaById(1),
        "Schema with ID 1 should match."
    );
  }

  @Test
  public void testGetLatestVersionSchemaOnly() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals(
        schemas.get(1),
        restApp.restClient.getLatestVersionSchemaOnly(subject),
        "Latest schema should be the same as version 2"
    );
  }

  @Test
  public void testGetVersionSchemaOnly() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(1);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);

    assertEquals(
        schemas.get(0),
        restApp.restClient.getVersionSchemaOnly(subject, 1),
        "Retrieved schema should be the same as version 1"
    );
  }

  @Test
  public void testSchemaReferences() throws Exception {
    testSchemaReferencesInContext("", "", 2);
  }

  @Test
  public void testSchemaReferencesSameContext() throws Exception {
    testSchemaReferencesInContext(":.ctx:", ":.ctx:", 2);
  }

  @Test
  public void testSchemaReferencesDifferentContext() throws Exception {
    testSchemaReferencesInContext("", ":.ctx:", 1);
  }

  private void testSchemaReferencesInContext(String context, String refContext, int parentId)
      throws Exception {
    List<String> schemas = TestUtils.getAvroSchemaWithReferences();
    String unqualifiedSubject = "my_reference";
    String subject = refContext + unqualifiedSubject;
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get(1));
    SchemaReference ref = new SchemaReference("otherns.Subrecord", subject, 1);
    request.setReferences(Collections.singletonList(ref));
    String subject2 = context + "my_referrer";
    int registeredId = restApp.restClient.registerSchema(request, subject2, false).getId();
    assertEquals(parentId, registeredId, "Registering a new schema should succeed");

    SchemaString schemaString = restApp.restClient.getId(parentId, subject2);
    assertEquals(subject2, schemaString.getSubject());
    assertEquals(1, schemaString.getVersion());

    // the newly registered schema should be immediately readable on the leader
    assertEquals(
        schemas.get(1),
        schemaString.getSchemaString(),
        "Registered schema should be found"
    );

    assertEquals(
        Collections.singletonList(ref),
        schemaString.getReferences(),
        "Schema references should be found"
    );

    List<Integer> refs = restApp.restClient.getReferencedBy(subject, 1);
    assertEquals(parentId, refs.get(0).intValue());

    ns.MyRecord myrecord = new ns.MyRecord();
    AvroSchema schema = new AvroSchema(AvroSchemaUtils.getSchema(myrecord));
    // Note that we pass an empty list of refs since SR will perform a deep equality check
    Schema registeredSchema = restApp.restClient.lookUpSubjectVersion(schema.canonicalString(),
        AvroSchema.TYPE, Collections.emptyList(), subject2, false);
    assertEquals(
        parentId, registeredSchema.getId().intValue(),
        "Registered schema should be found"
    );

    try {
      restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
          subject,
          String.valueOf(1)
      );
      fail("Deleting reference should fail with " + Errors.REFERENCE_EXISTS_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.REFERENCE_EXISTS_ERROR_CODE,
          rce.getErrorCode(),
          "Reference found"
      );
    }

    assertEquals((Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, subject2, "1"));

    refs = restApp.restClient.getReferencedBy(subject, 1);
    assertTrue(refs.isEmpty());

    refs = restApp.restClient.getReferencedByWithPagination(subject, 1, 0, 1);
    assertTrue(refs.isEmpty());

    assertEquals((Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1"));
  }

  @Test
  public void testSchemaUnqualifiedReferencesInContext() throws Exception {
    String context = ":.ctx:";
    int parentId = 2;
    List<String> schemas = TestUtils.getAvroSchemaWithReferences();
    String unqualifiedSubject = "my_reference";
    String subject = context + unqualifiedSubject;
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get(1));
    SchemaReference ref = new SchemaReference("otherns.Subrecord", unqualifiedSubject, 1);
    request.setReferences(Collections.singletonList(ref));
    String subject2 = context + "my_referrer";
    int registeredId = restApp.restClient.registerSchema(request, subject2, false).getId();
    assertEquals(parentId, registeredId, "Registering a new schema should succeed");

    SchemaString schemaString = restApp.restClient.getId(parentId, subject2);
    assertEquals(subject2, schemaString.getSubject());
    assertEquals(1, schemaString.getVersion());
    assertFalse(schemaString.getReferences().get(0).getSubject().startsWith(context));

    Schema schemaResult = restApp.restClient.getVersion(subject2, 1);
    assertEquals(subject2, schemaResult.getSubject());
    assertEquals(1, schemaResult.getVersion());
    assertFalse(schemaResult.getReferences().get(0).getSubject().startsWith(context));

    SchemaString schemaString2 = restApp.restClient.getId(RestService.DEFAULT_REQUEST_PROPERTIES,
        parentId, subject2, null, "qualified", null, false);
    assertEquals(subject2, schemaString2.getSubject());
    assertEquals(1, schemaString2.getVersion());
    assertTrue(schemaString2.getReferences().get(0).getSubject().startsWith(context));

    Schema schemaResult2 = restApp.restClient.getVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
        subject2, 1, null, "qualified", false, null);
    assertEquals(subject2, schemaResult2.getSubject());
    assertEquals(1, schemaResult2.getVersion());
    assertTrue(schemaResult2.getReferences().get(0).getSubject().startsWith(context));

    // the newly registered schema should be immediately readable on the leader
    assertEquals(
        schemas.get(1),
        schemaString.getSchemaString(),
        "Registered schema should be found"
    );

    assertEquals(
        Collections.singletonList(ref),
        schemaString.getReferences(),
        "Schema references should be found"
    );

    List<Integer> refs = restApp.restClient.getReferencedBy(subject, 1);
    assertEquals(parentId, refs.get(0).intValue());

    ns.MyRecord myrecord = new ns.MyRecord();
    AvroSchema schema = new AvroSchema(AvroSchemaUtils.getSchema(myrecord));
    // Note that we pass an empty list of refs since SR will perform a deep equality check
    Schema registeredSchema = restApp.restClient.lookUpSubjectVersion(schema.canonicalString(),
        AvroSchema.TYPE, Collections.emptyList(), subject2, false);
    assertEquals(
        parentId, registeredSchema.getId().intValue(),
        "Registered schema should be found"
    );

    try {
      restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
          subject,
          String.valueOf(1)
      );
      fail("Deleting reference should fail with " + Errors.REFERENCE_EXISTS_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.REFERENCE_EXISTS_ERROR_CODE,
          rce.getErrorCode(),
          "Reference found"
      );
    }

    assertEquals((Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, subject2, "1"));

    refs = restApp.restClient.getReferencedBy(subject, 1);
    assertTrue(refs.isEmpty());

    refs = restApp.restClient.getReferencedByWithPagination(subject, 1, 0, 1);
    assertTrue(refs.isEmpty());

    assertEquals((Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1"));
  }

  @Test
  public void testSchemaReferencesMultipleLevels() throws Exception {
    String root = "[\"myavro.BudgetDecreased\",\"myavro.BudgetUpdated\"]";

    String ref1 = "{\n"
        + "  \"type\" : \"record\",\n"
        + "  \"name\" : \"BudgetDecreased\",\n"
        + "  \"namespace\" : \"myavro\",\n"
        + "  \"fields\" : [ {\n"
        + "    \"name\" : \"buyerId\",\n"
        + "    \"type\" : \"long\"\n"
        + "  }, {\n"
        + "    \"name\" : \"currency\",\n"
        + "    \"type\" : {\n"
        + "      \"type\" : \"myavro.currencies.Currency\""
        + "    }\n"
        + "  }, {\n"
        + "    \"name\" : \"amount\",\n"
        + "    \"type\" : \"double\"\n"
        + "  } ]\n"
        + "}";

    String ref2 = "{\n"
        + "  \"type\" : \"record\",\n"
        + "  \"name\" : \"BudgetUpdated\",\n"
        + "  \"namespace\" : \"myavro\",\n"
        + "  \"fields\" : [ {\n"
        + "    \"name\" : \"buyerId\",\n"
        + "    \"type\" : \"long\"\n"
        + "  }, {\n"
        + "    \"name\" : \"currency\",\n"
        + "    \"type\" : {\n"
        + "      \"type\" : \"myavro.currencies.Currency\""
        + "    }\n"
        + "  }, {\n"
        + "    \"name\" : \"updatedValue\",\n"
        + "    \"type\" : \"double\"\n"
        + "  } ]\n"
        + "}";

    String sharedRef = "{\n"
        + "      \"type\" : \"enum\",\n"
        + "      \"name\" : \"Currency\",\n"
        + "      \"namespace\" : \"myavro.currencies\",\n"
        + "      \"symbols\" : [ \"EUR\", \"USD\" ]\n"
        + "    }\n";

    TestUtils.registerAndVerifySchema(
        restApp.restClient, new AvroSchema(sharedRef).canonicalString(), 1, "shared");

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(ref1);
    SchemaReference ref = new SchemaReference("myavro.currencies.Currency", "shared", 1);
    request.setReferences(Collections.singletonList(ref));
    int registeredId = restApp.restClient.registerSchema(request, "ref1", false).getId();
    assertEquals(2, registeredId, "Registering a new schema should succeed");

    request = new RegisterSchemaRequest();
    request.setSchema(ref2);
    ref = new SchemaReference("myavro.currencies.Currency", "shared", 1);
    request.setReferences(Collections.singletonList(ref));
    registeredId = restApp.restClient.registerSchema(request, "ref2", false).getId();
    assertEquals(3, registeredId, "Registering a new schema should succeed");

    request = new RegisterSchemaRequest();
    request.setSchema(root);
    SchemaReference r1 = new SchemaReference("myavro.BudgetDecreased", "ref1", 1);
    SchemaReference r2 = new SchemaReference("myavro.BudgetUpdated", "ref2", 1);
    request.setReferences(Arrays.asList(r1, r2));
    registeredId = restApp.restClient.registerSchema(request, "root", false).getId();
    assertEquals(4, registeredId, "Registering a new schema should succeed");

    SchemaString schemaString = restApp.restClient.getId(4);
    // the newly registered schema should be immediately readable on the leader
    assertEquals(
        root,
        schemaString.getSchemaString(),
        "Registered schema should be found"
    );

    assertEquals(
        Arrays.asList(r1, r2),
        schemaString.getReferences(),
        "Schema references should be found"
    );

    SchemaString schemaString2 = restApp.restClient.getId(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        4, "root", "resolved", null, false);
    // Ensure the guids match even though we are formatting the schema
    assertEquals(schemaString.getGuid(), schemaString2.getGuid());
  }

  @Test
  public void testSchemaMissingReferences() throws Exception {
    assertThrows(RestClientException.class, () -> {
      List<String> schemas = TestUtils.getAvroSchemaWithReferences();

      RegisterSchemaRequest request = new RegisterSchemaRequest();
      request.setSchema(schemas.get(1));
      request.setReferences(Collections.emptyList());
      restApp.restClient.registerSchema(request, "referrer", false);
    });
  }

  @Test
  public void testSchemaNormalization() throws Exception {
    String subject1 = "testSubject1";

    String reference1 = "{\"type\":\"record\","
        + "\"name\":\"Subrecord1\","
        + "\"namespace\":\"otherns\","
        + "\"fields\":"
        + "[{\"name\":\"field1\",\"type\":\"string\"}]}";
    TestUtils.registerAndVerifySchema(restApp.restClient, reference1, 1, "ref1");
    String reference2 = "{\"type\":\"record\","
        + "\"name\":\"Subrecord2\","
        + "\"namespace\":\"otherns\","
        + "\"fields\":"
        + "[{\"name\":\"field2\",\"type\":\"string\"}]}";
    TestUtils.registerAndVerifySchema(restApp.restClient, reference2, 2, "ref2");

    SchemaReference ref1 = new SchemaReference("otherns.Subrecord1", "ref1", 1);
    SchemaReference ref2 = new SchemaReference("otherns.Subrecord2", "ref2", 1);

    // Two versions of same schema
    String schemaString1 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":{\"type\":\"int\"},\"name\":\"field0" + "\"},"
        + "{\"name\":\"field1\",\"type\":\"otherns.Subrecord1\"},"
        + "{\"name\":\"field2\",\"type\":\"otherns.Subrecord2\"}"
        + "],"
        + "\"extraMetadata\": {\"a\": 1, \"b\": 2}"
        + "}";
    String schemaString2 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"int\",\"name\":\"field0" + "\"},"
        + "{\"name\":\"field1\",\"type\":\"otherns.Subrecord1\"},"
        + "{\"name\":\"field2\",\"type\":\"otherns.Subrecord2\"}"
        + "],"
        + "\"extraMetadata\": {\"b\": 2, \"a\": 1}"
        + "}";

    RegisterSchemaRequest registerRequest = new RegisterSchemaRequest();
    registerRequest.setSchema(schemaString1);
    registerRequest.setReferences(Arrays.asList(ref1, ref2));
    int idOfRegisteredSchema1Subject1 =
        restApp.restClient.registerSchema(registerRequest, subject1, true).getId();
    RegisterSchemaRequest lookUpRequest = new RegisterSchemaRequest();
    lookUpRequest.setSchema(schemaString2);
    lookUpRequest.setReferences(Arrays.asList(ref2, ref1));
    int versionOfRegisteredSchema1Subject1 =
        restApp.restClient.lookUpSubjectVersion(lookUpRequest, subject1, true, false).getVersion();
    assertEquals(
        1, versionOfRegisteredSchema1Subject1,
        "1st schema under subject1 should have version 1"
    );
    assertEquals(
        3, idOfRegisteredSchema1Subject1,
        "1st schema registered globally should have id 3"
    );

    // send schema with all references resolved
    lookUpRequest = new RegisterSchemaRequest();
    Parser parser = new Parser();
    parser.parse(reference1);
    parser.parse(reference2);
    AvroSchema resolvedSchema = new AvroSchema(parser.parse(schemaString2));
    lookUpRequest.setSchema(resolvedSchema.canonicalString());
    versionOfRegisteredSchema1Subject1 =
        restApp.restClient.lookUpSubjectVersion(lookUpRequest, subject1, true, false).getVersion();
    assertEquals(
        1, versionOfRegisteredSchema1Subject1,
        "1st schema under subject1 should have version 1"
    );


    String recordInvalidDefaultSchema =
        "{\"namespace\": \"namespace\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"test\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"string_default\", \"type\": \"string\", \"default\": null}\n"
            + "]\n"
            + "}";
    registerRequest = new RegisterSchemaRequest();
    registerRequest.setSchema(recordInvalidDefaultSchema);
    try {
      restApp.restClient.registerSchema(registerRequest, subject1, true);
      fail("Registering bad schema should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.INVALID_SCHEMA_ERROR_CODE,
          rce.getErrorCode(),
          "Invalid schema"
      );
    }

    List<String> messages = restApp.restClient.testCompatibility(
        registerRequest, subject1, null, true, true);
    assertTrue(!messages.isEmpty() && messages.get(0).contains("Invalid schema"));
  }

  @Test
  public void testBad() throws Exception {
    String subject1 = "testTopic1";
    List<String> allSubjects = new ArrayList<>();

    // test getAllSubjects with no existing data
    assertEquals(
        allSubjects,
        restApp.restClient.getAllSubjects(),
        "Getting all subjects should return empty"
    );

    try {
      TestUtils.registerAndVerifySchema(restApp.restClient, TestUtils.getBadSchema(), 1, subject1);
      fail("Registering bad schema should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.INVALID_SCHEMA_ERROR_CODE,
          rce.getErrorCode(),
          "Invalid schema"
      );
    }

    try {
      TestUtils.registerAndVerifySchema(restApp.restClient,
          TestUtils.getRandomCanonicalAvroString(1).get(0),
          ImmutableList.of(new SchemaReference("bad", "bad", 100)), 1, subject1);
      fail("Registering bad reference should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.INVALID_SCHEMA_ERROR_CODE,
          rce.getErrorCode(),
          "Invalid schema"
      );
    }

    // test getAllSubjects with existing data
    assertEquals(
        allSubjects,
        restApp.restClient.getAllSubjects(),
        "Getting all subjects should match all registered subjects"
    );
  }

  @Test
  public void testLookUpSchemaUnderNonExistentSubject() throws Exception {
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    try {
      restApp.restClient.lookUpSubjectVersion(schema, "non-existent-subject");
      fail("Looking up schema under missing subject should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " (subject not found)");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Subject not found"
      );
    }
  }

  @Test
  public void testLookUpNonExistentSchemaUnderSubject() throws Exception {
    String subject = "test";
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    restApp.restClient.updateCompatibility(CompatibilityLevel.NONE.name, subject);

    try {
      restApp.restClient.lookUpSubjectVersion(schemas.get(1), subject);
      fail("Looking up missing schema under subject should fail with "
           + Errors.SCHEMA_NOT_FOUND_ERROR_CODE
           + " (schema not found)");
    } catch (RestClientException rce) {
      assertEquals(Errors.SCHEMA_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
  }

  @Test
  public void testGetSubjectsAssociatedWithSchemaId() throws Exception {
    String subject1 = "testTopic1";
    String subject2 = "testTopic2";

    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject1);
    TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject2);

    List<String> associatedSubjects = restApp.restClient.getAllSubjectsById(1);
    assertEquals(associatedSubjects.size(), 2);
    assertEquals(Arrays.asList(subject1, subject2), associatedSubjects);

    assertEquals(
        (Integer) 1, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject2, "1"),
        "Deleting Schema Version Success"
    );

    associatedSubjects = restApp.restClient.getAllSubjectsById(1);
    assertEquals(associatedSubjects.size(), 1);
    assertEquals(Collections.singletonList(subject1), associatedSubjects);

    associatedSubjects = restApp.restClient.getAllSubjectsByIdWithPagination(
            RestService.DEFAULT_REQUEST_PROPERTIES, 1, null, false, 1, 1);
    assertEquals(associatedSubjects.size(), 0);
    assertEquals(Collections.emptyList(), associatedSubjects);

    associatedSubjects = restApp.restClient.getAllSubjectsById(
        RestService.DEFAULT_REQUEST_PROPERTIES, 1, null, true);
    assertEquals(associatedSubjects.size(), 2);
    assertEquals(Arrays.asList(subject1, subject2), associatedSubjects);

    associatedSubjects = restApp.restClient.getAllSubjectsByIdWithPagination(
            RestService.DEFAULT_REQUEST_PROPERTIES, 1, null, true, 1, 0);
    assertEquals(associatedSubjects.size(), 1);
    assertEquals(Collections.singletonList(subject1), associatedSubjects);

  }

  @Test
  public void testGetSubjectsAssociatedWithNotFoundSchemaId() throws Exception {
    try {
      restApp.restClient.getAllSubjectsById(1);
      fail("Getting all subjects associated with id 1 should fail with "
            + Errors.SCHEMA_NOT_FOUND_ERROR_CODE + " (schema not found)");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SCHEMA_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Should get a 404 status for non-existing schema"
      );
    }
  }

  @Test
  public void testGetVersionsAssociatedWithSchemaId() throws Exception {
    String subject1 = "testTopic1";
    String subject2 = "testTopic2";

    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject1);
    TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject2);

    List<SubjectVersion> associatedSubjects = restApp.restClient.getAllVersionsById(1);
    assertEquals(associatedSubjects.size(), 2);
    assertTrue(associatedSubjects.contains(new SubjectVersion(subject1, 1)));
    assertTrue(associatedSubjects.contains(new SubjectVersion(subject2, 1)));

    assertEquals(
        (Integer) 1, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject2, "1"),
        "Deleting Schema Version Success"
    );

    associatedSubjects = restApp.restClient.getAllVersionsById(1);
    assertEquals(associatedSubjects.size(), 1);
    assertTrue(associatedSubjects.contains(new SubjectVersion(subject1, 1)));

    associatedSubjects = restApp.restClient.getAllVersionsByIdWithPagination(
            RestService.DEFAULT_REQUEST_PROPERTIES, 1, null, false, 1, 1);
    assertEquals(associatedSubjects.size(), 0);

    associatedSubjects = restApp.restClient.getAllVersionsById(
        RestService.DEFAULT_REQUEST_PROPERTIES, 1, null, true);
    assertEquals(associatedSubjects.size(), 2);
    assertTrue(associatedSubjects.contains(new SubjectVersion(subject1, 1)));
    assertTrue(associatedSubjects.contains(new SubjectVersion(subject2, 1)));

    associatedSubjects = restApp.restClient.getAllVersionsByIdWithPagination(
            RestService.DEFAULT_REQUEST_PROPERTIES, 1, null, true, 1, 1);
    assertEquals(associatedSubjects.size(), 1);
    assertTrue(associatedSubjects.contains(new SubjectVersion(subject2, 1)));
  }

  @Test
  public void testCompatibilityNonExistentSubject() throws Exception {
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    boolean result = restApp.restClient.testCompatibility(schema, "non-existent-subject", "latest")
                                       .isEmpty();
    assertTrue(result, "Compatibility succeeds");

    result = restApp.restClient.testCompatibility(schema, "non-existent-subject", null)
        .isEmpty();
    assertTrue(result, "Compatibility succeeds");
  }

  @Test
  public void testCompatibilityNonExistentVersion() throws Exception {
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject);
    try {
      restApp.restClient.testCompatibility(schema, subject, "100");
      fail("Testing compatibility for missing version should fail with "
           + Errors.VERSION_NOT_FOUND_ERROR_CODE
           + " (version not found)");
    } catch (RestClientException rce) {
      assertEquals(Errors.VERSION_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
  }

  @Test
  public void testCompatibilityInvalidVersion() throws Exception {
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject);
    try {
      restApp.restClient.testCompatibility(schema, subject, "earliest");
      fail("Testing compatibility for invalid version should fail with "
           + RestInvalidVersionException.ERROR_CODE
           + " (version not found)");
    } catch (RestClientException rce) {
      assertEquals(RestInvalidVersionException.ERROR_CODE, rce.getErrorCode());
    }
  }

  @Test
  public void testGetConfigNonExistentSubject() throws Exception {
    try {
      restApp.restClient.getConfig("non-existent-subject");
      fail("Getting the configuration of a missing subject should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " error code (subject not found)");
    } catch (RestClientException rce) {
      assertEquals(Errors.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE,
                   rce.getErrorCode());
    }
  }

  @Test
  public void testCanonicalization() throws Exception {
    // schema string with extra white space
    String schema = "{   \"type\":   \"string\"}";
    String subject = "test";
    assertEquals(
        1,
        restApp.restClient.registerSchema(schema, subject),
        "Registering a new schema should succeed"
    );

    assertEquals(
        1,
        restApp.restClient.registerSchema(schema, subject),
        "Registering the same schema should get back the same id"
    );

    assertEquals(
        1,
        restApp.restClient.lookUpSubjectVersion(schema, subject)
            .getId().intValue(),
        "Lookup the same schema should get back the same id"
    );
  }

  @Test
  public void testDeleteSchemaVersionBasic() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals(
        (Integer) 2, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2"),
        "Deleting Schema Version Success"
    );

    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(subject));

    Schema schema = restApp.restClient.getVersion(subject, 2, true);
    assertEquals((Integer) 2, schema.getVersion());
    assertTrue(schema.getDeleted());

    try {
      restApp.restClient.getVersion(subject, 2);
      fail(String.format("Getting Version %s for subject %s should fail with %s", "2", subject,
                         Errors.VERSION_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals(
          Errors.VERSION_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Version not found"
      );
    }
    try {
      RegisterSchemaRequest request = new RegisterSchemaRequest();
      request.setSchema(schemas.get(1));
      restApp.restClient.lookUpSubjectVersion(
          RestService.DEFAULT_REQUEST_PROPERTIES, request, subject, false, false);
      fail(String.format("Lookup Subject Version %s for subject %s should fail with %s", "2",
                         subject,
                         Errors.SCHEMA_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SCHEMA_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Schema not found"
      );
    }

    assertEquals(
        (Integer) 1, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "latest"),
        "Deleting Schema Version Success"
    );
    try {
      List<Integer> versions = restApp.restClient.getAllVersions(subject);
      fail("Getting all versions from non-existing subject1 should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " (subject not found). Got " + versions);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Should get a 404 status for non-existing subject"
      );
    }

    //re-register twice and versions should be same
    for (int i = 0; i < 2; i++) {
      TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
      assertEquals(Collections.singletonList(3), restApp.restClient.getAllVersions(subject));
    }

  }

  @Test
  public void testDeleteSchemaVersionPermanent() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    //permanent delete without soft delete first
    try {
      restApp.restClient
              .deleteSchemaVersion
                      (RestService.DEFAULT_REQUEST_PROPERTIES, subject,
                              "2",
                              true);
      fail("Permanent deleting first time should throw schemaVersionNotSoftDeletedException");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SCHEMAVERSION_NOT_SOFT_DELETED_ERROR_CODE,
          rce.getErrorCode(),
          "Schema version must be soft deleted first"
      );
    }

    //soft delete
    assertEquals(
        (Integer) 2, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2"),
        "Deleting Schema Version Success"
    );

    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(subject));
    assertEquals(Arrays.asList(1,2), restApp.restClient.getAllVersions(
            RestService.DEFAULT_REQUEST_PROPERTIES,
            subject, true));
    // test with pagination
    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersionsWithPagination(
            RestService.DEFAULT_REQUEST_PROPERTIES,
            subject, true, 0, 1));
    //soft delete again
    try {
      restApp.restClient
              .deleteSchemaVersion
                      (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2");
      fail("Soft deleting second time should throw schemaVersionSoftDeletedException");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SCHEMAVERSION_SOFT_DELETED_ERROR_CODE,
          rce.getErrorCode(),
          "Schema version already soft deleted"
      );
    }

    try {
      restApp.restClient.getVersion(subject, 2);
      fail(String.format("Getting Version %s for subject %s should fail with %s", "2", subject,
              Errors.VERSION_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals(
          Errors.VERSION_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Version not found"
      );
    }

      Schema schema = restApp.restClient.getVersion(subject, 2, true);
      assertEquals((Integer) 2, schema.getVersion());

    try {
      RegisterSchemaRequest request = new RegisterSchemaRequest();
      request.setSchema(schemas.get(1));
      restApp.restClient.lookUpSubjectVersion(
          RestService.DEFAULT_REQUEST_PROPERTIES, request, subject, false, false);
      fail(String.format("Lookup Subject Version %s for subject %s should fail with %s", "2",
              subject,
              Errors.SCHEMA_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals(Errors.SCHEMA_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
    // permanent delete
    assertEquals(
        (Integer) 2, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2", true),
        "Deleting Schema Version Success"
    );
    // GET after permanent delete should give exception
    try {
      restApp.restClient.getVersion(subject, 2, true);
      fail(String.format("Getting Version %s for subject %s should fail with %s", "2", subject,
              Errors.VERSION_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals(Errors.VERSION_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
    //permanent delete again
    try {
      restApp.restClient.deleteSchemaVersion
                      (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2", true);
      fail(String.format("Getting Version %s for subject %s should fail with %s", "2", subject,
              Errors.VERSION_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals(Errors.VERSION_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }

    assertEquals(
        (Integer) 1, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "latest"),
        "Deleting Schema Version Success"
    );

    try {
      List<Integer> versions = restApp.restClient.getAllVersions(subject);
      fail("Getting all versions from non-existing subject1 should fail with "
              + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
              + " (subject not found). Got " + versions);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Should get a 404 status for non-existing subject"
      );
    }

    //re-register twice and versions should be same
    //after permanent delete of 2, the new version coming up will be 2
    for (int i = 0; i < 2; i++) {
      TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
      assertEquals(Collections.singletonList(2), restApp.restClient.getAllVersions(subject));
    }

  }

  @Test
  public void testDeleteSchemaVersionInvalidSubject() throws Exception {
    try {
      String subject = "test";
      restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1");
      fail("Deleting a non existent subject version should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " error code (subject not found)");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Subject not found"
      );
    }
  }

  @Test
  public void testDeleteLatestVersion() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(3);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals(
        (Integer) 2, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "latest"),
        "Deleting Schema Version Success"
    );

    Schema schema = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemas.get(0), schema.getSchema());

    assertEquals(
        (Integer) 1, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "latest"),
        "Deleting Schema Version Success"
    );
    try {
      restApp.restClient.getLatestVersion(subject);
      fail("Getting latest versions from non-existing subject should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " (subject not found).");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_NOT_FOUND_ERROR_CODE, rce.getErrorCode(),
          "Should get a 404 status for non-existing subject"
      );
    }

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(2), 3, subject);
    assertEquals(
        schemas.get(2),
        restApp.restClient.getLatestVersion(subject).getSchema(),
        "Latest version available after subject re-registration"
    );
  }

  @Test
  public void testGetLatestVersionNonExistentSubject() throws Exception {
    String subject = "non_existent_subject";

    try {
      restApp.restClient.getLatestVersion(subject);
      fail("Getting latest versions from non-existing subject should fail with "
              + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
              + " (subject not found).");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Should get a 404 status for non-existing subject"
      );
    }
  }

  @Test
  public void testGetLatestVersionDeleteOlder() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals(schemas.get(1), restApp.restClient.getLatestVersion(subject).getSchema());

    assertEquals(
        (Integer) 1, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1"),
        "Deleting Schema Older Version Success"
    );
    assertEquals(
        schemas.get(1),
        restApp.restClient.getLatestVersion(subject).getSchema(),
        "Latest Version Schema Still Same"
    );
  }

  @Test
  public void testDeleteInvalidVersion() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(1);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    try {
      restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.VERSION_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Should get a 404 status for non-existing subject version"
      );
    }

  }

  @Test
  public void testDeleteWithLookup() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);
    assertEquals(
        (Integer) 1, restApp.restClient
            .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1"),
        "Deleting Schema Version Success"
    );
    try {
      restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, false);
      fail(String.format("Lookup Subject Version %s for subject %s should fail with %s", "2",
                         subject,
                         Errors.SCHEMA_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals(Errors.SCHEMA_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
    //verify deleted schema
    Schema schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, true);
    assertEquals((Integer) 1, schema.getVersion());

    //re-register schema again and verify we get latest version
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, true);
    assertEquals((Integer) 3, schema.getVersion());
    schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, false);
    assertEquals((Integer) 3, schema.getVersion());
    schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject);
    assertEquals((Integer) 3, schema.getVersion());
  }

  @Test
  public void testIncompatibleSchemaLookupBySubjectAfterDelete() throws Exception {
    String subject = "testSubject";

    // Make two incompatible schemas - field 'g' has different types
    String schema1String = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":"
                           + "\"f" + "\"}]}";
    String schema1 = AvroUtils.parseSchema(schema1String).canonicalString();

    String wrongSchema2String = "{\"type\":\"record\","
                                + "\"name\":\"myrecord\","
                                + "\"fields\":"
                                + "[{\"type\":\"string\",\"name\":"
                                + "\"f" + "\"},"
                                + "{\"type\":\"string\",\"name\":"
                                + "\"g\" , \"default\":\"d\"}"
                                + "]}";
    String wrongSchema2 = AvroUtils.parseSchema(wrongSchema2String).canonicalString();

    String correctSchema2String = "{\"type\":\"record\","
                                  + "\"name\":\"myrecord\","
                                  + "\"fields\":"
                                  + "[{\"type\":\"string\",\"name\":"
                                  + "\"f" + "\"},"
                                  + "{\"type\":\"int\",\"name\":"
                                  + "\"g\" , \"default\":0}"
                                  + "]}";
    String correctSchema2 = AvroUtils.parseSchema(correctSchema2String).canonicalString();
    // ensure registering incompatible schemas will raise an error
    restApp.restClient.updateCompatibility(
        CompatibilityLevel.BACKWARD.name, subject);

    // test that compatibility check for incompatible schema returns false and the appropriate
    // error response from Avro
    restApp.restClient.registerSchema(schema1, subject);

    boolean isCompatible = restApp.restClient.testCompatibility(wrongSchema2, subject,
                                                                "latest").isEmpty();
    assertTrue(isCompatible, "Schema should be compatible with specified version");

    restApp.restClient.registerSchema(wrongSchema2, subject);

    isCompatible = restApp.restClient.testCompatibility(correctSchema2, subject,
                                                        "latest").isEmpty();
    assertFalse(isCompatible, "Schema should be incompatible with specified version");
    try {
      restApp.restClient.registerSchema(correctSchema2, subject);
      fail("Schema should be Incompatible");
    } catch (RestClientException rce) {
      assertEquals(Errors.INCOMPATIBLE_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }

    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "latest");
    isCompatible = restApp.restClient.testCompatibility(correctSchema2, subject,
                                                        "latest").isEmpty();
    assertTrue(isCompatible, "Schema should be compatible with specified version");

    restApp.restClient.registerSchema(correctSchema2, subject);

    assertEquals(
        (Integer) 3,
        restApp.restClient.lookUpSubjectVersion(correctSchema2String, subject).getVersion(),
        "Version is same"
    );

  }

  @Test
  public void testSubjectCompatibilityAfterDeletingAllVersions() throws Exception {
    String subject = "testSubject";

    String schema1String = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":"
                           + "\"f" + "\"}]}";
    String schema1 = AvroUtils.parseSchema(schema1String).canonicalString();

    String schema2String = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":"
                           + "\"f" + "\"},"
                           + "{\"type\":\"string\",\"name\":"
                           + "\"g\" , \"default\":\"d\"}"
                           + "]}";
    String schema2 = AvroUtils.parseSchema(schema2String).canonicalString();
    restApp.restClient.updateCompatibility(
        CompatibilityLevel.FULL.name, null);
    restApp.restClient.updateCompatibility(
        CompatibilityLevel.BACKWARD.name, subject);

    restApp.restClient.registerSchema(schema1, subject);
    restApp.restClient.registerSchema(schema2, subject);

    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1");
    assertEquals(CompatibilityLevel.BACKWARD.name, restApp
        .restClient.getConfig(subject).getCompatibilityLevel(), "Compatibility Level Exists");
    assertEquals(CompatibilityLevel.FULL.name, restApp
        .restClient.getConfig(null).getCompatibilityLevel(), "Top Compatibility Level Exists");
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2");
    try {
      restApp.restClient.getConfig(subject);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE, rce.getErrorCode(),
          "Compatibility Level doesn't exist"
      );
    }
    assertEquals(
        CompatibilityLevel.FULL.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Top Compatibility Level Exists"
    );

  }

  @Test
  public void testListSubjects() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject1 = "test1";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject1);
    String subject2 = "test2";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject2);
    List<String> expectedResponse = new ArrayList<>();
    expectedResponse.add(subject1);
    expectedResponse.add(subject2);
    assertEquals(
        expectedResponse,
        restApp.restClient.getAllSubjects(),
        "Current Subjects"
    );
    List<Integer> deletedResponse = new ArrayList<>();
    deletedResponse.add(1);
    assertEquals(
        deletedResponse,
        restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject2),
        "Versions Deleted Match"
    );

    expectedResponse = new ArrayList<>();
    expectedResponse.add(subject1);
    assertEquals(
        expectedResponse,
        restApp.restClient.getAllSubjects(),
        "Current Subjects"
    );

    expectedResponse = new ArrayList<>();
    expectedResponse.add(subject1);
    expectedResponse.add(subject2);
    assertEquals(
        expectedResponse,
        restApp.restClient.getAllSubjects(true),
        "Current Subjects"
    );

    assertEquals(
        deletedResponse,
        restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES,
                    subject2, true),
        "Versions Deleted Match"
    );

    expectedResponse = new ArrayList<>();
    expectedResponse.add(subject1);
    assertEquals(
        expectedResponse,
        restApp.restClient.getAllSubjects(),
        "Current Subjects"
    );
  }

  @Test
  public void testListSoftDeletedSubjectsAndSchemas() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(3);
    String subject1 = "test1";
    String subject2 = "test2";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject1);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject1);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(2), 3, subject2);

    assertEquals((Integer) 1,
        restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject1, "1"));
    assertEquals((Integer) 1,
        restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject2, "1"));

    assertEquals(
        Collections.singletonList(2), restApp.restClient.getAllVersions(subject1),
        "List All Versions Match"
    );
    assertEquals(
        Arrays.asList(1, 2),
        restApp.restClient.getAllVersions(RestService.DEFAULT_REQUEST_PROPERTIES, subject1, true),
        "List All Versions Include deleted Match"
    );
    assertEquals(
        Collections.singletonList(1),
        restApp.restClient.getDeletedOnlyVersions(subject1),
        "List Deleted Versions Match"
    );

    assertEquals(
        Collections.singletonList(subject1),
        restApp.restClient.getAllSubjects(),
        "List All Subjects Match"
    );
    assertEquals(
        Arrays.asList(subject1, subject2),
        restApp.restClient.getAllSubjects(true),
        "List All Subjects Include deleted Match"
    );
    assertEquals(
        Collections.singletonList(subject2),
        restApp.restClient.getDeletedOnlySubjects(null),
        "List Deleted Only Subjects Match"
    );
  }

  @Test
  public void testDeleteSubjectBasic() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);
    List<Integer> expectedResponse = new ArrayList<>();
    expectedResponse.add(1);
    expectedResponse.add(2);
    assertEquals(
        expectedResponse,
        restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject),
        "Versions Deleted Match"
    );
    try {
      restApp.restClient.getLatestVersion(subject);
      fail(String.format("Subject %s should not be found", subject));
    } catch (RestClientException rce) {
      assertEquals(Errors.SUBJECT_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }

  }

  @Test
  public void testDeleteSubjectException() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);
    List<Integer> expectedResponse = new ArrayList<>();
    expectedResponse.add(1);
    expectedResponse.add(2);
    assertEquals(
        expectedResponse,
        restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject),
        "Versions Deleted Match"
    );

    Schema schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, true );
    assertEquals(1, (long)schema.getVersion());
    schema = restApp.restClient.lookUpSubjectVersion(schemas.get(1), subject, true );
    assertEquals(2, (long)schema.getVersion());

    try {
      restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject);
      fail(String.format("Subject %s should not be found", subject));
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_SOFT_DELETED_ERROR_CODE, rce.getErrorCode(),
          "Subject exists in soft deleted format."
      );
    }
  }


  @Test
  public void testDeleteSubjectPermanent() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);
    List<Integer> expectedResponse = new ArrayList<>();
    expectedResponse.add(1);
    expectedResponse.add(2);

    try {
      restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES,
              subject,
              true);
      fail("Delete permanent should not succeed");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_NOT_SOFT_DELETED_ERROR_CODE,
          rce.getErrorCode(),
          "Subject was not deleted first before permanent delete"
      );
    }

    assertEquals(
        expectedResponse,
        restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject),
        "Versions Deleted Match"
    );

    Schema schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, true );
    assertEquals(1, (long)schema.getVersion());
    schema = restApp.restClient.lookUpSubjectVersion(schemas.get(1), subject, true );
    assertEquals(2, (long)schema.getVersion());

    assertEquals(
        expectedResponse,
        restApp.restClient
            .deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject, true),
        "Versions Deleted Match"
    );
    for (Integer i : expectedResponse) {
      try {
        restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, false);
        fail(String.format("Subject %s should not be found", subject));
      } catch (RestClientException rce) {
        assertEquals(Errors.SUBJECT_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
      }

      try {
        restApp.restClient.lookUpSubjectVersion(schemas.get(i-1), subject, true);
        fail(String.format("Subject %s should not be found", subject));
      } catch (RestClientException rce) {
        assertEquals(Errors.SUBJECT_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
      }
    }
  }

  @Test
  public void testDeleteSubjectAndRegister() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);
    restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject);

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals(
        Arrays.asList(3, 4),
        restApp.restClient.getAllVersions(subject),
        "Versions match"
    );
    try {
      restApp.restClient.getVersion(subject, 1);
      fail("Version 1 should not be found");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.VERSION_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Version not found"
      );
    }
  }

  @Test
  public void testSubjectCompatibilityAfterDeletingSubject() throws Exception {
    String subject = "testSubject";

    String schema1String = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":"
                           + "\"f" + "\"}]}";
    String schema1 = AvroUtils.parseSchema(schema1String).canonicalString();

    String schema2String = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":"
                           + "\"f" + "\"},"
                           + "{\"type\":\"string\",\"name\":"
                           + "\"g\" , \"default\":\"d\"}"
                           + "]}";
    String schema2 = AvroUtils.parseSchema(schema2String).canonicalString();
    restApp.restClient.updateCompatibility(
        CompatibilityLevel.FULL.name, null);
    restApp.restClient.updateCompatibility(
        CompatibilityLevel.BACKWARD.name, subject);

    restApp.restClient.registerSchema(schema1, subject);
    restApp.restClient.registerSchema(schema2, subject);

    restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject);
    try {
      restApp.restClient.getConfig(subject);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE,
          rce.getErrorCode(),
          "Compatibility Level doesn't exist"
      );
    }
    assertEquals(
        CompatibilityLevel.FULL.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Top Compatibility Level Exists"
    );

  }

  @Test
  public void testGetClusterId() throws Exception {
    try {
      ServerClusterId serverClusterId = restApp.restClient.getClusterId();
      assertEquals("", serverClusterId.getId());
      assertEquals(Collections.emptyList(), serverClusterId.getScope().get("path"));
      assertNotNull(serverClusterId.getScope().get("clusters"));
    } catch (RestClientException rce) {
      fail("The operation shouldn't have failed");
    }
  }

  @Test
  public void testGetSchemaRegistryServerVersion() throws Exception {
      SchemaRegistryServerVersion srVersion = restApp.restClient.getSchemaRegistryServerVersion();
      assertEquals(AppInfoParser.getVersion(), srVersion.getVersion());
      assertEquals(AppInfoParser.getCommitId(), srVersion.getCommitId());
  }

  @Test
  public void testHttpResponseHeaders() throws Exception {
    String baseUrl = restApp.restClient.getBaseUrls().current();
    String requestUrl = buildRequestUrl(baseUrl, "/v1/metadata/id");
    HttpURLConnection connection = null;
    try {
      URL url = new URL(requestUrl);
      connection = (HttpURLConnection) url.openConnection();
      connection.setConnectTimeout(60000);
      connection.setReadTimeout(60000);
      connection.setRequestMethod("GET");
      connection.setDoInput(true);

      Map<String,List<String>> httpResponseHeaders = connection.getHeaderFields();
      assertNotNull(matchHeaderValue(httpResponseHeaders,
              "X-XSS-Protection", "1; mode=block"));
      assertNotNull(matchHeaderValue(httpResponseHeaders,
              "Cache-Control",
              "no-cache, no-store, must-revalidate"));
      assertNull(matchHeaderValue(httpResponseHeaders,
              "Strict-Transport-Security", "max-age=31536000"));
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  @Test
  public void testGlobalMode() throws Exception {
    // test default globalMode
    assertEquals("READWRITE", restApp.restClient.getMode().getMode());

    //test subjectMode override globalMode
    String subject = "testSubject";
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject);

    // default context for error message
    String context = ".";

    try {
      restApp.restClient.getMode(subject).getMode();
      fail(String.format("Subject %s should not be found when there's no mode override", subject));
    } catch (RestClientException e) {
      assertEquals(
          Errors.SUBJECT_LEVEL_MODE_NOT_CONFIGURED_ERROR_CODE, e.getErrorCode(),
          String.format("No mode override for subject %s, get mode should return not configured", subject)
      );
    }
    assertEquals("READWRITE", restApp.restClient.getMode(subject, true).getMode());

    restApp.restClient.setMode("READONLY", null);
    restApp.restClient.setMode("READWRITE", subject);
    assertEquals("READWRITE", restApp.restClient.getMode(subject).getMode());

    //test delete subject mode
    restApp.restClient.deleteSubjectMode(subject);
    assertEquals("READONLY", restApp.restClient.getMode(subject, true).getMode());

    //test READONLY_OVERRIDE globalMode override subjectMode
    restApp.restClient.setMode("READONLY_OVERRIDE", null);
    assertEquals("READONLY_OVERRIDE", restApp.restClient.getMode(subject).getMode());
    try {
      restApp.restClient.registerSchema(schema, "testSubject2");
      fail(String.format("Subject %s in context %s is in read-only mode", "testSubject2", context));
    } catch (RestClientException rce) {
      assertEquals(
          Errors.OPERATION_NOT_PERMITTED_ERROR_CODE, rce.getErrorCode(),
          String.format("Subject %s in context" 
          +" %s is in read-only mode", "testSubject2", context)
      );
    }
  }

  @Test
  public void testRegisterWithAndWithoutMetadata() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Map<String, String> properties = Collections.singletonMap("application.version", "2");
    Metadata metadata = new Metadata(null, properties, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setMetadata(metadata);

    int id = restApp.restClient.registerSchema(request1, subject, false).getId();

    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema1);
    int id2 = restApp.restClient.registerSchema(request2, subject, false).getId();
    assertEquals(id, id2);
  }

  @Test
  public void testRegisterDropsRuleSet() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Rule r1 = new Rule("foo", null, null, RuleMode.READ, "ENCRYPT", null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    RuleSet ruleSet = new RuleSet(null, rules);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setRuleSet(ruleSet);
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId(),
        "Registering should succeed"
    );

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema1, subject);
    assertNull(schemaString.getRuleSet());
  }

  @Test
  public void testRegisterSchemaWithReservedFields() throws RestClientException, IOException {
    String subject0 = "testSubject0";
    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
                                                     + "\"name\":\"myrecord\","
                                                     + "\"fields\":"
                                                     + "[{\"type\":\"string\",\"name\":"
                                                     + "\"f" + "\"},"
                                                     + "{\"type\":\"string\",\"name\":"
                                                     + "\"g\" , \"default\":\"d\"}"
                                                     + "]}");
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(Objects.requireNonNull(schema1));
    request1.setMetadata(new Metadata(Collections.emptyMap(),
        Collections.singletonMap(ParsedSchema.RESERVED, "f"),
        Collections.emptySet()));

    // global validateFields = true
    ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest();
    configUpdateRequest.setCompatibilityLevel(BACKWARD.name());
    configUpdateRequest.setValidateFields(true);
    assertEquals(
        configUpdateRequest,
        restApp.restClient.updateConfig(configUpdateRequest, null),
        "Updating config should succeed"
    );
    assertThrows(
        RestClientException.class,
        () -> restApp.restClient.registerSchema(request1, subject0, false),
        "Fail registering subject0 because of global validateFields"
    );

    // global validateFields = false
    configUpdateRequest.setValidateFields(false);
    assertEquals(
        configUpdateRequest,
        restApp.restClient.updateConfig(configUpdateRequest, null),
        "Updating config should succeed"
    );
    assertEquals(
        1,
        restApp.restClient.registerSchema(request1, subject0, false).getId(),
        "Should register despite reserved fields"
    );

    // global validateFields = false; testSubject1 validateFields = true
    String subject1 = "testSubject1";
    configUpdateRequest.setValidateFields(true);
    assertEquals(
        configUpdateRequest,
        restApp.restClient.updateConfig(configUpdateRequest, subject1),
        "Updating config should succeed"
    );
    assertThrows(
        RestClientException.class,
        () -> restApp.restClient.registerSchema(request1, subject1, false),
        "Fail registering subject1 because of subject1 validateFields"
    );
    String subject2 = "testSubject2";
    assertEquals(
        1,
        restApp.restClient.registerSchema(request1, subject2, false).getId(),
        "Should register despite reserved fields"
    );

    // global validateFields = true; testSubject1 validateFields = false
    configUpdateRequest.setValidateFields(true);
    assertEquals(
        configUpdateRequest,
        restApp.restClient.updateConfig(configUpdateRequest, null),
        "Updating config should succeed"
    );
    configUpdateRequest.setValidateFields(false);
    assertEquals(
        configUpdateRequest,
        restApp.restClient.updateConfig(configUpdateRequest, subject1),
        "Updating config should succeed"
    );
    assertEquals(
        1,
        restApp.restClient.registerSchema(request1, subject1, false).getId(),
        "Should register despite reserved fields"
    );
    String subject3 = "testSubject3";
    assertThrows(
        RestClientException.class,
        () -> restApp.restClient.registerSchema(request1, subject3, false),
        "Fail registering because of subject3 validateFields"
    );

    // remove reserved fields for subject1
    request1.setMetadata(new Metadata(Collections.emptyMap(),
        Collections.singletonMap(ParsedSchema.RESERVED, "g"),
        Collections.emptySet()));
    assertEquals(
        2,
        restApp.restClient.registerSchema(request1, subject1, false).getId(),
        "Should register despite removal of reserved fields"
    );

    // remove reserved fields for subject0
    schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
                                        + "\"name\":\"myrecord\","
                                        + "\"fields\":"
                                        + "["
                                        + "{\"type\":\"string\",\"name\":"
                                        + "\"g\" , \"default\":\"d\"}"
                                        + "]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(Objects.requireNonNull(schema1));
    request2.setMetadata(new Metadata(Collections.emptyMap(),
        Collections.singletonMap(ParsedSchema.RESERVED, "g"),
        Collections.emptySet()));
    assertThrows(
        RestClientException.class,
        () -> restApp.restClient.registerSchema(request2, subject0, false),
        "Fail registering because of removal of reserved fields"
    );
  }

  @Test
  public void testInvalidSchema() {
    assertThrows(InvalidSchemaException.class, () ->
        ((KafkaSchemaRegistry) restApp.schemaRegistry()).parseSchema(null));
  }

  @Test
  public void testConfluentVersion() throws Exception {
    String subject = "test";
    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"name\":\"f1\",\"type\":\"string\"}]}";

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchemaType(AvroSchema.TYPE);
    request.setSchema(schemaString);
    // Register with null version
    registerAndVerifySchema(restApp.restClient, request, 1, subject);

    Schema result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 1, result.getVersion());
    assertNull(result.getMetadata());

    // Register schema with version -1
    request.setVersion(-1);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 1, subject);

    // Register schema with version 2
    request.setVersion(2);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 2, subject);

    // Register schema with version -1
    request.setVersion(-1);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 2, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    // Lookup schema with null version
    request.setVersion(null);
    request.setMetadata(null);
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 1, result.getVersion());
    assertNull(result.getMetadata());

    // Lookup schema with confluent:version 1 (should return one without metadata)
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "1"), null));
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 1, result.getVersion());
    assertNull(result.getMetadata());

    // Lookup schema with confluent:version 2
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "2"), null));
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    // Delete version 1
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1");

    // Lookup schema with null version
    request.setVersion(null);
    request.setMetadata(null);
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with null version
    registerAndVerifySchema(restApp.restClient, request, 2, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with version 3
    request.setVersion(3);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 3, subject);

    // Register schema with version -1
    request.setVersion(-1);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 3, subject);

    // Register schema with version -1
    request.setVersion(-1);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 3, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 3, result.getVersion());
    assertEquals("3", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with version 3
    request.setVersion(3);
    request.setMetadata(null);
    try {
      registerAndVerifySchema(restApp.restClient, request, 3, subject);
      fail("Registering version that is not next version should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }

    // Register schema with version 4
    request.setVersion(4);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 4, subject);

    // Lookup schema with null version
    request.setVersion(null);
    request.setMetadata(null);
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 4, result.getVersion());
    assertEquals("4", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with confluent:version -1
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "-1"), null));
    registerAndVerifySchema(restApp.restClient, request, 5, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 5, result.getVersion());
    assertEquals("5", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with confluent:version 2
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "2"), null));
    registerAndVerifySchema(restApp.restClient, request, 2, subject);

    // Register schema with confluent:version 3
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "3"), null));
    registerAndVerifySchema(restApp.restClient, request, 3, subject);

    // Register schema with confluent:version 0
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "0"), null));
    registerAndVerifySchema(restApp.restClient, request, 6, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 6, result.getVersion());
    assertEquals("6", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with empty metadata
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.emptyMap(), null));
    registerAndVerifySchema(restApp.restClient, request, 6, subject);

    // Register schema with new metadata
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("mykey", "myvalue"), null));
    registerAndVerifySchema(restApp.restClient, request, 7, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 7, result.getVersion());
    assertNull(result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with confluent:version -1
    request.setVersion(-1);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 8, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 8, result.getVersion());
    assertEquals("8", result.getMetadata().getProperties().get("confluent:version"));

    // Lookup schema with new metadata
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("mykey", "myvalue"), null));
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 7, result.getVersion());
    assertNull(result.getMetadata().getProperties().get("confluent:version"));

    // Delete version 7
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "7");

    // Lookup schema with new metadata
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("mykey", "myvalue"), null));
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 8, result.getVersion());
    assertEquals("8", result.getMetadata().getProperties().get("confluent:version"));

    // Use same schema with doc
    schemaString = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"doc\":\"mydoc\","
        + "\"fields\":"
        + "[{\"name\":\"f1\",\"type\":\"string\"}]}";
    request = new RegisterSchemaRequest();
    request.setSchemaType(AvroSchema.TYPE);
    request.setSchema(schemaString);

    // Look up schema with version -1
    request.setVersion(-1);
    request.setMetadata(null);
    try {
      restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
      fail("Looking up version that is not next version should fail with " + Errors.SCHEMA_NOT_FOUND_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(Errors.SCHEMA_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
    }
  }

  public static void registerAndVerifySchema(
      RestService restService,
      RegisterSchemaRequest request,
      int expectedId,
      String subject
  ) throws IOException, RestClientException {
    RegisterSchemaResponse response = restService.registerSchema(request, subject, false);
    assertNotNull(response.getVersion());

    int registeredId = response.getId();
    assertEquals(
        (long) expectedId,
        (long) registeredId,
        "Registering a new schema should succeed"
    );
    assertEquals(
        request.getSchema().trim(),
        restService.getId(expectedId).getSchemaString().trim(),
        "Registered schema should be found"
    );
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties schemaRegistryProps = new Properties();
    schemaRegistryProps.put("response.http.headers.config",
            "add X-XSS-Protection: 1; mode=block, \"add Cache-Control: no-cache, no-store, must-revalidate\"");
    schemaRegistryProps.put("schema.providers.avro.validate.defaults", "true");
    return schemaRegistryProps;
  }

  private String matchHeaderValue(Map<String, List<String>> responseHeaders,
                                  String headerName, String expectedHeaderValue) {
    if (responseHeaders.isEmpty() || responseHeaders.get(headerName) == null)
      return null;

    return responseHeaders.get(headerName)
            .stream()
            .filter(value -> expectedHeaderValue.equals(value.trim()))
            .findAny()
            .orElse(null);
  }

  private String buildRequestUrl(String baseUrl, String path) {
    // Join base URL and path, collapsing any duplicate forward slash delimiters
    return baseUrl.replaceFirst("/$", "") + "/" + path.replaceFirst("^/", "");
  }
}

