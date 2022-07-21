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
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryServerVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.ServerClusterId;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidSubjectException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidVersionException;
import io.confluent.kafka.schemaregistry.utils.AppInfoParser;
import io.confluent.kafka.schemaregistry.utils.TestUtils;

import org.apache.avro.Schema.Parser;
import org.junit.Test;

import java.util.*;
import java.net.URL;
import java.net.HttpURLConnection;

import static io.confluent.kafka.schemaregistry.CompatibilityLevel.FORWARD;
import static io.confluent.kafka.schemaregistry.CompatibilityLevel.NONE;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RestApiTest extends ClusterTestHarness {

  public RestApiTest() {
    super(1, true);
  }

  @Test
  public void testBasic() throws Exception {
    String subject1 = "testTopic1";
    String subject2 = "testTopic2";
    int schemasInSubject1 = 10;
    List<Integer> allVersionsInSubject1 = new ArrayList<Integer>();
    List<String> allSchemasInSubject1 = TestUtils.getRandomCanonicalAvroString(schemasInSubject1);
    int schemasInSubject2 = 5;
    List<Integer> allVersionsInSubject2 = new ArrayList<Integer>();
    List<String> allSchemasInSubject2 = TestUtils.getRandomCanonicalAvroString(schemasInSubject2);
    List<String> allSubjects = new ArrayList<String>();

    // test getAllVersions with no existing data
    try {
      restApp.restClient.getAllVersions(subject1);
      fail("Getting all versions from non-existing subject1 should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " (subject not found)");
    } catch (RestClientException rce) {
      assertEquals("Should get a 404 status for non-existing subject",
                   Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
    }

    // test getAllContexts
    assertEquals("Getting all subjects should return default context",
        Collections.singletonList(DEFAULT_CONTEXT),
        restApp.restClient.getAllContexts());

    // test getAllSubjects with no existing data
    assertEquals("Getting all subjects should return empty",
                 allSubjects,
                 restApp.restClient.getAllSubjects());

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
      assertEquals("Re-registering an existing schema should return the existing version",
                   expectedId, foundId);
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
    assertEquals("Getting all versions from subject1 should match all registered versions",
                 allVersionsInSubject1,
                 restApp.restClient.getAllVersions(subject1));
    assertEquals("Getting all versions from subject2 should match all registered versions",
                 allVersionsInSubject2,
                 restApp.restClient.getAllVersions(subject2));

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
                 allSubjects,
                 restApp.restClient.getAllSubjects());

    List<Schema> latestSchemas = restApp.restClient.getSchemas(null, false, true);
    assertEquals("Getting latest schemas should return two schemas",
                 2,
                 latestSchemas.size());
    assertEquals(Integer.valueOf(10), latestSchemas.get(0).getVersion());
    assertEquals(Integer.valueOf(5), latestSchemas.get(1).getVersion());
  }

  @Test
  public void testRegisterSameSchemaOnDifferentSubject() throws Exception {
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    int id1 = restApp.restClient.registerSchema(schema, "subject1");
    int id2 = restApp.restClient.registerSchema(schema, "subject2");
    assertEquals("Registering the same schema under different subjects should return the same id",
                 id1, id2);
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
      assertEquals("Invalid schema", Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }

    try {
      restApp.restClient.registerSchema(schema, subject);
      fail("Registering schema with invalid default should fail with "
          + Errors.INVALID_SCHEMA_ERROR_CODE
          + " (invalid schema)");
    } catch (RestClientException rce) {
      assertEquals("Invalid schema", Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
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
    assertEquals("1st schema registered globally should have id 1", 1,
        id1);

    boolean isCompatible = restApp.restClient.testCompatibility(jsonSchema, "JSON", null, subject, "latest", false).isEmpty();
    assertTrue("Different schema type is allowed when compatibility is NONE", isCompatible);

    int id2 = restApp.restClient.registerSchema(jsonSchema, "JSON", null, subject);
    assertEquals("2nd schema registered globally should have id 2", 2,
        id2);

    isCompatible = restApp.restClient.testCompatibility(protobufSchema, "PROTOBUF", null, subject, "latest", false).isEmpty();
    assertTrue("Different schema type is allowed when compatibility is NONE", isCompatible);

    int id3 = restApp.restClient.registerSchema(protobufSchema, "PROTOBUF", null, subject);
    assertEquals("3rd schema registered globally should have id 3", 3,
        id3);
  }

  @Test
  public void testRegisterDiffContext() throws Exception {
    List<String> avroSchemas = TestUtils.getRandomCanonicalAvroString(2);

    String subject = "testSubject";
    String avroSchema = avroSchemas.get(0);

    int id1 = restApp.restClient.registerSchema(avroSchema, subject);
    assertEquals("1st schema registered in first context should have id 1", 1,
        id1);

    String subject2 = ":.ctx:testSubject";
    String avroSchema2 = avroSchemas.get(1);

    int id2 = restApp.restClient.registerSchema(avroSchema2, subject2);
    assertEquals("2nd schema registered in second context should have id 1", 1,
        id2);

    List<String> subjects = restApp.restClient.getAllSubjects("", false);
    assertEquals(Collections.singletonList(subject), subjects);

    List<Schema> schemas = restApp.restClient.getSchemas(null, false, false);
    assertEquals(avroSchema, schemas.get(0).getSchema());

    List<String> subjects2 = restApp.restClient.getAllSubjects(":.ctx:", false);
    assertEquals(Collections.singletonList(subject2), subjects2);

    List<Schema> schemas2 = restApp.restClient.getSchemas(":.ctx:", false, false);
    assertEquals(avroSchema2, schemas2.get(0).getSchema());
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
      assertEquals("Overwrite schema for the same ID is not permitted.",
          Errors.OPERATION_NOT_PERMITTED_ERROR_CODE, e.getErrorCode());
    }

    try {
      int id1 = restApp.restClient.registerSchema(schema1, "subject1", 1, 1);
      assertEquals(1, id1);
      int id2 = restApp.restClient.registerSchema(schema2, "subject2", 1, 1);
      fail(String.format("Schema2 is registered with id %s, should receive error here", id2));
    } catch (RestClientException e) {
      assertEquals("Overwrite schema for the same ID is not permitted.",
          Errors.OPERATION_NOT_PERMITTED_ERROR_CODE, e.getErrorCode());
    }
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
    assertTrue("First schema registered should be compatible", isCompatible);

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
    assertFalse("Schema should be incompatible with specified version", isCompatible);
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
    assertTrue("Schema is compatible with the latest version", isCompatible);
    isCompatible = restApp.restClient.testCompatibility(schema3, subject, null).isEmpty();
    assertFalse("Schema should be incompatible with FORWARD_TRANSITIVE setting", isCompatible);
    try {
      restApp.restClient.registerSchema(schema3String, subject);
      fail("Schema register should fail since schema is incompatible");
    } catch (RestClientException e) {
      assertEquals("Schema register should fail since schema is incompatible",
          Errors.INCOMPATIBLE_SCHEMA_ERROR_CODE, e.getErrorCode());
    }
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
    assertEquals("1st schema under subject1 should have version 1", 1,
                 versionOfRegisteredSchema1Subject1);
    assertEquals("1st schema registered globally should have id 1", 1,
                 idOfRegisteredSchema1Subject1);

    int idOfRegisteredSchema2Subject1 =
        restApp.restClient.registerSchema(schema2, subject1);
    int versionOfRegisteredSchema2Subject1 =
        restApp.restClient.lookUpSubjectVersion(schema2, subject1).getVersion();
    assertEquals("2nd schema under subject1 should have version 2", 2,
                 versionOfRegisteredSchema2Subject1);
    assertEquals("2nd schema registered globally should have id 2", 2,
                 idOfRegisteredSchema2Subject1);

    int idOfRegisteredSchema2Subject2 =
        restApp.restClient.registerSchema(schema2, subject2);
    int versionOfRegisteredSchema2Subject2 =
        restApp.restClient.lookUpSubjectVersion(schema2, subject2).getVersion();
    assertEquals(
        "2nd schema under subject1 should still have version 1 as the first schema under subject2",
        1,
        versionOfRegisteredSchema2Subject2);
    assertEquals("Since schema is globally registered but not under subject2, id should not change",
                 2,
                 idOfRegisteredSchema2Subject2);
  }

  @Test
  public void testConfigDefaults() throws Exception {
    assertEquals("Default compatibility level should be none for this test instance",
                 NONE.name,
                 restApp.restClient.getConfig(null).getCompatibilityLevel());

    // change it to forward
    restApp.restClient.updateCompatibility(CompatibilityLevel.FORWARD.name, null);

    assertEquals("New compatibility level should be forward for this test instance",
                 FORWARD.name,
                 restApp.restClient.getConfig(null).getCompatibilityLevel());
  }

  @Test
  public void testNonExistentSubjectConfigChange() throws Exception {
    String subject = "testSubject";
    try {
      restApp.restClient.updateCompatibility(CompatibilityLevel.FORWARD.name, subject);
    } catch (RestClientException e) {
      fail("Changing config for an invalid subject should succeed");
    }
    assertEquals("New compatibility level for this subject should be forward",
                 FORWARD.name,
                 restApp.restClient.getConfig(subject).getCompatibilityLevel());
  }

  @Test
  public void testSubjectConfigChange() throws Exception {
    String subject = "testSubject";
    assertEquals("Default compatibility level should be none for this test instance",
                 NONE.name,
                 restApp.restClient.getConfig(null).getCompatibilityLevel());

    // change subject compatibility to forward
    restApp.restClient.updateCompatibility(CompatibilityLevel.FORWARD.name, subject);

    assertEquals("Global compatibility level should remain none for this test instance",
                 NONE.name,
                 restApp.restClient.getConfig(null).getCompatibilityLevel());

    assertEquals("New compatibility level for this subject should be forward",
                 FORWARD.name,
                 restApp.restClient.getConfig(subject).getCompatibilityLevel());

    // delete subject compatibility
    restApp.restClient.deleteSubjectConfig(subject);

    assertEquals("Compatibility level for this subject should be reverted to none",
        NONE.name,
        restApp.restClient
            .getConfig(RestService.DEFAULT_REQUEST_PROPERTIES, subject, true)
            .getCompatibilityLevel());
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
      assertEquals("Should get a 404 status for non-existing id",
                   Errors.SCHEMA_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
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
      assertEquals("Should get a 404 status for non-existing subject",
                   Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
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
      assertEquals("Unregistered subject shouldn't be found in getVersion()",
                   Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
                   e.getErrorCode());
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
      assertEquals("Unregistered version shouldn't be found",
                   Errors.VERSION_NOT_FOUND_ERROR_CODE, e.getErrorCode());
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
      assertEquals("Invalid version shouldn't be found",
                   RestInvalidVersionException.ERROR_CODE,
                   e.getErrorCode());
    }
  }

  @Test
  public void testRegisterInvalidSubject() throws Exception {
    // test invalid subject
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    String subject = "\rbad\nsubject\t";
    try {
      TestUtils.registerAndVerifySchema(restApp.restClient, schema, 1, subject);
      fail("Registering invalid subject should fail with "
          + RestInvalidSubjectException.ERROR_CODE
          + " (invalid subject)");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Invalid subject shouldn't be registered",
                   RestInvalidSubjectException.ERROR_CODE,
                   e.getErrorCode());
    }
  }

  @Test
  public void testGetVersion() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals("Version 1 schema should match",
                 schemas.get(0),
                 restApp.restClient.getVersion(subject, 1).getSchema());

    assertEquals("Version 2 schema should match",
                 schemas.get(1),
                 restApp.restClient.getVersion(subject, 2).getSchema());
    assertEquals("Latest schema should be the same as version 2",
                 schemas.get(1),
                 restApp.restClient.getLatestVersion(subject).getSchema());
  }

  @Test
  public void testGetLatestVersionSchemaOnly() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals("Latest schema should be the same as version 2",
                 schemas.get(1),
                 restApp.restClient.getLatestVersionSchemaOnly(subject));
  }

  @Test
  public void testGetVersionSchemaOnly() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(1);
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);

    assertEquals("Retrieved schema should be the same as version 1",
                schemas.get(0),
                restApp.restClient.getVersionSchemaOnly(subject, 1));
  }

  @Test
  public void testSchemaReferences() throws Exception {
    List<String> schemas = TestUtils.getAvroSchemaWithReferences();
    String subject = "reference";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get(1));
    SchemaReference ref = new SchemaReference("otherns.Subrecord", "reference", 1);
    request.setReferences(Collections.singletonList(ref));
    int registeredId = restApp.restClient.registerSchema(request, "referrer", false);
    assertEquals("Registering a new schema should succeed", 2, registeredId);

    SchemaString schemaString = restApp.restClient.getId(2);
    // the newly registered schema should be immediately readable on the leader
    assertEquals("Registered schema should be found",
        schemas.get(1),
        schemaString.getSchemaString());

    assertEquals("Schema references should be found",
        Collections.singletonList(ref),
        schemaString.getReferences());

    List<Integer> refs = restApp.restClient.getReferencedBy("reference", 1);
    assertEquals(2, refs.get(0).intValue());

    ns.MyRecord myrecord = new ns.MyRecord();
    AvroSchema schema = new AvroSchema(AvroSchemaUtils.getSchema(myrecord));
    // Note that we pass an empty list of refs since SR will perform a deep equality check
    Schema registeredSchema = restApp.restClient.lookUpSubjectVersion(schema.canonicalString(),
            AvroSchema.TYPE, Collections.emptyList(), "referrer", false);
    assertEquals("Registered schema should be found", 2, registeredSchema.getId().intValue());

    try {
      restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
          "reference",
          String.valueOf(1)
      );
      fail("Deleting reference should fail with " + Errors.REFERENCE_EXISTS_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals("Reference found",
          Errors.REFERENCE_EXISTS_ERROR_CODE,
          rce.getErrorCode());
    }

    assertEquals((Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, "referrer", "1"));

    refs = restApp.restClient.getReferencedBy("reference", 1);
    assertTrue(refs.isEmpty());

    assertEquals((Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, "reference", "1"));
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
    int registeredId = restApp.restClient.registerSchema(request, "ref1", false);
    assertEquals("Registering a new schema should succeed", 2, registeredId);

    request = new RegisterSchemaRequest();
    request.setSchema(ref2);
    ref = new SchemaReference("myavro.currencies.Currency", "shared", 1);
    request.setReferences(Collections.singletonList(ref));
    registeredId = restApp.restClient.registerSchema(request, "ref2", false);
    assertEquals("Registering a new schema should succeed", 3, registeredId);

    request = new RegisterSchemaRequest();
    request.setSchema(root);
    SchemaReference r1 = new SchemaReference("myavro.BudgetDecreased", "ref1", 1);
    SchemaReference r2 = new SchemaReference("myavro.BudgetUpdated", "ref2", 1);
    request.setReferences(Arrays.asList(r1, r2));
    registeredId = restApp.restClient.registerSchema(request, "root", false);
    assertEquals("Registering a new schema should succeed", 4, registeredId);

    SchemaString schemaString = restApp.restClient.getId(4);
    // the newly registered schema should be immediately readable on the leader
    assertEquals("Registered schema should be found",
        root,
        schemaString.getSchemaString());

    assertEquals("Schema references should be found",
        Arrays.asList(r1, r2),
        schemaString.getReferences());
  }

  @Test(expected = RestClientException.class)
  public void testSchemaMissingReferences() throws Exception {
    List<String> schemas = TestUtils.getAvroSchemaWithReferences();

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get(1));
    request.setReferences(Collections.emptyList());
    restApp.restClient.registerSchema(request, "referrer", false);
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
        restApp.restClient.registerSchema(registerRequest, subject1, true);
    RegisterSchemaRequest lookUpRequest = new RegisterSchemaRequest();
    lookUpRequest.setSchema(schemaString2);
    lookUpRequest.setReferences(Arrays.asList(ref2, ref1));
    int versionOfRegisteredSchema1Subject1 =
        restApp.restClient.lookUpSubjectVersion(lookUpRequest, subject1, true, false).getVersion();
    assertEquals("1st schema under subject1 should have version 1", 1,
        versionOfRegisteredSchema1Subject1);
    assertEquals("1st schema registered globally should have id 3", 3,
        idOfRegisteredSchema1Subject1);

    // send schema with all references resolved
    lookUpRequest = new RegisterSchemaRequest();
    Parser parser = new Parser();
    parser.parse(reference1);
    parser.parse(reference2);
    AvroSchema resolvedSchema = new AvroSchema(parser.parse(schemaString2));
    lookUpRequest.setSchema(resolvedSchema.canonicalString());
    versionOfRegisteredSchema1Subject1 =
        restApp.restClient.lookUpSubjectVersion(lookUpRequest, subject1, true, false).getVersion();
    assertEquals("1st schema under subject1 should have version 1", 1,
        versionOfRegisteredSchema1Subject1);
  }

  @Test
  public void testBad() throws Exception {
    String subject1 = "testTopic1";
    List<String> allSubjects = new ArrayList<String>();

    // test getAllSubjects with no existing data
    assertEquals("Getting all subjects should return empty",
        allSubjects,
        restApp.restClient.getAllSubjects()
    );

    try {
      TestUtils.registerAndVerifySchema(restApp.restClient, TestUtils.getBadSchema(), 1, subject1);
      fail("Registering bad schema should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals("Invalid schema",
          Errors.INVALID_SCHEMA_ERROR_CODE,
          rce.getErrorCode());
    }

    try {
      TestUtils.registerAndVerifySchema(restApp.restClient,
          TestUtils.getRandomCanonicalAvroString(1).get(0),
          Arrays.asList(new SchemaReference("bad", "bad", 100)), 1, subject1);
      fail("Registering bad reference should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals("Invalid schema",
          Errors.INVALID_SCHEMA_ERROR_CODE,
          rce.getErrorCode());
    }

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
        allSubjects,
        restApp.restClient.getAllSubjects()
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
      assertEquals("Subject not found",
                   Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
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
      assertEquals("Schema not found", Errors.SCHEMA_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
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

    assertEquals("Deleting Schema Version Success", (Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, subject2, "1"));

    associatedSubjects = restApp.restClient.getAllSubjectsById(1);
    assertEquals(associatedSubjects.size(), 1);
    assertEquals(Collections.singletonList(subject1), associatedSubjects);

    associatedSubjects = restApp.restClient.getAllSubjectsById(
        RestService.DEFAULT_REQUEST_PROPERTIES, 1, null, true);
    assertEquals(associatedSubjects.size(), 2);
    assertEquals(Arrays.asList(subject1, subject2), associatedSubjects);
  }

  @Test
  public void testGetSubjectsAssociatedWithNotFoundSchemaId() throws Exception {
    try {
      restApp.restClient.getAllSubjectsById(1);
      fail("Getting all subjects associated with id 1 should fail with "
            + Errors.SCHEMA_NOT_FOUND_ERROR_CODE + " (schema not found)");
    } catch (RestClientException rce) {
      assertEquals("Should get a 404 status for non-existing schema",
              Errors.SCHEMA_NOT_FOUND_ERROR_CODE,
              rce.getErrorCode());
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

    assertEquals("Deleting Schema Version Success", (Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, subject2, "1"));

    associatedSubjects = restApp.restClient.getAllVersionsById(1);
    assertEquals(associatedSubjects.size(), 1);
    assertTrue(associatedSubjects.contains(new SubjectVersion(subject1, 1)));

    associatedSubjects = restApp.restClient.getAllVersionsById(
        RestService.DEFAULT_REQUEST_PROPERTIES, 1, null, true);
    assertEquals(associatedSubjects.size(), 2);
    assertTrue(associatedSubjects.contains(new SubjectVersion(subject1, 1)));
    assertTrue(associatedSubjects.contains(new SubjectVersion(subject2, 1)));
  }

  @Test
  public void testCompatibilityNonExistentSubject() throws Exception {
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    boolean result = restApp.restClient.testCompatibility(schema, "non-existent-subject", "latest")
                                       .isEmpty();
    assertTrue("Compatibility succeeds", result);

    result = restApp.restClient.testCompatibility(schema, "non-existent-subject", null)
        .isEmpty();
    assertTrue("Compatibility succeeds", result);
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
      assertEquals("Version not found", Errors.VERSION_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
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
      assertEquals("Version not found",
                   RestInvalidVersionException.ERROR_CODE,
                   rce.getErrorCode());
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
      assertEquals("Subject not found",
                   Errors.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE,
                   rce.getErrorCode());
    }
  }

  @Test
  public void testCanonicalization() throws Exception {
    // schema string with extra white space
    String schema = "{   \"type\":   \"string\"}";
    String subject = "test";
    assertEquals("Registering a new schema should succeed",
                 1,
                 restApp.restClient.registerSchema(schema, subject));

    assertEquals("Registering the same schema should get back the same id",
                 1,
                 restApp.restClient.registerSchema(schema, subject));

    assertEquals("Lookup the same schema should get back the same id",
                 1,
                 restApp.restClient.lookUpSubjectVersion(schema, subject)
                     .getId().intValue());
  }

  @Test
  public void testDeleteSchemaVersionBasic() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals("Deleting Schema Version Success", (Integer) 2, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2"));

    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(subject));

    try {
      restApp.restClient.getVersion(subject, 2);
      fail(String.format("Getting Version %s for subject %s should fail with %s", "2", subject,
                         Errors.VERSION_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals("Version not found",
                   Errors.VERSION_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
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
      assertEquals("Schema not found",
                   Errors.SCHEMA_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
    }

    assertEquals("Deleting Schema Version Success", (Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "latest"));
    try {
      List<Integer> versions = restApp.restClient.getAllVersions(subject);
      fail("Getting all versions from non-existing subject1 should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " (subject not found). Got " + versions);
    } catch (RestClientException rce) {
      assertEquals("Should get a 404 status for non-existing subject",
                   Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
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
      fail(String.format("Permanent deleting first time should throw schemaVersionNotSoftDeletedException"));
    } catch (RestClientException rce) {
      assertEquals("Schema version must be soft deleted first",
              Errors.SCHEMAVERSION_NOT_SOFT_DELETED_ERROR_CODE,
              rce.getErrorCode());
    }

    //soft delete
    assertEquals("Deleting Schema Version Success", (Integer) 2, restApp.restClient
            .deleteSchemaVersion
                    (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2"));

    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(subject));
    assertEquals(Arrays.asList(1,2), restApp.restClient.getAllVersions(
            RestService.DEFAULT_REQUEST_PROPERTIES,
            subject, true));
    //soft delete again
    try {
      restApp.restClient
              .deleteSchemaVersion
                      (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2");
      fail(String.format("Soft deleting second time should throw schemaVersionSoftDeletedException"));
    } catch (RestClientException rce) {
      assertEquals("Schema version already soft deleted",
              Errors.SCHEMAVERSION_SOFT_DELETED_ERROR_CODE,
              rce.getErrorCode());
    }

    try {
      restApp.restClient.getVersion(subject, 2);
      fail(String.format("Getting Version %s for subject %s should fail with %s", "2", subject,
              Errors.VERSION_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals("Version not found",
              Errors.VERSION_NOT_FOUND_ERROR_CODE,
              rce.getErrorCode());
    }

      Schema schema = restApp.restClient.getVersion(subject, 2, true);
      assertEquals("Lookup Version Match", (Integer) 2, schema.getVersion());

    try {
      RegisterSchemaRequest request = new RegisterSchemaRequest();
      request.setSchema(schemas.get(1));
      restApp.restClient.lookUpSubjectVersion(
          RestService.DEFAULT_REQUEST_PROPERTIES, request, subject, false, false);
      fail(String.format("Lookup Subject Version %s for subject %s should fail with %s", "2",
              subject,
              Errors.SCHEMA_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals("Schema not found",
              Errors.SCHEMA_NOT_FOUND_ERROR_CODE,
              rce.getErrorCode());
    }
    // permanent delete
    assertEquals("Deleting Schema Version Success", (Integer) 2, restApp.restClient
            .deleteSchemaVersion
                    (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2", true));
    // GET after permanent delete should give exception
    try {
      restApp.restClient.getVersion(subject, 2, true);
      fail(String.format("Getting Version %s for subject %s should fail with %s", "2", subject,
              Errors.VERSION_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals("Version not found",
              Errors.VERSION_NOT_FOUND_ERROR_CODE,
              rce.getErrorCode());
    }
    //permanent delete again
    try {
      restApp.restClient.deleteSchemaVersion
                      (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2", true);
      fail(String.format("Getting Version %s for subject %s should fail with %s", "2", subject,
              Errors.VERSION_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals("Version not found",
              Errors.VERSION_NOT_FOUND_ERROR_CODE,
              rce.getErrorCode());
    }

    assertEquals("Deleting Schema Version Success", (Integer) 1, restApp.restClient
            .deleteSchemaVersion
                    (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "latest"));

    try {
      List<Integer> versions = restApp.restClient.getAllVersions(subject);
      fail("Getting all versions from non-existing subject1 should fail with "
              + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
              + " (subject not found). Got " + versions);
    } catch (RestClientException rce) {
      assertEquals("Should get a 404 status for non-existing subject",
              Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
              rce.getErrorCode());
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
      assertEquals("Subject not found",
                   Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
    }
  }

  @Test
  public void testDeleteLatestVersion() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(3);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals("Deleting Schema Version Success", (Integer) 2, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "latest"));

    Schema schema = restApp.restClient.getLatestVersion(subject);
    assertEquals("Latest Version Schema", schemas.get(0), schema.getSchema());

    assertEquals("Deleting Schema Version Success", (Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "latest"));
    try {
      restApp.restClient.getLatestVersion(subject);
      fail("Getting latest versions from non-existing subject should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " (subject not found).");
    } catch (RestClientException rce) {
      assertEquals("Should get a 404 status for non-existing subject",
                   Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
    }

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(2), 3, subject);
    assertEquals("Latest version available after subject re-registration",
            schemas.get(2),
            restApp.restClient.getLatestVersion(subject).getSchema());
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
      assertEquals("Should get a 404 status for non-existing subject",
              Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
              rce.getErrorCode());
    }
  }

  @Test
  public void testGetLatestVersionDeleteOlder() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);

    assertEquals("Latest Version Schema", schemas.get(1), restApp.restClient.getLatestVersion(subject).getSchema());

    assertEquals("Deleting Schema Older Version Success", (Integer) 1, restApp.restClient
            .deleteSchemaVersion
                    (RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1"));
    assertEquals("Latest Version Schema Still Same",
            schemas.get(1),
            restApp.restClient.getLatestVersion(subject).getSchema());
  }

  @Test
  public void testDeleteInvalidVersion() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(1);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    try {
      restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2");
    } catch (RestClientException rce) {
      assertEquals("Should get a 404 status for non-existing subject version",
                   Errors.VERSION_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
    }

  }

  @Test
  public void testDeleteWithLookup() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject = "test";

    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject);
    assertEquals("Deleting Schema Version Success", (Integer) 1, restApp.restClient
        .deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1"));
    try {
      restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, false);
      fail(String.format("Lookup Subject Version %s for subject %s should fail with %s", "2",
                         subject,
                         Errors.SCHEMA_NOT_FOUND_ERROR_CODE));
    } catch (RestClientException rce) {
      assertEquals("Schema not found",
                   Errors.SCHEMA_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
    }
    //verify deleted schema
    Schema schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, true);
    assertEquals("Lookup Version Match", (Integer) 1, schema.getVersion());

    //re-register schema again and verify we get latest version
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject);
    schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, true);
    assertEquals("Lookup Version Match", (Integer) 3, schema.getVersion());
    schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, false);
    assertEquals("Lookup Version Match", (Integer) 3, schema.getVersion());
    schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject);
    assertEquals("Lookup Version Match", (Integer) 3, schema.getVersion());
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
    assertTrue("Schema should be compatible with specified version", isCompatible);

    restApp.restClient.registerSchema(wrongSchema2, subject);

    isCompatible = restApp.restClient.testCompatibility(correctSchema2, subject,
                                                        "latest").isEmpty();
    assertFalse("Schema should be incompatible with specified version", isCompatible);
    try {
      restApp.restClient.registerSchema(correctSchema2, subject);
      fail("Schema should be Incompatible");
    } catch (RestClientException rce) {
      assertEquals("Incompatible Schema",
                   Errors.INCOMPATIBLE_SCHEMA_ERROR_CODE,
                   rce.getErrorCode());
    }

    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "latest");
    isCompatible = restApp.restClient.testCompatibility(correctSchema2, subject,
                                                        "latest").isEmpty();
    assertTrue("Schema should be compatible with specified version", isCompatible);

    restApp.restClient.registerSchema(correctSchema2, subject);

    assertEquals("Version is same", (Integer) 3, restApp.restClient.lookUpSubjectVersion
        (correctSchema2String, subject).getVersion());

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
    assertEquals("Compatibility Level Exists", CompatibilityLevel.BACKWARD.name, restApp
        .restClient.getConfig(subject).getCompatibilityLevel());
    assertEquals("Top Compatibility Level Exists", CompatibilityLevel.FULL.name, restApp
        .restClient.getConfig(null).getCompatibilityLevel());
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2");
    try {
      restApp.restClient.getConfig(subject);
    } catch (RestClientException rce) {
      assertEquals("Compatibility Level doesn't exist",
              Errors.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE, rce.getErrorCode());
    }
    assertEquals("Top Compatibility Level Exists", CompatibilityLevel.FULL.name, restApp
        .restClient.getConfig(null).getCompatibilityLevel());

  }

  @Test
  public void testListSubjects() throws Exception {
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(2);
    String subject1 = "test1";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(0), 1, subject1);
    String subject2 = "test2";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemas.get(1), 2, subject2);;
    List<String> expectedResponse = new ArrayList<>();
    expectedResponse.add(subject1);
    expectedResponse.add(subject2);
    assertEquals("Current Subjects", expectedResponse,
            restApp.restClient.getAllSubjects());
    List<Integer> deletedResponse = new ArrayList<>();
    deletedResponse.add(1);
    assertEquals("Versions Deleted Match", deletedResponse,
            restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject2));

    expectedResponse = new ArrayList<>();
    expectedResponse.add(subject1);
    assertEquals("Current Subjects", expectedResponse,
            restApp.restClient.getAllSubjects());

    expectedResponse = new ArrayList<>();
    expectedResponse.add(subject1);
    expectedResponse.add(subject2);
    assertEquals("Current Subjects", expectedResponse,
            restApp.restClient.getAllSubjects(true));

    assertEquals("Versions Deleted Match", deletedResponse,
            restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES,
                    subject2, true));

    expectedResponse = new ArrayList<>();
    expectedResponse.add(subject1);
    assertEquals("Current Subjects", expectedResponse,
            restApp.restClient.getAllSubjects());
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
    assertEquals("Versions Deleted Match", expectedResponse,
        restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject));
    try {
      restApp.restClient.getLatestVersion(subject);
      fail(String.format("Subject %s should not be found", subject));
    } catch (RestClientException rce) {
      assertEquals("Subject Not Found", Errors.SUBJECT_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
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
    assertEquals("Versions Deleted Match", expectedResponse,
            restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject));

    Schema schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, true );
    assertEquals(1, (long)schema.getVersion());
    schema = restApp.restClient.lookUpSubjectVersion(schemas.get(1), subject, true );
    assertEquals(2, (long)schema.getVersion());

    try {
      restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject);
      fail(String.format("Subject %s should not be found", subject));
    } catch (RestClientException rce) {
      assertEquals("Subject exists in soft deleted format.", Errors.SUBJECT_SOFT_DELETED_ERROR_CODE, rce.getErrorCode());
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
      fail(String.format("Delete permanent should not succeed"));
    } catch (RestClientException rce) {
      assertEquals("Subject '%s' was not deleted first before permanent delete", Errors.SUBJECT_NOT_SOFT_DELETED_ERROR_CODE, rce.getErrorCode());
    }

    assertEquals("Versions Deleted Match", expectedResponse,
            restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject));

    Schema schema = restApp.restClient.lookUpSubjectVersion(schemas.get(0), subject, true );
    assertEquals(1, (long)schema.getVersion());
    schema = restApp.restClient.lookUpSubjectVersion(schemas.get(1), subject, true );
    assertEquals(2, (long)schema.getVersion());

    assertEquals("Versions Deleted Match", expectedResponse,
            restApp.restClient.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES,
                    subject,
                    true));
    for (Integer i : expectedResponse) {
      try {
        restApp.restClient.lookUpSubjectVersion(schemas.get(i-i), subject, false);
        fail(String.format("Subject %s should not be found", subject));
      } catch (RestClientException rce) {
        assertEquals("Subject Not Found", Errors.SUBJECT_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
      }

      try {
        restApp.restClient.lookUpSubjectVersion(schemas.get(i-1), subject, true);
        fail(String.format("Subject %s should not be found", subject));
      } catch (RestClientException rce) {
        assertEquals("Subject Not Found", Errors.SUBJECT_NOT_FOUND_ERROR_CODE, rce.getErrorCode());
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

    assertEquals("Versions match", Arrays.asList(3, 4),
                 restApp.restClient.getAllVersions(subject));
    try {
      restApp.restClient.getVersion(subject, 1);
      fail("Version 1 should not be found");
    } catch (RestClientException rce) {
      assertEquals("Version not found", Errors.VERSION_NOT_FOUND_ERROR_CODE,
                   rce.getErrorCode());
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
      assertEquals("Compatibility Level doesn't exist",
              Errors.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE, rce.getErrorCode());
    }
    assertEquals("Top Compatibility Level Exists", CompatibilityLevel.FULL.name, restApp
        .restClient.getConfig(null).getCompatibilityLevel());

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

    try {
      restApp.restClient.getMode(subject).getMode();
      fail(String.format("Subject %s should not be found when there's no mode override", subject));
    } catch (RestClientException e) {
      assertEquals(String.format("No mode override for subject %s, get mode should return not configured", subject),
          Errors.SUBJECT_LEVEL_MODE_NOT_CONFIGURED_ERROR_CODE, e.getErrorCode());
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
      fail(String.format("Subject %s is in read-only mode", "testSubject2"));
    } catch (RestClientException rce) {
      assertEquals("Subject is in read-only mode", Errors.OPERATION_NOT_PERMITTED_ERROR_CODE, rce
          .getErrorCode());
    }
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

