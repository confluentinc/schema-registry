/*
 * Copyright 2021 Confluent Inc.
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

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class RestApiContextTest extends ClusterTestHarness {

  public RestApiContextTest() {
    super(1, true);
  }

  @Test
  public void testQualifiedSubjects() throws Exception {
    String subject1 = ":.ctx1:testTopic1";
    String subject2 = ":.ctx2:testTopic2";
    String subject3 = ":.ctx3:testTopic1";
    String subject4 = "testTopic1";
    int schemasInSubject1 = 10;
    List<Integer> allVersionsInSubject1 = new ArrayList<Integer>();
    List<String> allSchemasInSubject1 = TestUtils.getRandomCanonicalAvroString(schemasInSubject1);
    int schemasInSubject2 = 5;
    List<Integer> allVersionsInSubject2 = new ArrayList<Integer>();
    List<String> allSchemasInSubject2 = TestUtils.getRandomCanonicalAvroString(schemasInSubject2);
    int schemasInSubject3 = 2;
    List<Integer> allVersionsInSubject3 = new ArrayList<Integer>();
    List<String> allSchemasInSubject3 = TestUtils.getRandomCanonicalAvroString(schemasInSubject3);
    int schemasInSubject4 = 1;
    List<Integer> allVersionsInSubject4 = new ArrayList<Integer>();
    List<String> allSchemasInSubject4 = TestUtils.getRandomCanonicalAvroString(schemasInSubject4);

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

    // test getAllSubjects with no existing data
    assertEquals(
        Collections.emptyList(),
        restApp.restClient.getAllSubjects(),
        "Getting all subjects should return empty"
    );

    // test registering and verifying new schemas in subject1
    int schemaIdCounter = 1;
    for (int i = 0; i < schemasInSubject1; i++) {
      String schema = allSchemasInSubject1.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter,
                              subject1);
      schemaIdCounter++;
      allVersionsInSubject1.add(expectedVersion);
    }

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

    // reset the schema id counter due to a different context
    schemaIdCounter = 1;

    // test registering schemas in subject2
    for (int i = 0; i < schemasInSubject2; i++) {
      String schema = allSchemasInSubject2.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter,
                              subject2);
      schemaIdCounter++;
      allVersionsInSubject2.add(expectedVersion);
    }

    // reset the schema id counter due to a different context
    schemaIdCounter = 1;

    // test registering schemas in subject3
    for (int i = 0; i < schemasInSubject3; i++) {
      String schema = allSchemasInSubject3.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter,
          subject3);
      schemaIdCounter++;
      allVersionsInSubject3.add(expectedVersion);
    }

    // reset the schema id counter due to a different context
    schemaIdCounter = 1;

    // test registering schemas in subject4
    for (int i = 0; i < schemasInSubject4; i++) {
      String schema = allSchemasInSubject4.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter,
          subject4);
      schemaIdCounter++;
      allVersionsInSubject4.add(expectedVersion);
    }

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

    // test getAllContexts
    assertEquals(
        ImmutableList.of(DEFAULT_CONTEXT, ".ctx1", ".ctx2", ".ctx3"),
        restApp.restClient.getAllContexts(),
        "Getting all contexts should return all registered contexts"
    );

    // test getAllSubjectsWithPrefix with existing data
    assertEquals(
        Collections.singletonList(subject1),
        restApp.restClient.getAllSubjects(":.ctx1:", false),
        "Getting all subjects should match all registered subjects"
    );

    // test getAllSubjectsWithPrefix with existing data
    assertEquals(
        Collections.singletonList(subject2),
        restApp.restClient.getAllSubjects(":.ctx2:", false),
        "Getting all subjects should match all registered subjects"
    );

    // test getAllSubjects with existing data
    assertEquals(
        Collections.singletonList(subject4),
        restApp.restClient.getAllSubjects("", false),
        "Getting all subjects should match default subjects"
    );

    // test getAllSubjectsWithPrefix with context wildcard
    assertEquals(
        ImmutableList.of(subject1, subject2, subject3, subject4),
        restApp.restClient.getAllSubjects(":*:", false),
        "Getting all subjects should match all registered subjects"
    );

    // test getSchemas with context wildcard
    assertEquals(
        schemasInSubject1 + schemasInSubject2 + schemasInSubject3 + schemasInSubject4,
        restApp.restClient.getSchemas(":*:", false, false).size(),
        "Getting all schemas should match all registered subjects"
    );

    // test getSchemas with context wildcard and subject
    assertEquals(
        schemasInSubject1 + schemasInSubject3 + schemasInSubject4,
        restApp.restClient.getSchemas(":*:testTopic1", false, false).size(),
        "Getting all schemas should match registered subjects"
    );

    // test getSchemas with context wildcard and subject
    assertEquals(
        schemasInSubject2,
        restApp.restClient.getSchemas(":*:testTopic2", false, false).size(),
        "Getting all schemas should match registered subjects"
    );

    Schema schema = restApp.restClient.getVersion("testTopic2", 1);
    assertEquals(
        1,
        schema.getVersion().intValue(),
        "Getting schema by version w/o context should succeed"
    );

    schema = restApp.restClient.lookUpSubjectVersion(schema.getSchema(), "testTopic2");
    assertEquals(
        1,
        schema.getVersion().intValue(),
        "Getting schema by schema w/o context should succeed"
    );
  }

  @Test
  public void testContextPaths() throws Exception {
    RestService restClient1 = new RestService(restApp.restConnect + "/contexts/.ctx1");
    RestService restClient2 = new RestService(restApp.restConnect + "/contexts/.ctx2");
    RestService restClient3 = new RestService(restApp.restConnect + "/contexts/:.:");
    RestService noCtxRestClient3 = new RestService(restApp.restConnect);

    String subject1 = "testTopic1";
    String subject2A = "testTopic2A";
    String subject2B = "testTopic2B";
    String subject2C = "testTopic2C";
    String subject3 = "testTopic3";
    int schemasInSubject1 = 10;
    List<Integer> allVersionsInSubject1 = new ArrayList<Integer>();
    List<String> allSchemasInSubject1 = TestUtils.getRandomCanonicalAvroString(schemasInSubject1);
    int schemasInSubject2 = 5;
    List<Integer> allVersionsInSubject2 = new ArrayList<Integer>();
    List<String> allSchemasInSubject2 = TestUtils.getRandomCanonicalAvroString(schemasInSubject2);
    int schemasInSubject3 = 2;
    List<Integer> allVersionsInSubject3 = new ArrayList<Integer>();
    List<String> allSchemasInSubject3 = TestUtils.getRandomCanonicalAvroString(schemasInSubject3);

    // test getAllVersions with no existing data
    try {
      restClient1.getAllVersions(subject1);
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

    // test registering and verifying new schemas in subject1
    int schemaIdCounter = 1;
    for (int i = 0; i < schemasInSubject1; i++) {
      String schema = allSchemasInSubject1.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restClient1, schema, schemaIdCounter,
          subject1);
      schemaIdCounter++;
      allVersionsInSubject1.add(expectedVersion);
    }

    // test re-registering existing schemas
    for (int i = 0; i < schemasInSubject1; i++) {
      int expectedId = i + 1;
      String schemaString = allSchemasInSubject1.get(i);
      int foundId = restClient1.registerSchema(schemaString, subject1);
      assertEquals(
          expectedId, foundId,
          "Re-registering an existing schema should return the existing version"
      );
    }

    // reset the schema id counter due to a different context
    schemaIdCounter = 1;

    // test registering schemas in subject2A, subject2B, subject2C
    for (int i = 0; i < schemasInSubject2; i++) {
      String schema = allSchemasInSubject2.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restClient2, schema, schemaIdCounter,
          subject2A);
      registerAndVerifySchema(restClient2, schema, schemaIdCounter,
          subject2B);
      registerAndVerifySchema(restClient2, schema, schemaIdCounter,
          subject2C);
      schemaIdCounter++;
      allVersionsInSubject2.add(expectedVersion);
    }

    // reset the schema id counter due to a different context
    schemaIdCounter = 1;

    try {
      restClient3.getId(schemaIdCounter);
      fail("Registered schema should not be found in default context");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.SCHEMA_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode(),
          "Should get a 404 status for non-existing schema"
      );
    }

    // test registering schemas in subject3
    for (int i = 0; i < schemasInSubject3; i++) {
      String schema = allSchemasInSubject3.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restClient3, schema, schemaIdCounter,
          subject3);
      schemaIdCounter++;
      allVersionsInSubject3.add(expectedVersion);
    }

    // test getAllVersions with existing data
    assertEquals(
        allVersionsInSubject1,
        restClient1.getAllVersions(subject1),
        "Getting all versions from subject1 should match all registered versions"
    );
    assertEquals(
        allVersionsInSubject2,
        restClient2.getAllVersions(subject2A),
        "Getting all versions from subject2A should match all registered versions"
    );
    assertEquals(
        allVersionsInSubject3,
        restClient3.getAllVersions(subject3),
        "Getting all versions from subject3 should match all registered versions"
    );
    assertEquals(
        allVersionsInSubject3,
        noCtxRestClient3.getAllVersions(subject3),
        "Getting all versions from subject3 should match all registered versions"
    );
    assertEquals(
        allVersionsInSubject3,
        noCtxRestClient3.getAllVersions(":.:" + subject3),
        "Getting all versions from subject3 should match all registered versions"
    );

    // test getAllContexts
    assertEquals(
        ImmutableList.of(DEFAULT_CONTEXT, ".ctx1", ".ctx2"),
        restClient1.getAllContexts(),
        "Getting all contexts should return all registered contexts"
    );

    // test getAllSubjects with existing data
    assertEquals(
        Collections.singletonList(":.ctx1:" + subject1),
        restClient1.getAllSubjects(),
        "Getting all subjects should match all registered subjects"
    );

    // test getAllSubjects with existing data
    assertEquals(
        ImmutableList.of(":.ctx2:" + subject2A, ":.ctx2:" + subject2B, ":.ctx2:" + subject2C),
        restClient2.getAllSubjects(),
        "Getting all subjects should match all registered subjects"
    );

    // test getAllSubjects with existing data
    assertEquals(
        Collections.singletonList(subject3),
        restClient3.getAllSubjects(),
        "Getting all subjects should match all registered subjects"
    );

    // test getAllSubjects with existing data
    assertEquals(
        Collections.singletonList(subject3),
        noCtxRestClient3.getAllSubjects("", false),
        "Getting all subjects should match all registered subjects"
    );

    // Now ask for schema id 1 from subject3.
    // This should return schema id 1 from the default context
    assertEquals(
        allSchemasInSubject3.get(0),
        noCtxRestClient3.getId(1, subject3).getSchemaString(),
        "Registered schema should be found"
    );

    // Now ask for schema id 1 from subject2A.
    // This should NOT return schema id 1 from the default context
    assertEquals(
        allSchemasInSubject2.get(0),
        noCtxRestClient3.getId(1, subject2A).getSchemaString(),
        "Registered schema should be found"
    );

    // Delete subject1
    restClient1.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject1, false);
    restClient1.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, subject1, true);

    try {
      restClient1.getAllVersions(subject1);
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

    try {
      noCtxRestClient3.deleteContext(RestService.DEFAULT_REQUEST_PROPERTIES, ".ctx2");
      fail("Deleting context .ctx2 should fail with "
          + Errors.CONTEXT_NOT_EMPTY_ERROR_CODE
          + " (context not empty)");
    } catch (RestClientException rce) {
      assertEquals(
          Errors.CONTEXT_NOT_EMPTY_ERROR_CODE,
          rce.getErrorCode(),
          "Should get a 422 status for non-empty context"
      );
    }

    noCtxRestClient3.deleteContext(RestService.DEFAULT_REQUEST_PROPERTIES, ".ctx1");

    List<String> contexts = restClient1.getAllContexts();
    assertEquals(
        ImmutableList.of(DEFAULT_CONTEXT, ".ctx2"),
        contexts,
        "Getting all contexts should return all registered contexts after subject1 deletion"
    );
  }

  static void registerAndVerifySchema(RestService restService, String schemaString,
      int expectedId, String subject)
      throws IOException, RestClientException {
    int registeredId = restService.registerSchema(schemaString, subject);
    assertEquals(expectedId, registeredId, "Registering a new schema should succeed");

    // the newly registered schema should be immediately readable on the leader
    // Note: this differs from TestUtils in that it passes the subject to getId()
    assertEquals(
        schemaString,
        restService.getId(expectedId, subject).getSchemaString(),
        "Registered schema should be found"
    );
  }
}

