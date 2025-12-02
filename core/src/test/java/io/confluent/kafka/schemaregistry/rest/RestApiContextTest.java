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
import io.confluent.kafka.schemaregistry.RestApp;
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

public abstract class RestApiContextTest {

  protected RestApp restApp = null;

  public void setRestApp(RestApp restApp) {
    this.restApp = restApp;
  }

  protected int expectedSchemaId(int sequentialId) {
    return sequentialId;
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
      registerAndVerifySchema(restApp.restClient, schema, expectedSchemaId(schemaIdCounter),
                              subject1);
      schemaIdCounter++;
      allVersionsInSubject1.add(expectedVersion);
    }

    // test re-registering existing schemas
    for (int i = 0; i < schemasInSubject1; i++) {
      int expectedId = expectedSchemaId(i + 1);
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
      registerAndVerifySchema(restApp.restClient, schema, expectedSchemaId(schemaIdCounter),
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
      registerAndVerifySchema(restApp.restClient, schema, expectedSchemaId(schemaIdCounter),
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
      registerAndVerifySchema(restApp.restClient, schema, expectedSchemaId(schemaIdCounter),
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
    testContextPathsImpl(restClient1, restClient2, restClient3, noCtxRestClient3);
  }

  public void testContextPathsImpl(
      RestService restClient1,
      RestService restClient2,
      RestService restClient3,
      RestService noCtxRestClient3) throws Exception {
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
      registerAndVerifySchema(restClient1, schema, expectedSchemaId(schemaIdCounter),
          subject1);
      schemaIdCounter++;
      allVersionsInSubject1.add(expectedVersion);
    }

    // test re-registering existing schemas
    for (int i = 0; i < schemasInSubject1; i++) {
      int expectedId = expectedSchemaId(i + 1);
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
      registerAndVerifySchema(restClient2, schema, expectedSchemaId(schemaIdCounter),
          subject2A);
      registerAndVerifySchema(restClient2, schema, expectedSchemaId(schemaIdCounter),
          subject2B);
      registerAndVerifySchema(restClient2, schema, expectedSchemaId(schemaIdCounter),
          subject2C);
      schemaIdCounter++;
      allVersionsInSubject2.add(expectedVersion);
    }

    // reset the schema id counter due to a different context
    schemaIdCounter = 1;

    try {
      restClient3.getId(expectedSchemaId(schemaIdCounter));
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
      registerAndVerifySchema(restClient3, schema, expectedSchemaId(schemaIdCounter),
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
        noCtxRestClient3.getId(expectedSchemaId(1), subject3).getSchemaString(),
        "Registered schema should be found"
    );

    // Now ask for schema id 1 from subject2A.
    // This should NOT return schema id 1 from the default context
    assertEquals(
        allSchemasInSubject2.get(0),
        noCtxRestClient3.getId(expectedSchemaId(1), subject2A).getSchemaString(),
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

  protected static void registerAndVerifySchema(RestService restService, String schemaString,
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

  @Test
  public void testContextPrefixFilter() throws Exception {
    // Register schemas in multiple contexts to create test data
    String subject1 = ":.prod:testSubject1";
    String subject2 = ":.prod-eu:testSubject2";
    String subject3 = ":.staging:testSubject3";
    String subject4 = ":.dev:testSubject4";
    String subject5 = "testSubject5";  // default context

    List<String> schemas = TestUtils.getRandomCanonicalAvroString(5);

    // Register schemas in different contexts
    restApp.restClient.registerSchema(schemas.get(0), subject1);
    restApp.restClient.registerSchema(schemas.get(1), subject2);
    restApp.restClient.registerSchema(schemas.get(2), subject3);
    restApp.restClient.registerSchema(schemas.get(3), subject4);
    restApp.restClient.registerSchema(schemas.get(4), subject5);

    // Test 1: Get all contexts without filter
    List<String> allContexts = restApp.restClient.getAllContexts();
    assertEquals(
        ImmutableList.of(DEFAULT_CONTEXT, ".dev", ".prod", ".prod-eu", ".staging"),
        allContexts,
        "Getting all contexts should return all registered contexts"
    );

    // Test 2: Filter by prefix ".prod" - should match both ".prod" and ".prod-eu"
    List<String> prodContexts = restApp.restClient.getAllContexts(0, -1, ".prod");
    assertEquals(
        ImmutableList.of(".prod", ".prod-eu"),
        prodContexts,
        "Filtering by '.prod' should return contexts starting with '.prod'"
    );

    // Test 3: Filter by exact match ".prod-eu"
    List<String> prodEuContexts = restApp.restClient.getAllContexts(0, -1, ".prod-eu");
    assertEquals(
        Collections.singletonList(".prod-eu"),
        prodEuContexts,
        "Filtering by '.prod-eu' should return only '.prod-eu' context"
    );

    // Test 4: Filter by "." - should return all contexts (all start with ".")
    List<String> dotPrefixContexts = restApp.restClient.getAllContexts(0, -1, ".");
    assertEquals(
        allContexts,
        dotPrefixContexts,
        "Filtering by '.' should return all contexts as all contexts start with '.'"
    );

    // Test 5: Filter by ".s" - should return ".staging"
    List<String> stagingContexts = restApp.restClient.getAllContexts(0, -1, ".s");
    assertEquals(
        Collections.singletonList(".staging"),
        stagingContexts,
        "Filtering by '.s' should return '.staging' context"
    );

    // Test 6: Filter by non-matching prefix
    List<String> noMatch = restApp.restClient.getAllContexts(0, -1, ".nonexistent");
    assertEquals(
        Collections.emptyList(),
        noMatch,
        "Filtering by non-matching prefix should return empty list"
    );

    // Test 7: Filter with empty string - should return all contexts
    List<String> emptyFilter = restApp.restClient.getAllContexts(0, -1, "");
    assertEquals(
        allContexts,
        emptyFilter,
        "Filtering with empty string should return all contexts"
    );
  }

  @Test
  public void testContextPrefixFilterWithPagination() throws Exception {
    // Register schemas in multiple contexts
    String subject1 = ":.alpha:test";
    String subject2 = ":.beta:test";
    String subject3 = ":.gamma:test";
    String subject4 = ":.delta:test";

    List<String> schemas = TestUtils.getRandomCanonicalAvroString(4);

    restApp.restClient.registerSchema(schemas.get(0), subject1);
    restApp.restClient.registerSchema(schemas.get(1), subject2);
    restApp.restClient.registerSchema(schemas.get(2), subject3);
    restApp.restClient.registerSchema(schemas.get(3), subject4);

    // Test 1: Get all contexts (should have default + 4 named contexts = 5 total)
    List<String> allContexts = restApp.restClient.getAllContexts();
    assertEquals(
        5,
        allContexts.size(),
        "Should have 5 total contexts"
    );

    // Test 2: Filter by "." and paginate - offset 0, limit 2
    List<String> page1 = restApp.restClient.getAllContexts(0, 2, ".");
    assertEquals(
        2,
        page1.size(),
        "First page should have 2 contexts"
    );
    assertEquals(
        ImmutableList.of(DEFAULT_CONTEXT, ".alpha"),
        page1,
        "First page should contain default and .alpha"
    );

    // Test 3: Filter by "." and paginate - offset 2, limit 2
    List<String> page2 = restApp.restClient.getAllContexts(2, 2, ".");
    assertEquals(
        2,
        page2.size(),
        "Second page should have 2 contexts"
    );
    assertEquals(
        ImmutableList.of(".beta", ".delta"),
        page2,
        "Second page should contain .beta and .delta"
    );

    // Test 4: Filter by "." and paginate - offset 4, limit 2
    List<String> page3 = restApp.restClient.getAllContexts(4, 2, ".");
    assertEquals(
        1,
        page3.size(),
        "Third page should have 1 context"
    );
    assertEquals(
        Collections.singletonList(".gamma"),
        page3,
        "Third page should contain .gamma"
    );

    // Test 5: Filter by ".a" (should match .alpha) with pagination
    List<String> alphaPage = restApp.restClient.getAllContexts(0, 1, ".a");
    assertEquals(
        Collections.singletonList(".alpha"),
        alphaPage,
        "Filtering by '.a' with limit 1 should return .alpha"
    );
  }

  @Test
  public void testContextPrefixFilterEdgeCases() throws Exception {
    // Create a context
    String subject = ":.test:subject";
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(1);
    restApp.restClient.registerSchema(schemas.get(0), subject);

    // Test 1: Null contextPrefix (should return all contexts)
    List<String> nullFilter = restApp.restClient.getAllContexts(0, -1, null);
    assertEquals(
        ImmutableList.of(DEFAULT_CONTEXT, ".test"),
        nullFilter,
        "Null contextPrefix should return all contexts"
    );

    // Test 2: Very long non-matching prefix
    List<String> longPrefix = restApp.restClient.getAllContexts(
        0, -1, ".thisIsAVeryLongPrefixThatWillNeverMatch");
    assertEquals(
        Collections.emptyList(),
        longPrefix,
        "Long non-matching prefix should return empty list"
    );

    // Test 3: Filter by "." - should return all contexts (all start with ".")
    List<String> dotPrefixCtx = restApp.restClient.getAllContexts(0, -1, ".");
    assertEquals(
        ImmutableList.of(DEFAULT_CONTEXT, ".test"),
        dotPrefixCtx,
        "Prefix '.' should match all contexts as all contexts start with '.'"
    );

    // Test 4: Case-sensitive matching - ".TEST" should not match ".test"
    List<String> upperCaseFilter = restApp.restClient.getAllContexts(0, -1, ".TEST");
    assertEquals(
        Collections.emptyList(),
        upperCaseFilter,
        "Context prefix filter should be case-sensitive"
    );
  }
}
