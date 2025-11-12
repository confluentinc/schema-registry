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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
import org.junit.Test;

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
                 Collections.emptyList(),
                 restApp.restClient.getAllSubjects());

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
      assertEquals("Re-registering an existing schema should return the existing version",
                   expectedId, foundId);
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
    assertEquals("Getting all versions from subject1 should match all registered versions",
                 allVersionsInSubject1,
                 restApp.restClient.getAllVersions(subject1));
    assertEquals("Getting all versions from subject2 should match all registered versions",
                 allVersionsInSubject2,
                 restApp.restClient.getAllVersions(subject2));

    // test getAllContexts
    assertEquals("Getting all contexts should return all registered contexts",
                 ImmutableList.of(DEFAULT_CONTEXT, ".ctx1", ".ctx2", ".ctx3"),
                 restApp.restClient.getAllContexts());

    // test getAllSubjectsWithPrefix with existing data
    assertEquals("Getting all subjects should match all registered subjects",
                 Collections.singletonList(subject1),
                restApp.restClient.getAllSubjects(":.ctx1:", false));

    // test getAllSubjectsWithPrefix with existing data
    assertEquals("Getting all subjects should match all registered subjects",
                 Collections.singletonList(subject2),
                 restApp.restClient.getAllSubjects(":.ctx2:", false));

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match default subjects",
                 Collections.singletonList(subject4),
                 restApp.restClient.getAllSubjects("", false));

    // test getAllSubjectsWithPrefix with context wildcard
    assertEquals("Getting all subjects should match all registered subjects",
        ImmutableList.of(subject1, subject2, subject3, subject4),
        restApp.restClient.getAllSubjects(":*:", false));

    // test getSchemas with context wildcard
    assertEquals("Getting all schemas should match all registered subjects",
        schemasInSubject1 + schemasInSubject2 + schemasInSubject3 + schemasInSubject4,
        restApp.restClient.getSchemas(":*:", false, false).size());

    // test getSchemas with context wildcard and subject
    assertEquals("Getting all schemas should match registered subjects",
        schemasInSubject1 + schemasInSubject3 + schemasInSubject4,
        restApp.restClient.getSchemas(":*:testTopic1", false, false).size());

    // test getSchemas with context wildcard and subject
    assertEquals("Getting all schemas should match registered subjects",
        schemasInSubject2,
        restApp.restClient.getSchemas(":*:testTopic2", false, false).size());

    Schema schema = restApp.restClient.getVersion("testTopic2", 1);
    assertEquals("Getting schema by version w/o context should succeed",
        1,
        schema.getVersion().intValue());

    schema = restApp.restClient.lookUpSubjectVersion(schema.getSchema(), "testTopic2");
    assertEquals("Getting schema by schema w/o context should succeed",
        1,
        schema.getVersion().intValue());
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
      assertEquals("Should get a 404 status for non-existing subject",
          Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode());
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
      assertEquals("Re-registering an existing schema should return the existing version",
          expectedId, foundId);
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
      assertEquals("Should get a 404 status for non-existing schema",
          Errors.SCHEMA_NOT_FOUND_ERROR_CODE,
          rce.getErrorCode());
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
    assertEquals("Getting all versions from subject1 should match all registered versions",
        allVersionsInSubject1,
        restClient1.getAllVersions(subject1));
    assertEquals("Getting all versions from subject2A should match all registered versions",
        allVersionsInSubject2,
        restClient2.getAllVersions(subject2A));
    assertEquals("Getting all versions from subject3 should match all registered versions",
        allVersionsInSubject3,
        restClient3.getAllVersions(subject3));
    assertEquals("Getting all versions from subject3 should match all registered versions",
        allVersionsInSubject3,
        noCtxRestClient3.getAllVersions(subject3));
    assertEquals("Getting all versions from subject3 should match all registered versions",
        allVersionsInSubject3,
        noCtxRestClient3.getAllVersions(":.:" + subject3));

    // test getAllContexts
    assertEquals("Getting all contexts should return all registered contexts",
        ImmutableList.of(DEFAULT_CONTEXT, ".ctx1", ".ctx2"),
        restClient1.getAllContexts());

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
        Collections.singletonList(":.ctx1:" + subject1),
        restClient1.getAllSubjects());

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
        ImmutableList.of(":.ctx2:" + subject2A, ":.ctx2:" + subject2B, ":.ctx2:" + subject2C),
        restClient2.getAllSubjects());

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
        Collections.singletonList(subject3),
        restClient3.getAllSubjects());

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
        Collections.singletonList(subject3),
        noCtxRestClient3.getAllSubjects("", false));

    // Now ask for schema id 1 from subject3.
    // This should return schema id 1 from the default context
    assertEquals("Registered schema should be found",
        allSchemasInSubject3.get(0),
        noCtxRestClient3.getId(1, subject3).getSchemaString());

    // Now ask for schema id 1 from subject2A.
    // This should NOT return schema id 1 from the default context
    assertEquals("Registered schema should be found",
        allSchemasInSubject2.get(0),
        noCtxRestClient3.getId(1, subject2A).getSchemaString());
  }

  static void registerAndVerifySchema(RestService restService, String schemaString,
      int expectedId, String subject)
      throws IOException, RestClientException {
    int registeredId = restService.registerSchema(schemaString, subject);
    assertEquals("Registering a new schema should succeed", expectedId, registeredId);

    // the newly registered schema should be immediately readable on the leader
    // Note: this differs from TestUtils in that it passes the subject to getId()
    assertEquals("Registered schema should be found",
        schemaString,
        restService.getId(expectedId, subject).getSchemaString());
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
    assertEquals("Getting all contexts should return all registered contexts",
        ImmutableList.of(DEFAULT_CONTEXT, ".dev", ".prod", ".prod-eu", ".staging"),
        allContexts);

    // Test 2: Filter by prefix ".prod" - should match both ".prod" and ".prod-eu"
    List<String> prodContexts = restApp.restClient.getAllContexts(0, -1, ".prod");
    assertEquals("Filtering by '.prod' should return contexts starting with '.prod'",
        ImmutableList.of(".prod", ".prod-eu"),
        prodContexts);

    // Test 3: Filter by exact match ".prod-eu"
    List<String> prodEuContexts = restApp.restClient.getAllContexts(0, -1, ".prod-eu");
    assertEquals("Filtering by '.prod-eu' should return only '.prod-eu' context",
        Collections.singletonList(".prod-eu"),
        prodEuContexts);

    // Test 4: Filter by "." - should return all contexts (all start with ".")
    List<String> dotPrefixContexts = restApp.restClient.getAllContexts(0, -1, ".");
    assertEquals("Filtering by '.' should return all contexts as all contexts start with '.'",
        allContexts,
        dotPrefixContexts);

    // Test 5: Filter by ".s" - should return ".staging"
    List<String> stagingContexts = restApp.restClient.getAllContexts(0, -1, ".s");
    assertEquals("Filtering by '.s' should return '.staging' context",
        Collections.singletonList(".staging"),
        stagingContexts);

    // Test 6: Filter by non-matching prefix
    List<String> noMatch = restApp.restClient.getAllContexts(0, -1, ".nonexistent");
    assertEquals("Filtering by non-matching prefix should return empty list",
        Collections.emptyList(),
        noMatch);

    // Test 7: Filter with empty string - should return all contexts
    List<String> emptyFilter = restApp.restClient.getAllContexts(0, -1, "");
    assertEquals("Filtering with empty string should return all contexts",
        allContexts,
        emptyFilter);
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
    assertEquals("Should have 5 total contexts",
        5,
        allContexts.size());

    // Test 2: Filter by "." and paginate - offset 0, limit 2
    List<String> page1 = restApp.restClient.getAllContexts(0, 2, ".");
    assertEquals("First page should have 2 contexts",
        2,
        page1.size());
    assertEquals("First page should contain default and .alpha",
        ImmutableList.of(DEFAULT_CONTEXT, ".alpha"),
        page1);

    // Test 3: Filter by "." and paginate - offset 2, limit 2
    List<String> page2 = restApp.restClient.getAllContexts(2, 2, ".");
    assertEquals("Second page should have 2 contexts",
        2,
        page2.size());
    assertEquals("Second page should contain .beta and .delta",
        ImmutableList.of(".beta", ".delta"),
        page2);

    // Test 4: Filter by "." and paginate - offset 4, limit 2
    List<String> page3 = restApp.restClient.getAllContexts(4, 2, ".");
    assertEquals("Third page should have 1 context",
        1,
        page3.size());
    assertEquals("Third page should contain .gamma",
        Collections.singletonList(".gamma"),
        page3);

    // Test 5: Filter by ".a" (should match .alpha) with pagination
    List<String> alphaPage = restApp.restClient.getAllContexts(0, 1, ".a");
    assertEquals("Filtering by '.a' with limit 1 should return .alpha",
        Collections.singletonList(".alpha"),
        alphaPage);
  }

  @Test
  public void testContextPrefixFilterEdgeCases() throws Exception {
    // Create a context
    String subject = ":.test:subject";
    List<String> schemas = TestUtils.getRandomCanonicalAvroString(1);
    restApp.restClient.registerSchema(schemas.get(0), subject);

    // Test 1: Null contextPrefix (should return all contexts)
    List<String> nullFilter = restApp.restClient.getAllContexts(0, -1, null);
    assertEquals("Null contextPrefix should return all contexts",
        ImmutableList.of(DEFAULT_CONTEXT, ".test"),
        nullFilter);

    // Test 2: Very long non-matching prefix
    List<String> longPrefix = restApp.restClient.getAllContexts(
        0, -1, ".thisIsAVeryLongPrefixThatWillNeverMatch");
    assertEquals("Long non-matching prefix should return empty list",
        Collections.emptyList(),
        longPrefix);

    // Test 3: Filter by "." - should return all contexts (all start with ".")
    List<String> dotPrefixCtx = restApp.restClient.getAllContexts(0, -1, ".");
    assertEquals("Prefix '.' should match all contexts as all contexts start with '.'",
        ImmutableList.of(DEFAULT_CONTEXT, ".test"),
        dotPrefixCtx);

    // Test 4: Case-sensitive matching - ".TEST" should not match ".test"
    List<String> upperCaseFilter = restApp.restClient.getAllContexts(0, -1, ".TEST");
    assertEquals("Context prefix filter should be case-sensitive",
        Collections.emptyList(),
        upperCaseFilter);
  }
}
