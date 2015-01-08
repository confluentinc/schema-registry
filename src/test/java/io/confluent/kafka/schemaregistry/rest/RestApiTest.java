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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.utils.RestUtils;
import io.confluent.kafka.schemaregistry.utils.TestUtils;

import static io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel.FORWARD;
import static io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel.NONE;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
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
    assertEquals("Getting all versions from subject1 should return empty",
                 allVersionsInSubject1,
                 RestUtils.getAllVersions(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                          subject1));

    // test getAllSubjects with no existing data
    assertEquals("Getting all subjects should return empty",
                 allSubjects,
                 RestUtils
                     .getAllSubjects(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES));

    // test getVersion on a non-existing subject
    try {
      RestUtils.getVersion(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                           "non-existing-subject", 1);
    } catch (WebApplicationException e) {
      // this is expected.
      assertEquals("Unregistered subject shouldn't be found in getVersion()",
                   Response.Status.NOT_FOUND,
                   e.getResponse().getStatusInfo());
    }

    // test registering and verifying new schemas in subject1
    for (int i = 0; i < schemasInSubject1; i++) {
      String schema = allSchemasInSubject1.get(i);
      int expectedVersion = i + 1;
      TestUtils.registerAndVerifySchema(restApp.restConnect, schema, expectedVersion,
                                        subject1);
      allVersionsInSubject1.add(expectedVersion);
    }
    allSubjects.add(subject1);

    // test getVersion on a non-existing version
    try {
      RestUtils.getVersion(restApp.restConnect,
                           TestUtils.DEFAULT_REQUEST_PROPERTIES, subject1,
                           schemasInSubject1 + 1);
    } catch (WebApplicationException e) {
      // this is expected.
      assertEquals("Unregistered version shouldn't be found", e.getResponse().getStatusInfo(),
                   Response.Status.NOT_FOUND);
    }

    // test re-registering existing schemas
    for (int i = 0; i < schemasInSubject1; i++) {
      int expectedVersion = allVersionsInSubject1.get(i);
      String schemaString = allSchemasInSubject1.get(i);
      assertEquals("Re-registering an existing schema should return the existing version",
                   expectedVersion,
                   TestUtils.registerSchema(restApp.restConnect, schemaString, subject1));
    }

    // test registering schemas in subject2
    for (int i = 0; i < schemasInSubject2; i++) {
      String schema = allSchemasInSubject2.get(i);
      int expectedVersion = i + 1;
      TestUtils.registerAndVerifySchema(restApp.restConnect, schema, expectedVersion,
                                        subject2);
      allVersionsInSubject2.add(expectedVersion);
    }
    allSubjects.add(subject2);

    // test getAllVersions with existing data
    assertEquals("Getting all versions from subject1 should match all registered versions",
                 allVersionsInSubject1,
                 RestUtils.getAllVersions(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                          subject1));
    assertEquals("Getting all versions from subject2 should match all registered versions",
                 allVersionsInSubject2,
                 RestUtils.getAllVersions(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                          subject2));

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
                 allSubjects,
                 RestUtils
                     .getAllSubjects(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES));
  }

  @Test
  public void testConfigDefaults() throws IOException {
    String subject = "testSubject";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);
    assertEquals("Default compatibility level should be none for this test instance",
                 NONE,
                 RestUtils.getConfig(restApp.restConnect,
                                     TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                     null).getCompatibilityLevel());

    // change it to forward
    TestUtils.changeCompatibility(restApp.restConnect, AvroCompatibilityLevel.FORWARD, null);

    assertEquals("New compatibility level should be forward for this test instance",
                 FORWARD,
                 RestUtils.getConfig(restApp.restConnect,
                                     TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                     null).getCompatibilityLevel());

    assertNull("Default compatibility level should not match current top level config for this "
               + "subject",
               RestUtils.getConfig(restApp.restConnect,
                                   TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                   subject).getCompatibilityLevel());

  }

  @Test
  public void testInvalidSubjectConfigChange() throws IOException {
    String subject = "testSubject";
    try {
      TestUtils.changeCompatibility(restApp.restConnect, AvroCompatibilityLevel.FORWARD, subject);
    } catch (WebApplicationException e) {
      fail("Changing config for an invalid subject should succeed");
    }
    assertEquals("New compatibility level for this subject should be forward",
                 FORWARD,
                 RestUtils.getConfig(restApp.restConnect,
                                     TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                     subject).getCompatibilityLevel());
  }

  @Test
  public void testSubjectConfigChange() throws IOException {
    String subject = "testSubject";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);
    assertEquals("Default compatibility level should be none for this test instance",
                 NONE,
                 RestUtils.getConfig(restApp.restConnect,
                                     TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                     null).getCompatibilityLevel());

    // change subject compatibility to forward
    TestUtils.changeCompatibility(restApp.restConnect, AvroCompatibilityLevel.FORWARD, subject);

    assertEquals("Global compatibility level should remain none for this test instance",
                 NONE,
                 RestUtils.getConfig(restApp.restConnect,
                                     TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                     null).getCompatibilityLevel());

    assertEquals("New compatibility level for this subject should be forward",
                 FORWARD,
                 RestUtils.getConfig(restApp.restConnect,
                                     TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                     subject).getCompatibilityLevel());

  }
}
