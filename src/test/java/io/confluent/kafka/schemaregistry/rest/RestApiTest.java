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

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.utils.RestUtils;
import io.confluent.kafka.schemaregistry.utils.TestUtils;

import static org.junit.Assert.assertEquals;

public class RestApiTest extends ClusterTestHarness {

  public RestApiTest() {
    super(1, true);
  }

  @Test
  public void testBasic() throws Exception {
    String subject1 = "testTopic1";
    String subject2 = "testTopic2";
    List<Integer> allVersionsInSubject1 = new ArrayList<Integer>();
    List<String> allSchemasInSubject1 = new ArrayList<String>();
    List<Integer> allVersionsInSubject2 = new ArrayList<Integer>();
    List<String> allSubjects = new ArrayList<String>();

    // test getAllVersions with no existing data
    assertEquals("Getting all versions from subject1 should return empty",
                 RestUtils.getAllVersions(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                          subject1),
                 allVersionsInSubject1);

    // test getAllSubjects with no existing data
    assertEquals("Getting all subjects should return empty",
                 RestUtils
                     .getAllSubjects(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES),
                 allSubjects);

    // test getVersion on a non-existing subject
    try {
      RestUtils.getVersion(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                           "non-existing-subject", 1);
    } catch (WebApplicationException e) {
      // this is expected.
      assertEquals("Unregistered subject shouldn't be found in getVersion()",
                   e.getResponse().getStatusInfo(),
                   Response.Status.NOT_FOUND);
    }

    // test registering and verifying new schemas in subject1
    int schemasInSubject1 = 10;
    for (int i = 0; i < schemasInSubject1; i++) {
      String schema = subject1 + " schema " + i;
      int expectedVersion = i + 1;
      TestUtils.registerAndVerifySchema(restApp.restConnect, schema, expectedVersion,
                                        subject1);
      allVersionsInSubject1.add(expectedVersion);
      allSchemasInSubject1.add(schema);
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
      int expectedVersion = i + 1;
      String schemaString = allSchemasInSubject1.get(i);
      assertEquals("Re-registering an existing schema should return the existing version",
                   TestUtils.registerSchema(restApp.restConnect, schemaString, subject1),
                   expectedVersion);
    }

    // test registering schemas in subject2
    int schemasInSubject2 = 5;
    for (int i = 0; i < schemasInSubject2; i++) {
      String schema = subject2 + " schema " + i;
      int expectedVersion = i + 1;
      TestUtils.registerAndVerifySchema(restApp.restConnect, schema, expectedVersion,
                                        subject2);
      allVersionsInSubject2.add(expectedVersion);
    }
    allSubjects.add(subject2);

    // test getAllVersions with existing data
    assertEquals("Getting all versions from subject1 should match all registered versions",
                 RestUtils.getAllVersions(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                          subject1),
                 allVersionsInSubject1);
    assertEquals("Getting all versions from subject2 should match all registered versions",
                 RestUtils.getAllVersions(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                          subject2),
                 allVersionsInSubject2);

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
                 RestUtils
                     .getAllSubjects(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES),
                 allSubjects);
  }
}
