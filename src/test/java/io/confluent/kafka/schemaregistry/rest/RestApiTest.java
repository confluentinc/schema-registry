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
    String topic1 = "testTopic1";
    String topic2 = "testTopic2";
    List<Integer> allVersionsInTopic1 = new ArrayList<Integer>();
    List<String> allSchemasInTopic1 = new ArrayList<String>();
    List<Integer> allVersionsInTopic2 = new ArrayList<Integer>();
    List<String> allTopics = new ArrayList<String>();

    // test getAllVersions with no existing data
    assertEquals("Getting all versions from topic1 should return empty",
                 RestUtils.getAllVersions(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                          topic1),
                 allVersionsInTopic1);

    // test getAllSubjects with no existing data
    assertEquals("Getting all topics should return empty",
                 RestUtils
                     .getAllSubjects(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES),
                 allTopics);

    // test getVersion on a non-existing topic
    try {
      RestUtils.getVersion(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                           "non-existing-topic", 1);
    } catch (WebApplicationException e) {
      // this is expected.
      assertEquals("Unregistered topic shouldn't be found in getVersion()",
                   e.getResponse().getStatusInfo(),
                   Response.Status.NOT_FOUND);
    }

    // test registering and verifying new schemas in topic1
    int schemasInTopic1 = 10;
    for (int i = 0; i < schemasInTopic1; i++) {
      String schema = topic1 + " schema " + i;
      int expectedVersion = i + 1;
      TestUtils.registerAndVerifySchema(restApp.restConnect, schema, expectedVersion,
                                        topic1);
      allVersionsInTopic1.add(expectedVersion);
      allSchemasInTopic1.add(schema);
    }
    allTopics.add(topic1);

    // test getVersion on a non-existing version
    try {
      RestUtils.getVersion(restApp.restConnect,
                           TestUtils.DEFAULT_REQUEST_PROPERTIES, topic1,
                           schemasInTopic1 + 1);
    } catch (WebApplicationException e) {
      // this is expected.
      assertEquals("Unregistered version shouldn't be found", e.getResponse().getStatusInfo(),
                   Response.Status.NOT_FOUND);
    }

    // test re-registering existing schemas
    for (int i = 0; i < schemasInTopic1; i++) {
      int expectedVersion = i + 1;
      String schemaString = allSchemasInTopic1.get(i);
      assertEquals("Re-registering an existing schema should return the existing version",
                   TestUtils.registerSchema(restApp.restConnect, schemaString, topic1),
                   expectedVersion);
    }

    // test registering schemas in topic2
    int schemasInTopic2 = 5;
    for (int i = 0; i < schemasInTopic2; i++) {
      String schema = topic2 + " schema " + i;
      int expectedVersion = i + 1;
      TestUtils.registerAndVerifySchema(restApp.restConnect, schema, expectedVersion,
                                        topic2);
      allVersionsInTopic2.add(expectedVersion);
    }
    allTopics.add(topic2);

    // test getAllVersions with existing data
    assertEquals("Getting all versions from topic1 should match all registered versions",
                 RestUtils.getAllVersions(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                          topic1),
                 allVersionsInTopic1);
    assertEquals("Getting all versions from topic2 should match all registered versions",
                 RestUtils.getAllVersions(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES,
                                          topic2),
                 allVersionsInTopic2);

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
                 RestUtils
                     .getAllSubjects(restApp.restConnect, TestUtils.DEFAULT_REQUEST_PROPERTIES),
                 allTopics);
  }
}
