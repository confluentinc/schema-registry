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
package io.confluent.kafka.schemaregistry.zookeeper;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;

import javax.ws.rs.WebApplicationException;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.utils.RestUtils;
import io.confluent.kafka.schemaregistry.utils.TestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MasterElectorTest extends ClusterTestHarness {

  @Test
  public void testAutoFailover() throws Exception {
    final int ID_BATCH_SIZE = 20;
    final String subject = "testTopic";
    List<String> avroSchemas = TestUtils.getRandomCanonicalAvroString(4);

    // create schema registry instance 1
    final RestApp restApp1 = new RestApp(kafka.utils.TestUtils.choosePort(),
                                         zkConnect, KAFKASTORE_TOPIC);
    restApp1.start();

    // create schema registry instance 2
    final RestApp restApp2 = new RestApp(kafka.utils.TestUtils.choosePort(),
                                         zkConnect, KAFKASTORE_TOPIC);
    restApp2.start();
    assertTrue("Schema registry instance 1 should be the master", restApp1.isMaster());
    assertFalse("Schema registry instance 2 shouldn't be the master", restApp2.isMaster());
    assertEquals("Instance 2's master should be instance 1",
                 restApp1.myIdentity(), restApp2.masterIdentity());

    // test registering a schema to the master and finding it on the expected version
    final String firstSchema = avroSchemas.get(0);
    final int firstSchemaExpectedId = 0;
    TestUtils.registerAndVerifySchema(restApp1.restConnect, firstSchema, firstSchemaExpectedId,
                                      subject);
    // the newly registered schema should be eventually readable on the non-master
    waitUntilIdExists(restApp2.restConnect, firstSchemaExpectedId, firstSchema,
                      "Registered schema should be found on the non-master");

    // test registering a schema to the non-master and finding it on the expected version
    final String secondSchema = avroSchemas.get(1);
    final int secondSchemaExpectedId = 1;
    final int secondSchemaExpectedVersion = 2;
    assertEquals("Registering a new schema to the non-master should succeed",
                 secondSchemaExpectedId,
                 TestUtils.registerSchema(restApp2.restConnect, secondSchema, subject));

    // the newly registered schema should be immediately readable on the master using the id
    assertEquals("Registered schema should be found on the master",
                 secondSchema,
                 RestUtils.getId(restApp1.restConnect, RestUtils.DEFAULT_REQUEST_PROPERTIES,
                                 secondSchemaExpectedId).getSchema());

    // the newly registered schema should be immediately readable on the master using the version
    assertEquals("Registered schema should be found on the master",
                 secondSchema,
                 RestUtils.getVersion(restApp1.restConnect,
                                      RestUtils.DEFAULT_REQUEST_PROPERTIES, subject,
                                      secondSchemaExpectedVersion).getSchema());

    // the newly registered schema should be eventually readable on the non-master
    waitUntilIdExists(restApp2.restConnect, secondSchemaExpectedId, secondSchema,
                      "Registered schema should be found on the non-master");

    // test registering an existing schema to the master
    assertEquals("Registering an existing schema to the master should return its id",
                 secondSchemaExpectedId,
                 TestUtils.registerSchema(restApp1.restConnect, secondSchema, subject));

    // test registering an existing schema to the non-master
    assertEquals("Registering an existing schema to the non-master should return its id",
                 secondSchemaExpectedId,
                 TestUtils.registerSchema(restApp2.restConnect, secondSchema, subject));

    // fake an incorrect master and registration should fail
    restApp1.setMaster(null);
    int statusCodeFromRestApp1 = 0;
    try {
      TestUtils.registerSchema(restApp1.restConnect, "failed schema", subject);
      fail("Registration should fail on the master");
    } catch (WebApplicationException e) {
      // this is expected.
      statusCodeFromRestApp1 = e.getResponse().getStatus();
    }

    int statusCodeFromRestApp2 = 0;
    try {
      TestUtils.registerSchema(restApp2.restConnect, "failed schema", subject);
      fail("Registration should fail on the non-master");
    } catch (WebApplicationException e) {
      // this is expected.
      statusCodeFromRestApp2 = e.getResponse().getStatus();
    }

    assertEquals("Status code from a non-master rest app for register schema should be 500",
                 500, statusCodeFromRestApp1);
    assertEquals("Error code from the master and the non-master should be the same",
                 statusCodeFromRestApp1, statusCodeFromRestApp2);

    // set the correct master identity back
    restApp1.setMaster(restApp1.myIdentity());

    // registering a schema to the master
    final String thirdSchema = avroSchemas.get(2);
    final int thirdSchemaExpectedVersion = 3;
    final int thirdSchemaExpectedId = ID_BATCH_SIZE;
    assertEquals("Registering a new schema to the master should succeed",
                 thirdSchemaExpectedId,
                 TestUtils.registerSchema(restApp1.restConnect, thirdSchema, subject));

    // stop schema registry instance 1; instance 2 should become the new master
    restApp1.stop();
    Callable<Boolean> condition = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return restApp2.isMaster();
      }
    };
    TestUtils.waitUntilTrue(condition, 5000,
                            "Schema registry instance 2 should become the master");

    // the latest version should be immediately available on the new master using the id
    assertEquals("Latest version should be found on the new master",
                 thirdSchema,
                 RestUtils.getId(restApp2.restConnect, RestUtils.DEFAULT_REQUEST_PROPERTIES,
                                 thirdSchemaExpectedId).getSchema());

    // the latest version should be immediately available on the new master using the version
    assertEquals("Latest version should be found on the new master",
                 thirdSchema,
                 RestUtils.getVersion(restApp2.restConnect,
                                      RestUtils.DEFAULT_REQUEST_PROPERTIES, subject,
                                      thirdSchemaExpectedVersion).getSchema());

    // register a schema to the new master
    final String fourthSchema = avroSchemas.get(3);
    final int fourthSchemaExpectedId = 2 * ID_BATCH_SIZE;
    TestUtils.registerAndVerifySchema(restApp2.restConnect, fourthSchema,
                                      fourthSchemaExpectedId,
                                      subject);

    restApp2.stop();
  }

  private void waitUntilIdExists(final String baseUrl, final int expectedId,
                                 final String expectedSchemaString, String errorMsg) {
    Callable<Boolean> condition = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          Schema schema = RestUtils.getId(baseUrl,
                                          RestUtils.DEFAULT_REQUEST_PROPERTIES, expectedId);
          return expectedSchemaString.compareTo(schema.getSchema()) == 0;
        } catch (WebApplicationException e) {
          return false;
        }
      }
    };
    TestUtils.waitUntilTrue(condition, 5000, errorMsg);
  }
}
