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

import javax.ws.rs.WebApplicationException;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityType;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.rest.exceptions.IncompatibleAvroSchemaException;
import io.confluent.kafka.schemaregistry.rest.exceptions.InvalidAvroException;
import io.confluent.kafka.schemaregistry.utils.TestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RestApiCompatibilityTest extends ClusterTestHarness {

  public RestApiCompatibilityTest() {
    super(1, true, AvroCompatibilityType.BACKWARD.name);
  }

  @Test
  public void testCompatibility() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
        .canonicalString;
    int expectedVersionSchema1 = 1;
    assertEquals("Registering should succeed",
                 expectedVersionSchema1,
                 TestUtils.registerSchema(restApp.restConnect, schemaString1, subject));

    // register an incompatible avro
    String incompatibleSchemaString = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}"
    ).canonicalString;
    try {
      TestUtils.registerSchema(restApp.restConnect, incompatibleSchemaString, subject);
      fail("Registering an incompatible schema should fail");
    } catch (WebApplicationException e) {
      // this is expected.
      assertEquals("Should get a conflict status",
                   IncompatibleAvroSchemaException.STATUS.getStatusCode(),
                   e.getResponse().getStatus());
    }

    // register a non-avro
    String nonAvroSchemaString = "non-avro schema string";
    try {
      TestUtils.registerSchema(restApp.restConnect, nonAvroSchemaString, subject);
      fail("Registering a non-avro schema should fail");
    } catch (WebApplicationException e) {
      // this is expected.
      assertEquals("Should get a bad request status",
                   InvalidAvroException.STATUS.getStatusCode(),
                   e.getResponse().getStatus());
    }

    // register a backward compatible avro
    String schemaString2 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}"
    ).canonicalString;
    int expectedVersionSchema2 = 2;
    assertEquals("Registering a compatible schema should succeed",
                 expectedVersionSchema2,
                 TestUtils.registerSchema(restApp.restConnect, schemaString2, subject));
  }
}
