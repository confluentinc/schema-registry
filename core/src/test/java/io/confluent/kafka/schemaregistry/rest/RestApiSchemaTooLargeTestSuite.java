/*
 * Copyright 2022 Confluent Inc.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.SchemaRegistryTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import org.junit.jupiter.api.Test;

/**
 * Interface for schema-too-large REST API integration tests with default test implementations.
 * Implementing classes must provide test harness implementation.
 */
public interface RestApiSchemaTooLargeTestSuite {

  SchemaRegistryTestHarness getHarness();
  
  default RestApp restApp() {
    return getHarness().getRestApp();
  }

  String schema =
      "{\"namespace\": \"namespace\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"test\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"null\", \"type\": \"null\"},\n"
          + "     {\"name\": \"boolean\", \"type\": \"boolean\"},\n"
          + "     {\"name\": \"int\", \"type\": \"int\"},\n"
          + "     {\"name\": \"long\", \"type\": \"long\"},\n"
          + "     {\"name\": \"float\", \"type\": \"float\"},\n"
          + "     {\"name\": \"double\", \"type\": \"double\"},\n"
          + "     {\"name\": \"bytes\", \"type\": \"bytes\"},\n"
          + "     {\"name\": \"string\", \"type\": \"string\", \"aliases\": [\"string_alias\"]},\n"
          + "     {\"name\": \"null_default\", \"type\": \"null\", \"default\": null},\n"
          + "     {\"name\": \"boolean_default\", \"type\": \"boolean\", \"default\": false},\n"
          + "     {\"name\": \"int_default\", \"type\": \"int\", \"default\": 24},\n"
          + "     {\"name\": \"long_default\", \"type\": \"long\", \"default\": 4000000000},\n"
          + "     {\"name\": \"float_default\", \"type\": \"float\", \"default\": 12.3},\n"
          + "     {\"name\": \"double_default\", \"type\": \"double\", \"default\": 23.2},\n"
          + "     {\"name\": \"bytes_default\", \"type\": \"bytes\", \"default\": \"bytes\"},\n"
          + "     {\"name\": \"string_default\", \"type\": \"string\", \"default\": "
          + "\"default string\"}\n"
          + "]\n"
          + "}";

  @Test
  default void testSchemaTooLarge() throws Exception {
    String subject = "testTopic1";
    try {
      restApp().restClient.registerSchema(schema, subject);
      fail("Registering a schema should return " + Errors.SUBJECT_NOT_FOUND_ERROR_CODE);
    } catch (RestClientException e) {
      assertEquals(422, e.getStatus());
      assertEquals(Errors.SCHEMA_TOO_LARGE_ERROR_CODE, e.getErrorCode());
    }
  }
}

