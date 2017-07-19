/*
 * Copyright 2017 Confluent Inc.
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
import java.util.Properties;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Configurable;
import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SchemaRegistryExtensionTest extends ClusterTestHarness {

  public SchemaRegistryExtensionTest() {
    super(1, true, AvroCompatibilityLevel.BACKWARD.name);
  }

  @Test
  public void testBasic() throws Exception {
    String subject = "testSubject";

    String schemaString1 = AvroUtils.parseSchema(
        "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
        .canonicalString;

    restApp.restClient.registerSchema(schemaString1, subject);
    int expectedIdSchema1 = 1;
    assertEquals(
        "Registering should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject)
    );

    try {
      restApp.restClient.getAllVersions(subject);
      fail("Getting all versions from non-existing subject1 should fail with "
           + Errors.SUBJECT_NOT_FOUND_ERROR_CODE
           + " (subject not found)");
    } catch (RestClientException rce) {
      assertEquals(
          "Should get a 401 status for GET operations",
          401,
          rce.getStatus()
      );
    }
  }

  protected Properties gerSchemaRegistryProperties() {
    Properties props = new Properties();
    props.put(
        SchemaRegistryConfig.SCHEMAREGISTRY_RESOURCE_EXTENSION_CONFIG,
        TestSchemaRegistryExtension.class.getName()
    );
    return props;
  }

  public static class TestSchemaRegistryExtension implements SchemaRegistryResourceExtension {

    @Override
    public void register(
        Configurable<?> config,
        SchemaRegistryConfig schemaRegistryConfig,
        KafkaSchemaRegistry schemaRegistry
    ) {
      config.register(new ContainerRequestFilter() {

        @Override
        public void filter(ContainerRequestContext requestContext) throws IOException {
          if (!requestContext.getMethod().equalsIgnoreCase("POST")) {
            requestContext.abortWith(
                Response.status(Response.Status.UNAUTHORIZED)
                    .entity("User cannot access the resource.")
                    .build());
          }
        }
      });
    }

    @Override
    public void clean() {

    }
  }
}
