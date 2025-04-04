/*
 * Copyright 2018 Confluent Inc.
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

import java.io.IOException;
import java.util.Properties;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Configurable;
import jakarta.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SchemaRegistryExtensionTest extends ClusterTestHarness {

  public SchemaRegistryExtensionTest() {
    super(1, true, CompatibilityLevel.BACKWARD.name);
  }

  @Test
  public void testAllowResource() throws Exception {
    String subject = "testSubject";

    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

  }

  @Test
  public void tesRejectResource() throws Exception {
    String subject = "testSubject";

    try {
      restApp.restClient.getLatestVersion(subject);
      fail("Getting all versions from non-existing subject1 should fail with 401");
    } catch (RestClientException rce) {
      assertEquals(
          401,
          rce.getStatus(),
          "Should get a 401 status for GET operations"
      );
    }
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.put(
        SchemaRegistryConfig.RESOURCE_EXTENSION_CONFIG,
        TestSchemaRegistryExtension.class.getName()
    );
    return props;
  }

  public static class TestSchemaRegistryExtension implements SchemaRegistryResourceExtension {

    @Override
    public void register(
        Configurable<?> config,
        SchemaRegistryConfig schemaRegistryConfig,
        SchemaRegistry schemaRegistry
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
    public void close() {

    }
  }
}
