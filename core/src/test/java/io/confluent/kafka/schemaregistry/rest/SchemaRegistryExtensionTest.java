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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Configurable;
import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class SchemaRegistryExtensionTest extends ClusterTestHarness {

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { SchemaRegistryConfig.RESOURCE_EXTENSION_CONFIG },
        { SchemaRegistryConfig.SCHEMAREGISTRY_RESOURCE_EXTENSION_CONFIG },
        { SchemaRegistryConfig.INIT_RESOURCE_EXTENSION_CONFIG}
    });
  }

  @Parameter
  public String resourceExtensionConfigName;

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
        "Registering should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject)
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
          "Should get a 401 status for GET operations",
          401,
          rce.getStatus()
      );
    }
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.put(
        resourceExtensionConfigName,
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
