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
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.Callback;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SchemaRegistryExtensionTest extends ClusterTestHarness {
  private static final String SUBJECT = "testSubject";

  public SchemaRegistryExtensionTest() {
    super(1, true, CompatibilityLevel.BACKWARD.name);
  }

  @Test
  public void testAllowResource() throws Exception {

    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, SUBJECT),
        "Registering should succeed"
    );

  }

  @Test
  public void tesRejectResource() throws Exception {
    try {
      restApp.restClient.getLatestVersion(SUBJECT);
      fail("Getting all versions from non-existing subject1 should fail with 401");
    } catch (RestClientException rce) {
      assertEquals(
          401,
          rce.getStatus(),
          "Should get a 401 status for GET operations"
      );
    }
  }


  @Test
  public void testExtensionAddedHandler() throws Exception {
    SchemaRegistry kafkaSchemaRegistry = restApp.schemaRegistry();
    Assert.assertEquals(kafkaSchemaRegistry.getCustomHandler().size(), 1);

    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    restApp.restClient.registerSchema(schemaString1, SUBJECT);
    // verify extension added handler and it worked
    Assert.assertEquals(kafkaSchemaRegistry.getCustomHandler().size(), 2);

  }


  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.put(
        SchemaRegistryConfig.RESOURCE_EXTENSION_CONFIG,
        TestSchemaRegistryExtension.class.getName()
                + "," + TestSchemaRegistryHandlerExtension.class.getName()
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
      // testing method, no need to implement
    }
  }


  public static class TestSchemaRegistryHandlerExtension implements SchemaRegistryResourceExtension {

    @Override
    public void register(
            Configurable<?> config,
            SchemaRegistryConfig schemaRegistryConfig,
            SchemaRegistry schemaRegistry
    ) {
      SchemaRegistry kafkaSchemaRegistry = schemaRegistry;
      kafkaSchemaRegistry.addCustomHandler(new Handler.Wrapper() {
        @Override
        public boolean handle(Request request, org.eclipse.jetty.server.Response response, Callback callback) throws Exception {
          // adding another handler so we can assert handler is indeed added into the handler chain based on number of handler
          kafkaSchemaRegistry.addCustomHandler(new Handler.Wrapper());
          super.handle(request, response, callback);
          return true;
        }
      });
    }

    @Override
    public void close() {
      // testing method, no need to implement
    }
  }
}
