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
import java.util.concurrent.atomic.AtomicReference;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.login.Configuration;
import java.util.Map;
import java.net.URL;

import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Configurable;
import jakarta.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
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

  // Configure the client with authentication
  BasicAuthCredentialProvider basicAuthCredentialProvider = new BasicAuthCredentialProvider() {
    @Override
    public String alias() {
      return "TEST";
    }

    @Override
    public void configure(Map<String, ?> configs) {
      // No configuration needed for test
    }

    @Override
    public String getUserInfo(URL url) {
      return "testuser:testpass";
    }
  };

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
    restApp.restClient.setBasicAuthCredentialProvider(basicAuthCredentialProvider);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, SUBJECT),
        "Registering should succeed"
    );

  }

  @Test
  public void tesRejectResource() throws Exception {
    try {
      restApp.restClient.setBasicAuthCredentialProvider(basicAuthCredentialProvider);
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
    KafkaSchemaRegistry kafkaSchemaRegistry = (KafkaSchemaRegistry) restApp.schemaRegistry();
    Assert.assertEquals(kafkaSchemaRegistry.getCustomHandler().size(), 2);

    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    restApp.restClient.setBasicAuthCredentialProvider(basicAuthCredentialProvider);
    restApp.restClient.registerSchema(schemaString1, SUBJECT);
    // verify extension added handler and it worked
    Assert.assertEquals(kafkaSchemaRegistry.getCustomHandler().size(), 3);

  }

  @Test
  public void testSecurityHandlerBeforeCustomHandler() throws Exception {
    // This test validates that the security handler is placed before custom handlers
    // in the handler chain to ensure authentication happens before custom logic

    AtomicReference<TestSecurityOrderHandler> testHandlerRef = new AtomicReference<>();
    // Get the schema registry and find our test handler
    KafkaSchemaRegistry kafkaSchemaRegistry = (KafkaSchemaRegistry) restApp.schemaRegistry();
    kafkaSchemaRegistry.getCustomHandler().forEach(handler -> {
      if (handler instanceof TestSecurityOrderHandler) {
        testHandlerRef.set((TestSecurityOrderHandler) handler);
      }
    });

    TestSecurityOrderHandler testHandler = testHandlerRef.get();
    Assert.assertNotNull("TestSecurityOrderHandler should be found in custom handlers", testHandler);

    // Reset the handler call tracking
    testHandler.reset();

    String schemaString = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();

    // Make a request without authentication - should be blocked by security handler
    // before reaching the custom handler
    try {
      restApp.restClient.registerSchema(schemaString, SUBJECT);
      fail("Request without authentication should be blocked by security handler");
    } catch (RestClientException rce) {
      assertEquals(401, rce.getStatus(), "Should get 401 from security handler");

      // Verify that our custom handler was NOT called because security blocked it first
      Assert.assertFalse("Custom handler should not be called when security blocks request",
                        testHandler.wasHandlerCalled());
    }

    // Reset for next test
    testHandler.reset();

    // Now make an authenticated request that should pass security and reach our custom handler
    restApp.restClient.setBasicAuthCredentialProvider(basicAuthCredentialProvider);

    restApp.restClient.registerSchema(schemaString, SUBJECT);

    // Verify that our custom handler was called for the authenticated request
    Assert.assertTrue("Custom handler should be called for authenticated requests that pass security",
                     testHandler.wasHandlerCalled());
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.put(
        SchemaRegistryConfig.RESOURCE_EXTENSION_CONFIG,
        TestSchemaRegistryExtension.class.getName()
                + "," + TestSchemaRegistryHandlerExtension.class.getName()
    );

    // Add BASIC authentication for the security handler test
    props.put(SchemaRegistryConfig.AUTHENTICATION_METHOD_CONFIG,
              SchemaRegistryConfig.AUTHENTICATION_METHOD_BASIC);
    props.put(SchemaRegistryConfig.AUTHENTICATION_REALM_CONFIG, "SchemaRegistry-Test");
    props.put(SchemaRegistryConfig.AUTHENTICATION_ROLES_CONFIG, "admin");

    try {
      // Create a temporary JAAS config file for testing
      File jaasConfigFile = File.createTempFile("test-jaas-", ".conf");
      jaasConfigFile.deleteOnExit();

      // Create a temporary user properties file
      File userPropsFile = File.createTempFile("test-users-", ".properties");
      userPropsFile.deleteOnExit();
      Files.write(userPropsFile.toPath(),
                  "testuser: testpass,admin\n".getBytes(StandardCharsets.UTF_8));

      // Create JAAS configuration
      List<String> jaasLines = new ArrayList<>();
      jaasLines.add("SchemaRegistry-Test {");
      jaasLines.add("  org.eclipse.jetty.security.jaas.spi.PropertyFileLoginModule required");
      jaasLines.add("  file=\"" + userPropsFile.getAbsolutePath() + "\";");
      jaasLines.add("};");
      Files.write(jaasConfigFile.toPath(), jaasLines, StandardCharsets.UTF_8);

      System.setProperty("java.security.auth.login.config", jaasConfigFile.getAbsolutePath());
      Configuration.setConfiguration(null); // Force reload

    } catch (Exception e) {
      throw new RuntimeException("Failed to setup authentication", e);
    }

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
      KafkaSchemaRegistry kafkaSchemaRegistry = (KafkaSchemaRegistry) schemaRegistry;
      // Create a custom handler that tracks when it's called
      TestSecurityOrderHandler testHandler = new TestSecurityOrderHandler();
      kafkaSchemaRegistry.addCustomHandler(testHandler);
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

  // Helper class to track whether a custom handler was called
  public static class TestSecurityOrderHandler extends Handler.Wrapper {
    private volatile boolean handlerCalled = false;

    @Override
    public boolean handle(Request request, org.eclipse.jetty.server.Response response, Callback callback) throws Exception {
      handlerCalled = true;
      return super.handle(request, response, callback);
    }

    public boolean wasHandlerCalled() {
      return handlerCalled;
    }

    public void reset() {
      handlerCalled = false;
    }
  }
}
