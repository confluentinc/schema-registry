/*
 * Copyright 2024 Confluent Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.rest.Application;
import io.confluent.rest.ApplicationServer;
import io.confluent.rest.RestConfig;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configurable;
import jakarta.ws.rs.core.MediaType;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end check that a {@link SchemaRegistryConfig}-driven server actually enforces the
 * rest-utils blanket request timeout (HTTP 504). This closes the gap between the rest-utils
 * handler and the Schema Registry config that enables it.
 *
 * <p>The test is self-guarding: it is skipped unless the rest-utils on the runtime classpath
 * includes {@code RequestTimeoutHandler}. So it stays green in CI against an older pinned
 * rest-utils (auto-skipped) and starts exercising the behavior once the rest-utils dependency
 * is bumped to a build that contains the handler.
 */
public class RequestTimeoutIntegrationTest {

  private ApplicationServer<SchemaRegistryConfig> server;

  @Before
  public void requireTimeoutSupport() {
    Assume.assumeTrue(
        "Requires a rest-utils build that includes io.confluent.rest.handlers.RequestTimeoutHandler",
        classExists("io.confluent.rest.handlers.RequestTimeoutHandler"));
  }

  @After
  public void tearDown() throws Exception {
    SlowResource.release();
    if (server != null) {
      server.stop();
    }
  }

  private void startServer(long requestTimeoutMs) throws Exception {
    Properties props = new Properties();
    props.setProperty(RestConfig.LISTENERS_CONFIG, "http://0.0.0.0:0");
    props.setProperty("request.timeout.ms", String.valueOf(requestTimeoutMs));

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    server = new ApplicationServer<>(config);
    server.registerApplication(new TestApp(config, "/"));
    server.start();
  }

  @Test
  public void slowRequestTimesOutWith504() throws Exception {
    SlowResource.reset();
    startServer(500);

    long start = System.currentTimeMillis();
    int status = makeGetRequest("/slow");
    long elapsed = System.currentTimeMillis() - start;

    assertEquals("slow request should be aborted with a 504", 504, status);
    assertTrue("504 should be returned promptly, took " + elapsed + " ms", elapsed < 5_000);
  }

  @Test
  public void fastRequestIsUnaffected() throws Exception {
    startServer(10_000);
    assertEquals("fast request should succeed normally", 200, makeGetRequest("/fast"));
  }

  private int makeGetRequest(String path) throws Exception {
    int port = localPort();
    HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + port + path)
        .openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(5_000);
    conn.setReadTimeout(30_000);
    try {
      return conn.getResponseCode();
    } finally {
      conn.disconnect();
    }
  }

  private int localPort() {
    for (Connector connector : server.getConnectors()) {
      if (connector instanceof ServerConnector) {
        return ((ServerConnector) connector).getLocalPort();
      }
    }
    throw new IllegalStateException("No ServerConnector found");
  }

  private static boolean classExists(String name) {
    try {
      Class.forName(name, false, RequestTimeoutIntegrationTest.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private static class TestApp extends Application<SchemaRegistryConfig> {
    TestApp(SchemaRegistryConfig config, String path) {
      super(config, path);
    }

    @Override
    public void setupResources(Configurable<?> config, SchemaRegistryConfig appConfig) {
      config.register(SlowResource.class);
    }
  }

  @Path("/")
  @Produces(MediaType.TEXT_PLAIN)
  public static class SlowResource {
    private static volatile CountDownLatch latch = new CountDownLatch(1);

    static void reset() {
      latch = new CountDownLatch(1);
    }

    static void release() {
      latch.countDown();
    }

    @GET
    @Path("/slow")
    public String slow() throws InterruptedException {
      latch.await(60, TimeUnit.SECONDS);
      return "slow";
    }

    @GET
    @Path("/fast")
    public String fast() {
      return "fast";
    }
  }
}
