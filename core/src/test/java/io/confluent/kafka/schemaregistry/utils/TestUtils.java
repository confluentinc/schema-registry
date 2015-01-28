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
package io.confluent.kafka.schemaregistry.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.rest.entities.requests.ConfigUpdateRequest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * For general utility methods used in unit tests.
 */
public class TestUtils {

  private static final String IoTmpDir = System.getProperty("java.io.tmpdir");
  private static final Random random = new Random();

  /**
   * Create a temporary directory
   */
  public static File tempDir(String namePrefix) {
    final File f = new File(IoTmpDir, namePrefix + "-" + random.nextInt(1000000));
    f.mkdirs();
    f.deleteOnExit();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        rm(f);
      }
    });
    return f;
  }

  /**
   * Recursively delete the given file/directory and any subfiles (if any exist)
   *
   * @param file The root file at which to begin deleting
   */
  public static void rm(File file) {
    if (file == null) {
      return;
    } else if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File f : files) {
          rm(f);
        }
      }
    } else {
      file.delete();
    }
  }

  /**
   * Wait until a callable returns true or the timeout is reached.
   */
  public static void waitUntilTrue(Callable<Boolean> callable, long timeoutMs, String errorMsg) {
    try {
      long startTime = System.currentTimeMillis();
      Boolean state = false;
      do {
        state = callable.call();
        if (System.currentTimeMillis() > startTime + timeoutMs) {
          fail(errorMsg);
        }
        Thread.sleep(50);
      } while (!state);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  public static int registerSchema(String baseUrl, String schemaString, String subject)
      throws IOException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);

    return RestUtils.registerSchema(
        baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, request, subject);
  }

  public static int registerSchemaDryRun(String baseUrl, String schemaString, String subject)
      throws IOException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);

    return RestUtils.registerSchemaDryRun(
        baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, request, subject);
  }

  public static boolean testCompatibility(String baseUrl, String schemaString, String subject,
                                          String version)
      throws IOException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);

    return RestUtils.testCompatibility(
        baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, request, subject, version);
  }

  public static Schema getId(String baseUrl, int id)
      throws IOException {
    return RestUtils.getId(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, id);
  }

  /**
   * Helper method which checks the number of versions registered under the given subject.
   */
  public static void checkNumberOfVersions(String baseUrl, int expected, String subject)
      throws IOException {
    List<Integer> versions = RestUtils.getAllVersions(baseUrl,
                                                      RestUtils.DEFAULT_REQUEST_PROPERTIES,
                                                      subject);
    assertEquals("Expected " + expected + " registered versions under subject " + subject +
                 ", but found " + versions.size(),
                 expected, versions.size());
  }

  public static void changeCompatibility(String baseUrl,
                                         AvroCompatibilityLevel newCompatibilityLevel,
                                         String subject)
      throws IOException {
    ConfigUpdateRequest request = new ConfigUpdateRequest();
    request.setCompatibilityLevel(newCompatibilityLevel);

    RestUtils.updateConfig(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, request, subject);
  }

  /**
   * Register a new schema and verify that it can be found on the expected version.
   */
  public static void registerAndVerifySchema(String baseUrl, String schemaString,
                                             int expectedId, String subject)
      throws IOException {
    assertEquals("Registering a new schema should succeed",
                 expectedId,
                 TestUtils.registerSchema(baseUrl, schemaString, subject));

    // the newly registered schema should be immediately readable on the master
    assertEquals("Registered schema should be found",
                 schemaString,
                 RestUtils.getId(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, expectedId)
                     .getSchema());
  }

  public static List<String> getRandomCanonicalAvroString(int num) {
    List<String> avroStrings = new ArrayList<String>();

    for (int i = 0; i < num; i++) {
      String schemaString = "{\"type\":\"record\","
                            + "\"name\":\"myrecord\","
                            + "\"fields\":"
                            + "[{\"type\":\"string\",\"name\":"
                            + "\"f" + random.nextInt(Integer.MAX_VALUE) + "\"}]}";
      avroStrings.add(AvroUtils.parseSchema(schemaString).canonicalString);
    }
    return avroStrings;
  }
}
