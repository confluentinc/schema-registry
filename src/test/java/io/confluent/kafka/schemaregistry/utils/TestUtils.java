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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.rest.Versions;
import io.confluent.kafka.schemaregistry.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.rest.entities.requests.RegisterSchemaRequest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * For general utility methods used in unit tests.
 */
public class TestUtils {

  public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;

  static {
    DEFAULT_REQUEST_PROPERTIES = new HashMap<String, String>();
    DEFAULT_REQUEST_PROPERTIES.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
  }

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
      do {
        Boolean state = callable.call();
        if (state) {
          return;
        }
        if (System.currentTimeMillis() > startTime + timeoutMs) {
          fail(errorMsg);
        }
        Thread.sleep(50);
      } while (true);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  public static int registerSchema(String baseUrl, String schemaString, String subject)
      throws IOException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);

    Map<String, String> requestProperties = new HashMap<String, String>();
    requestProperties.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);

    return RestUtils.registerSchema(baseUrl, requestProperties, request, subject);
  }

  public static void changeCompatibility(String baseUrl,
                                         AvroCompatibilityLevel newCompatibilityLevel,
                                         String subject)
      throws IOException {
    ConfigUpdateRequest request = new ConfigUpdateRequest();
    request.setCompatibilityLevel(newCompatibilityLevel);

    Map<String, String> requestProperties = new HashMap<String, String>();
    requestProperties.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);

    RestUtils.updateConfig(baseUrl, requestProperties, request, subject);
  }

  /**
   * Register a new schema and verify that it can be found on the expected version.
   */
  public static void registerAndVerifySchema(String baseUrl, String schemaString,
                                             int expectedVersion, String subject)
      throws IOException {
    assertEquals("Registering a new schema should succeed",
                 expectedVersion,
                 TestUtils.registerSchema(baseUrl, schemaString, subject));

    // the newly registered schema should be immediately readable on the master
    assertEquals("Registered schema should be found",
                 schemaString,
                 RestUtils.getVersion(baseUrl, TestUtils.DEFAULT_REQUEST_PROPERTIES, subject,
                                      expectedVersion)
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
