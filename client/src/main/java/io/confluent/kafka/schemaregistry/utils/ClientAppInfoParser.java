/*
 * Copyright 2025 Confluent Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class to read Schema Registry client application information
 * such as version and commit ID at runtime.
 */
public class ClientAppInfoParser {

  private static final Logger log = LoggerFactory.getLogger(ClientAppInfoParser.class);

  /**
   * Returns the version of the Schema Registry client.
   *
   * @return The version string, or "unknown" if not available
   */
  public static String getVersion() {
    return readAppProperty("application.version", "unknown");
  }

  /**
   * Builds the Confluent-Client-Version header value.
   * Format: "java/{version}"
   *
   * @return The client version string
   */
  public static String getClientVersion() {
    String version = getVersion();
    return String.format("java/%s", version);
  }

  private static String readAppProperty(String propertyName, String defaultValue) {
    String fileName = "/schema-registry-client-app.properties";
    String propertyValue = defaultValue;
    try (InputStream propFile = ClientAppInfoParser.class.getResourceAsStream(fileName)) {
      if (propFile != null) {
        Properties props = new Properties();
        props.load(propFile);
        propertyValue = props.getProperty(propertyName, defaultValue).trim();
      } else {
        log.debug("Cannot find client properties file: {}", fileName);
      }
    } catch (IOException e) {
      log.warn("Cannot parse client properties file", e);
    }
    return propertyValue;
  }

}