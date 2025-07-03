/*
 * Copyright 2014-2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppInfoParser {

  private static final Logger log = LoggerFactory.getLogger(AppInfoParser.class);

  public static String getCommitId() {
    return readAppProperty("application.commitId", "Unknown");
  }

  public static String getVersion() {
    return readAppProperty("application.version", "Unknown");
  }

  private static String readAppProperty(String propertyName, String defaultValue) {
    String fileName = "/schema-registry-app.properties";
    String propertyValue = defaultValue;
    try (InputStream propFile = AppInfoParser.class.getResourceAsStream(fileName)) {
      if (propFile != null) {
        Properties props = new Properties();
        props.load(propFile);
        propertyValue = props.getProperty(propertyName, defaultValue).trim();
      } else {
        log.error("Cannot find properties file");
      }
    } catch (IOException e) {
      log.warn("Cannot parse properties file", e);
    }
    return propertyValue;
  }

}
