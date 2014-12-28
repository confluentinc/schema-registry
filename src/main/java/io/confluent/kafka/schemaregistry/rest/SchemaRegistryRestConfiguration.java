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
package io.confluent.kafka.schemaregistry.rest;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.storage.SchemaRegistryConfig;
import io.confluent.rest.Configuration;
import io.confluent.rest.ConfigurationException;

public class SchemaRegistryRestConfiguration extends Configuration {

  public static final String DEFAULT_DEBUG = "false";

  private final boolean debug;
  private final int port;
  private final SchemaRegistryConfig schemaRegistryConfig;

  public SchemaRegistryRestConfiguration(String propsFile) throws ConfigurationException {
    this(getPropsFromFile(propsFile));
  }

  public SchemaRegistryRestConfiguration(Properties props) throws ConfigurationException {
    debug = Boolean.parseBoolean(props.getProperty("debug", DEFAULT_DEBUG));
    port = Integer.parseInt(
        props.getProperty("port", String.valueOf(SchemaRegistryConfig.DEFAULT_PORT)));
    schemaRegistryConfig = new SchemaRegistryConfig(props);
  }

  private static Properties getPropsFromFile(String propsFile) throws ConfigurationException {
    Properties props = new Properties();
    if (propsFile == null) {
      return props;
    }
    try {
      FileInputStream propStream = new FileInputStream(propsFile);
      props.load(propStream);
    } catch (IOException e) {
      throw new ConfigurationException("Couldn't load properties from " + propsFile, e);
    }
    return props;
  }

  @Override
  public boolean getDebug() {
    return debug;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public Iterable<String> getPreferredResponseMediaTypes() {
    return Versions.PREFERRED_RESPONSE_TYPES;
  }

  @Override
  public String getDefaultResponseMediaType() {
    return Versions.SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT;
  }

  public SchemaRegistryConfig getSchemaRegistryConfig() {
    return schemaRegistryConfig;
  }
}
