/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.dekregistry.storage;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.rest.RestConfigException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DekRegistryConfig extends SchemaRegistryConfig {

  private static final Logger log = LoggerFactory.getLogger(DekRegistryConfig.class);

  public static final String DEK_REGISTRY_TOPIC_CONFIG = "dek.registry.topic";
  public static final String DEK_REGISTRY_MAX_KEYS_CONFIG = "dek.registry.max.keys";
  public static final String DEK_REGISTRY_UPDATE_HANDLERS_CONFIG = "dek.registry.update.handlers";

  protected static final String DEK_REGISTRY_TOPIC_DEFAULT = "_dek_registry_keys";
  protected static final int DEK_REGISTRY_MAX_KEYS_DEFAULT = 10000;

  protected static final String DEK_REGISTRY_TOPIC_DOC =
      "The topic used to persist keys for the dek registry.";
  protected static final String DEK_REGISTRY_MAX_KEYS_DOC =
      "Maximum number of keys per tenant.";
  protected static final String DEK_REGISTRY_UPDATE_HANDLERS_DOC =
      "A list of classes to use as CacheUpdateHandler. Implementing the interface "
          + "<code>CacheUpdateHandler</code> allows you to handle Kafka cache update events.";

  private static final ConfigDef serverConfig;

  static {
    serverConfig = baseSchemaRegistryConfigDef()
        .define(DEK_REGISTRY_TOPIC_CONFIG, STRING, DEK_REGISTRY_TOPIC_DEFAULT,
            HIGH, DEK_REGISTRY_TOPIC_DOC)
        .define(DEK_REGISTRY_MAX_KEYS_CONFIG, INT, DEK_REGISTRY_MAX_KEYS_DEFAULT,
            LOW, DEK_REGISTRY_MAX_KEYS_DOC)
        .define(DEK_REGISTRY_UPDATE_HANDLERS_CONFIG, ConfigDef.Type.LIST, "",
            LOW, DEK_REGISTRY_UPDATE_HANDLERS_DOC);
  }

  private static Properties getPropsFromFile(String propsFile) throws RestConfigException {
    Properties props = new Properties();
    if (propsFile == null) {
      return props;
    }

    try (FileInputStream propStream = new FileInputStream(propsFile)) {
      props.load(propStream);
    } catch (IOException e) {
      throw new RestConfigException("Couldn't load properties from " + propsFile, e);
    }

    return props;
  }

  public DekRegistryConfig(String propsFile) throws RestConfigException {
    this(getPropsFromFile(propsFile));
  }

  public DekRegistryConfig(Properties props) throws RestConfigException {
    this(serverConfig, props);
  }

  public DekRegistryConfig(ConfigDef configDef, Properties props) throws RestConfigException {
    super(configDef, props);
  }

  public String topic() {
    return getString(DEK_REGISTRY_TOPIC_CONFIG);
  }

  public int maxKeys() {
    return getInt(DEK_REGISTRY_MAX_KEYS_CONFIG);
  }
}
