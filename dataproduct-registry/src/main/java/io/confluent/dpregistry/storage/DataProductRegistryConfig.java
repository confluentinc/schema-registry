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

package io.confluent.dpregistry.storage;

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

public class DataProductRegistryConfig extends SchemaRegistryConfig {

  private static final Logger log = LoggerFactory.getLogger(DataProductRegistryConfig.class);

  public static final String DATAPRODUCT_REGISTRY_TOPIC_CONFIG = "dataproduct.registry.topic";
  public static final String DATAPRODUCT_REGISTRY_UPDATE_HANDLERS_CONFIG =
      "dataproduct.registry.update.handlers";

  protected static final String DATAPRODUCT_REGISTRY_TOPIC_DEFAULT = "_dp_registry";

  protected static final String DATAPRODUCT_NAME_SEARCH_DEFAULT_LIMIT_CONFIG =
      "dataproduct.name.search.default.limit";
  protected static final int DATAPRODUCT_NAME_SEARCH_DEFAULT_LIMIT_DEFAULT = Integer.MAX_VALUE;

  protected static final String DATAPRODUCT_NAME_SEARCH_MAX_LIMIT_CONFIG =
      "dataproduct.name.search.max.limit";
  protected static final int DATAPRODUCT_NAME_SEARCH_MAX_LIMIT_DEFAULT = Integer.MAX_VALUE;

  protected static final String DATAPRODUCT_VERSION_SEARCH_DEFAULT_LIMIT_CONFIG =
      "dataproduct.version.search.default.limit";
  protected static final int DATAPRODUCT_VERSION_SEARCH_DEFAULT_LIMIT_DEFAULT = Integer.MAX_VALUE;

  protected static final String DATAPRODUCT_VERSION_SEARCH_MAX_LIMIT_CONFIG =
      "dataproduct.version.search.max.limit";
  protected static final int DATAPRODUCT_VERSION_SEARCH_MAX_LIMIT_DEFAULT = Integer.MAX_VALUE;

  protected static final String DATAPRODUCT_REGISTRY_TOPIC_DOC =
      "The topic used to persist data products for the data product registry.";
  protected static final String DATAPRODUCT_REGISTRY_UPDATE_HANDLERS_DOC =
      "A list of classes to use as CacheUpdateHandler. Implementing the interface "
          + "<code>CacheUpdateHandler</code> allows you to handle Kafka cache update events.";
  protected static final String DATAPRODUCT_NAME_SEARCH_DEFAULT_LIMIT_DOC =
      "The default limit for data product searches by name.";
  protected static final String DATAPRODUCT_NAME_SEARCH_MAX_LIMIT_DOC =
      "The max limit for data product searches by name.";
  protected static final String DATAPRODUCT_VERSION_SEARCH_DEFAULT_LIMIT_DOC =
      "The default limit for data product searches by version.";
  protected static final String DATAPRODUCT_VERSION_SEARCH_MAX_LIMIT_DOC =
      "The max limit for data product searches by version.";

  private static final ConfigDef serverConfig;

  static {
    serverConfig = baseSchemaRegistryConfigDef()
        .define(DATAPRODUCT_REGISTRY_TOPIC_CONFIG, STRING, DATAPRODUCT_REGISTRY_TOPIC_DEFAULT,
            HIGH, DATAPRODUCT_REGISTRY_TOPIC_DOC)
        .define(DATAPRODUCT_NAME_SEARCH_DEFAULT_LIMIT_CONFIG, INT,
            DATAPRODUCT_NAME_SEARCH_DEFAULT_LIMIT_DEFAULT,
            LOW, DATAPRODUCT_NAME_SEARCH_DEFAULT_LIMIT_DOC)
        .define(DATAPRODUCT_NAME_SEARCH_MAX_LIMIT_CONFIG, INT,
            DATAPRODUCT_NAME_SEARCH_MAX_LIMIT_DEFAULT,
            LOW, DATAPRODUCT_NAME_SEARCH_MAX_LIMIT_DOC)
        .define(DATAPRODUCT_VERSION_SEARCH_DEFAULT_LIMIT_CONFIG, INT,
            DATAPRODUCT_VERSION_SEARCH_DEFAULT_LIMIT_DEFAULT,
            LOW, DATAPRODUCT_VERSION_SEARCH_DEFAULT_LIMIT_DOC)
        .define(DATAPRODUCT_VERSION_SEARCH_MAX_LIMIT_CONFIG, INT,
            DATAPRODUCT_VERSION_SEARCH_MAX_LIMIT_DEFAULT,
            LOW, DATAPRODUCT_VERSION_SEARCH_MAX_LIMIT_DOC)
        .define(DATAPRODUCT_REGISTRY_UPDATE_HANDLERS_CONFIG, ConfigDef.Type.LIST, "",
            LOW, DATAPRODUCT_REGISTRY_UPDATE_HANDLERS_DOC);
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

  public DataProductRegistryConfig(String propsFile) throws RestConfigException {
    this(getPropsFromFile(propsFile));
  }

  public DataProductRegistryConfig(Properties props) throws RestConfigException {
    this(serverConfig, props);
  }

  public DataProductRegistryConfig(ConfigDef configDef, Properties props)
      throws RestConfigException {
    super(configDef, props);
  }

  public String topic() {
    return getString(DATAPRODUCT_REGISTRY_TOPIC_CONFIG);
  }
}
