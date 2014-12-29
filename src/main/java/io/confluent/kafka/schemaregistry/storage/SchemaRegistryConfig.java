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
package io.confluent.kafka.schemaregistry.storage;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.rest.Versions;
import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;

import static io.confluent.common.config.ConfigDef.Range.atLeast;

public class SchemaRegistryConfig extends RestConfig {

  public static final String KAFKASTORE_CONNECTION_URL_CONFIG = "kafkastore.connection.url";
  protected static final String KAFKASTORE_CONNECTION_URL_DOC =
      "Zookeeper url for the Kafka cluster";

  /**
   * <code>kafkastore.zk.session.timeout.ms</code>
   */
  public static final String KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG
      = "kafkastore.zk.session.timeout.ms";
  protected static final String KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC =
      "Zookeeper session timeout";

  /**
   * <code>kafkastore.topic</code>
   */
  public static final String KAFKASTORE_TOPIC_CONFIG = "kafkastore.topic";
  public static final String DEFAULT_KAFKASTORE_TOPIC = "_schemas";
  protected static final String KAFKASTORE_TOPIC_DOC =
      "The durable single partition topic that acts" +
      "as the durable log for the data";

  /**
   * <code>kafkastore.timeout.ms</code>
   */
  public static final String KAFKASTORE_TIMEOUT_CONFIG = "kafkastore.timeout.ms";
  protected static final String KAFKASTORE_TIMEOUT_DOC =
      "The timeout for an operation on the Kafka store";

  /**
   * <code>kafkastore.commit.interval.ms</code>
   */
  public static final String KAFKASTORE_COMMIT_INTERVAL_MS_CONFIG = "kafkastore.commit.interval.ms";
  protected static final String KAFKASTORE_COMMIT_INTERVAL_MS_DOC =
      "The interval to commit offsets while consuming the Kafka topic";

  /**
   * <code>advertised.host</code>
   */
  public static final String ADVERTISED_HOST_CONFIG = "advertised.host";
  protected static final String ADVERTISED_HOST_DOC = "The host name advertised in Zookeeper";

  static {
    config
        .defineOverride(RESPONSE_MEDIATYPE_PREFERRED_CONFIG, ConfigDef.Type.LIST,
                        Versions.PREFERRED_RESPONSE_TYPES, ConfigDef.Importance.HIGH,
                        RESPONSE_MEDIATYPE_PREFERRED_CONFIG_DOC)
        .defineOverride(RESPONSE_MEDIATYPE_DEFAULT_CONFIG, ConfigDef.Type.STRING,
                        Versions.SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT, ConfigDef.Importance.HIGH,
                        RESPONSE_MEDIATYPE_DEFAULT_CONFIG_DOC)
        .define(KAFKASTORE_CONNECTION_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                KAFKASTORE_CONNECTION_URL_DOC)
        .define(KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG, ConfigDef.Type.INT, 10000, atLeast(0),
                ConfigDef.Importance.LOW, KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC)
        .define(KAFKASTORE_TOPIC_CONFIG, ConfigDef.Type.STRING, DEFAULT_KAFKASTORE_TOPIC,
                ConfigDef.Importance.HIGH, KAFKASTORE_TOPIC_DOC)
        .define(KAFKASTORE_TIMEOUT_CONFIG, ConfigDef.Type.INT, 500, atLeast(0),
                ConfigDef.Importance.MEDIUM, KAFKASTORE_TIMEOUT_DOC)
        .define(KAFKASTORE_COMMIT_INTERVAL_MS_CONFIG, ConfigDef.Type.INT, 60000,
                ConfigDef.Importance.MEDIUM,
                KAFKASTORE_COMMIT_INTERVAL_MS_DOC)
        .define(ADVERTISED_HOST_CONFIG, ConfigDef.Type.STRING, getDefaultHost(),
                ConfigDef.Importance.LOW, ADVERTISED_HOST_DOC);
  }

  private static String getDefaultHost() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new ConfigException("Unknown local hostname", e);
    }
  }

  public SchemaRegistryConfig(Map<? extends Object, ? extends Object> props) {
    super(props);
  }

  public SchemaRegistryConfig(String propsFile) throws RestConfigException {
    this(getPropsFromFile(propsFile));
  }

  public static void main(String[] args) {
    System.out.println(config.toHtmlTable());
  }
}
