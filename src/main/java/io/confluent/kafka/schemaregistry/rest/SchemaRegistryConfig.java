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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigException;
import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;

import static io.confluent.common.config.ConfigDef.Range.atLeast;

public class SchemaRegistryConfig extends RestConfig {

  public static final String KAFKASTORE_CONNECTION_URL_CONFIG = "kafkastore.connection.url";
  /**
   * <code>kafkastore.zk.session.timeout.ms</code>
   */
  public static final String KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG
      = "kafkastore.zk.session.timeout.ms";
  /**
   * <code>kafkastore.topic</code>
   */
  public static final String KAFKASTORE_TOPIC_CONFIG = "kafkastore.topic";
  public static final String DEFAULT_KAFKASTORE_TOPIC = "_schemas";
  /**
   * <code>kafkastore.timeout.ms</code>
   */
  public static final String KAFKASTORE_TIMEOUT_CONFIG = "kafkastore.timeout.ms";
  /**
   * <code>kafkastore.commit.interval.ms</code>
   */
  public static final String KAFKASTORE_COMMIT_INTERVAL_MS_CONFIG = "kafkastore.commit.interval.ms";
  public static final int OFFSET_COMMIT_OFF = -1;
  // TODO: turn off offset commit by default for now since we only have an in-memory store
  private static final int KAFKASTORE_COMMIT_INTERVAL_MS_DEFAULT = OFFSET_COMMIT_OFF;
  /**
   * <code>advertised.host</code>
   */
  public static final String ADVERTISED_HOST_CONFIG = "advertised.host";
  protected static final String KAFKASTORE_CONNECTION_URL_DOC =
      "Zookeeper url for the Kafka cluster";
  protected static final String KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC =
      "Zookeeper session timeout";
  protected static final String KAFKASTORE_TOPIC_DOC =
      "The durable single partition topic that acts" +
      "as the durable log for the data";
  protected static final String KAFKASTORE_TIMEOUT_DOC =
      "The timeout for an operation on the Kafka store";
  protected static final String KAFKASTORE_COMMIT_INTERVAL_MS_DOC =
      "The interval to commit offsets while consuming the Kafka topic";
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
        .define(KAFKASTORE_COMMIT_INTERVAL_MS_CONFIG, ConfigDef.Type.INT,
                KAFKASTORE_COMMIT_INTERVAL_MS_DEFAULT, ConfigDef.Importance.MEDIUM,
                KAFKASTORE_COMMIT_INTERVAL_MS_DOC)
        .define(ADVERTISED_HOST_CONFIG, ConfigDef.Type.STRING, getDefaultHost(),
                ConfigDef.Importance.LOW, ADVERTISED_HOST_DOC);
  }

  public SchemaRegistryConfig(Map<? extends Object, ? extends Object> props) {
    super(props);
  }

  public SchemaRegistryConfig(String propsFile) throws RestConfigException {
    this(getPropsFromFile(propsFile));
  }

  private static String getDefaultHost() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new ConfigException("Unknown local hostname", e);
    }
  }

  public static void main(String[] args) {
    System.out.println(config.toHtmlTable());
  }
}
