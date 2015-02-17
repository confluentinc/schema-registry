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
import java.util.Properties;

import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;

import static io.confluent.common.config.ConfigDef.Range.atLeast;

public class SchemaRegistryConfig extends RestConfig {

  private static final int SCHEMAREGISTRY_PORT_DEFAULT = 8081;

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
   * <code>kafkastore.topic.replication.factor</code>
   */
  public static final String KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG =
      "kafkastore.topic.replication.factor";
  public static final int DEFAULT_KAFKASTORE_TOPIC_REPLICATION_FACTOR = 3;
  /**
   * <code>kafkastore.write.max.retries</code>
   */
  public static final String KAFKASTORE_WRITE_MAX_RETRIES_CONFIG =
      "kafkastore.write.max.retries";
  public static final int DEFAULT_KAFKASTORE_WRITE_MAX_RETRIES = Integer.MAX_VALUE;
  /**
   * <code>kafkastore.write.retry.backoff.ms</code>
   */
  public static final String KAFKASTORE_WRITE_RETRY_BACKOFF_MS_CONFIG =
      "kafkastore.write.retry.backoff.ms";
  public static final int DEFAULT_KAFKASTORE_WRITE_RETRY_BACKOFF_MS = 100;
  /**
   * <code>kafkastore.timeout.ms</code>
   */
  public static final String KAFKASTORE_TIMEOUT_CONFIG = "kafkastore.timeout.ms";
  /**
   * <code>kafkastore.init.timeout.ms</code>
   */
  public static final String KAFKASTORE_INIT_TIMEOUT_CONFIG = "kafkastore.init.timeout.ms";
  /**
   * <code>kafkastore.commit.interval.ms</code>
   */
  public static final String KAFKASTORE_COMMIT_INTERVAL_MS_CONFIG = "kafkastore.commit.interval.ms";
  public static final int OFFSET_COMMIT_OFF = -1;
  // TODO: turn off offset commit by default for now since we only have an in-memory store
  private static final int KAFKASTORE_COMMIT_INTERVAL_MS_DEFAULT = OFFSET_COMMIT_OFF;
  /**
   * <code>master.eligibility</code>* 
   */
  public static final String MASTER_ELIGIBILITY = "master.eligibility";
  public static final boolean DEFAULT_MASTER_ELIGIBILITY = true;
  /**
   * <code>schema.registry.zk.name</code>* 
   */
  public static final String SCHEMAREGISTRY_ZK_NAMESPACE = "schema.registry.zk.namespace";
  public static final String DEFAULT_SCHEMAREGISTRY_ZK_NAMESPACE = "schema_registry";
  /**
   * <code>host.name</code>
   */
  public static final String HOST_NAME_CONFIG = "host.name";
  /**
   * <code>avro.compatibility.level</code>
   */
  public static final String COMPATIBILITY_CONFIG = "avro.compatibility.level";
  protected static final String KAFKASTORE_CONNECTION_URL_DOC =
      "Zookeeper url for the Kafka cluster";
  protected static final String SCHEMAREGISTRY_ZK_NAMESPACE_DOC =
      "The string that is used as the zookeeper namespace for storing schema registry "
      + "metadata. SchemaRegistry instances which are part of the same schema registry service "
      + "should have the same ZooKeeper namespace.";
  protected static final String KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC =
      "Zookeeper session timeout";
  protected static final String KAFKASTORE_TOPIC_DOC =
      "The durable single partition topic that acts" +
      "as the durable log for the data";
  protected static final String KAFKASTORE_TOPIC_REPLICATION_FACTOR_DOC =
      "The desired replication factor of the schema topic. The actual replication factor " +
      "will be the smaller of this value and the number of live Kafka brokers.";
  protected static final String KAFKASTORE_WRITE_RETRIES_DOC =
      "Setting for the producer producing to the underlying Kafka logs. "
      + "The producer will retry failed writes up to this many times, for example in case of" 
      + "a Kafka broker failure. This should be set to the maximum possible value to help ensure"
      + " consistency between the Kafka Schema Registry caches and the Kafka logs.";
  protected static final String KAFKASTORE_WRITE_RETRY_BACKOFF_MS_DOC =
      "The amount of time in milliseconds to wait before attempting to retry a failed write "
      + "to the Kafka store";
  protected static final String KAFKASTORE_INIT_TIMEOUT_DOC =
      "The timeout for initialization of the Kafka store, including creation of the Kafka topic "
      + "that stores schema data.";
  protected static final String KAFKASTORE_TIMEOUT_DOC =
      "The timeout for an operation on the Kafka store";
  protected static final String KAFKASTORE_COMMIT_INTERVAL_MS_DOC =
      "The interval to commit offsets while consuming the Kafka topic";
  protected static final String HOST_DOC = "The host name advertised in Zookeeper";
  protected static final String COMPATIBILITY_DOC =
      "The Avro compatibility type. Valid values are: "
      + "none (new schema can be any valid Avro schema), "
      + "backward (new schema can read data produced by latest registered schema), "
      + "forward (latest registered schema can read data produced by the new schema), "
      + "full (new schema is backward and forward compatible with latest registered schema)";
  protected static final String MASTER_ELIGIBILITY_DOC = 
      "If true, this node can participate in master election. In a multi-colo setup, turn this off" 
      + "for clusters in the slave data center.";                               
  private static final String COMPATIBILITY_DEFAULT = "backward";
  private static final String METRICS_JMX_PREFIX_DEFAULT_OVERRIDE = "kafka.schema.registry";
  private static final ConfigDef config;
  static {
    config = baseConfigDef()
        .defineOverride(PORT_CONFIG, ConfigDef.Type.INT, SCHEMAREGISTRY_PORT_DEFAULT,
                        ConfigDef.Importance.LOW, PORT_CONFIG_DOC)
        .defineOverride(RESPONSE_MEDIATYPE_PREFERRED_CONFIG, ConfigDef.Type.LIST,
                        io.confluent.kafka.schemaregistry.client.rest.Versions.PREFERRED_RESPONSE_TYPES,
                        ConfigDef.Importance.HIGH,
                        RESPONSE_MEDIATYPE_PREFERRED_CONFIG_DOC)
        .defineOverride(RESPONSE_MEDIATYPE_DEFAULT_CONFIG, ConfigDef.Type.STRING,
                        io.confluent.kafka.schemaregistry.client.rest.Versions.SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        RESPONSE_MEDIATYPE_DEFAULT_CONFIG_DOC)
        .define(KAFKASTORE_CONNECTION_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                KAFKASTORE_CONNECTION_URL_DOC)
        .define(SCHEMAREGISTRY_ZK_NAMESPACE, ConfigDef.Type.STRING,
                DEFAULT_SCHEMAREGISTRY_ZK_NAMESPACE,
                ConfigDef.Importance.LOW, SCHEMAREGISTRY_ZK_NAMESPACE_DOC)
        .define(KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG, ConfigDef.Type.INT, 30000, atLeast(0),
                ConfigDef.Importance.LOW, KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC)
        .define(KAFKASTORE_TOPIC_CONFIG, ConfigDef.Type.STRING, DEFAULT_KAFKASTORE_TOPIC,
                ConfigDef.Importance.HIGH, KAFKASTORE_TOPIC_DOC)
        .define(KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG, ConfigDef.Type.INT,
                DEFAULT_KAFKASTORE_TOPIC_REPLICATION_FACTOR,
                ConfigDef.Importance.HIGH, KAFKASTORE_TOPIC_REPLICATION_FACTOR_DOC)
        .define(KAFKASTORE_WRITE_MAX_RETRIES_CONFIG, ConfigDef.Type.INT,
                DEFAULT_KAFKASTORE_WRITE_MAX_RETRIES, ConfigDef.Importance.HIGH,
                KAFKASTORE_WRITE_RETRIES_DOC)
        .define(KAFKASTORE_WRITE_RETRY_BACKOFF_MS_CONFIG, ConfigDef.Type.INT,
                DEFAULT_KAFKASTORE_WRITE_RETRY_BACKOFF_MS, ConfigDef.Importance.MEDIUM,
                KAFKASTORE_WRITE_RETRY_BACKOFF_MS_DOC)
        .define(KAFKASTORE_INIT_TIMEOUT_CONFIG, ConfigDef.Type.INT, 5000, atLeast(0),
                ConfigDef.Importance.MEDIUM, KAFKASTORE_INIT_TIMEOUT_DOC)
        .define(KAFKASTORE_TIMEOUT_CONFIG, ConfigDef.Type.INT, 500, atLeast(0),
                ConfigDef.Importance.MEDIUM, KAFKASTORE_TIMEOUT_DOC)
        .define(KAFKASTORE_COMMIT_INTERVAL_MS_CONFIG, ConfigDef.Type.INT,
                KAFKASTORE_COMMIT_INTERVAL_MS_DEFAULT, ConfigDef.Importance.MEDIUM,
                KAFKASTORE_COMMIT_INTERVAL_MS_DOC)
        .define(HOST_NAME_CONFIG, ConfigDef.Type.STRING, getDefaultHost(),
                ConfigDef.Importance.LOW, HOST_DOC)
        .define(COMPATIBILITY_CONFIG, ConfigDef.Type.STRING, COMPATIBILITY_DEFAULT,
                ConfigDef.Importance.HIGH, COMPATIBILITY_DOC)
        .define(MASTER_ELIGIBILITY, ConfigDef.Type.BOOLEAN, DEFAULT_MASTER_ELIGIBILITY, 
                ConfigDef.Importance.MEDIUM, MASTER_ELIGIBILITY_DOC)
        .defineOverride(METRICS_JMX_PREFIX_CONFIG, ConfigDef.Type.STRING,
                        METRICS_JMX_PREFIX_DEFAULT_OVERRIDE, ConfigDef.Importance.LOW,
                        METRICS_JMX_PREFIX_DOC);
  }
  private final AvroCompatibilityLevel compatibilityType;

  public SchemaRegistryConfig(Map<? extends Object, ? extends Object> props)
      throws RestConfigException {
    super(config, props);
    String compatibilityTypeString = getString(SchemaRegistryConfig.COMPATIBILITY_CONFIG);
    compatibilityType = AvroCompatibilityLevel.forName(compatibilityTypeString);
    if (compatibilityType == null) {
      throw new RestConfigException("Unknown Avro compatibility level: " + compatibilityTypeString);
    }
  }

  public SchemaRegistryConfig(String propsFile) throws RestConfigException {
    this(getPropsFromFile(propsFile));
  }

  public SchemaRegistryConfig(Properties props) throws RestConfigException {
    super(config, props);
    String compatibilityTypeString = getString(SchemaRegistryConfig.COMPATIBILITY_CONFIG);
    compatibilityType = AvroCompatibilityLevel.forName(compatibilityTypeString);
    if (compatibilityType == null) {
      throw new RestConfigException("Unknown Avro compatibility level: " + compatibilityTypeString);
    }
  }

  private static String getDefaultHost() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new ConfigException("Unknown local hostname", e);
    }
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }

  public AvroCompatibilityLevel compatibilityType() {
    return compatibilityType;
  }
}
