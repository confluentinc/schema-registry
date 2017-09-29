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

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;
import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import static io.confluent.common.config.ConfigDef.Range.atLeast;
import static io.confluent.kafka.schemaregistry.client.rest.Versions.PREFERRED_RESPONSE_TYPES;
import static io.confluent.kafka.schemaregistry.client.rest.Versions.SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT;

public class SchemaRegistryConfig extends RestConfig {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryConfig.class);

  private static final int SCHEMAREGISTRY_PORT_DEFAULT = 8081;
  // TODO: change this to "http://0.0.0.0:8081" when PORT_CONFIG is deleted.
  private static final String SCHEMAREGISTRY_LISTENERS_DEFAULT = "";

  public static final String SCHEMAREGISTRY_GROUP_ID_CONFIG = "schema.registry.group.id";

  @Deprecated
  public static final String KAFKASTORE_SECURITY_PROTOCOL_SSL = "SSL";
  @Deprecated
  public static final String KAFKASTORE_SECURITY_PROTOCOL_PLAINTEXT = "PLAINTEXT";

  public static final String KAFKASTORE_CONNECTION_URL_CONFIG = "kafkastore.connection.url";
  public static final String KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG = "kafkastore.bootstrap.servers";
  public static final String KAFKASTORE_GROUP_ID_CONFIG = "kafkastore.group.id";
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
   * <code>kafkastore.timeout.ms</code>
   */
  public static final String KAFKASTORE_TIMEOUT_CONFIG = "kafkastore.timeout.ms";
  /**
   * <code>kafkastore.init.timeout.ms</code>
   */
  public static final String KAFKASTORE_INIT_TIMEOUT_CONFIG = "kafkastore.init.timeout.ms";

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

  public static final String ZOOKEEPER_SET_ACL_CONFIG = "zookeeper.set.acl";
  public static final String KAFKASTORE_SECURITY_PROTOCOL_CONFIG =
      "kafkastore.security.protocol";
  public static final String KAFKASTORE_SSL_TRUSTSTORE_LOCATION_CONFIG =
      "kafkastore.ssl.truststore.location";
  public static final String KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_CONFIG =
      "kafkastore.ssl.truststore.password";
  public static final String KAFKASTORE_SSL_KEYSTORE_LOCATION_CONFIG =
      "kafkastore.ssl.keystore.location";
  public static final String KAFKASTORE_SSL_TRUSTSTORE_TYPE_CONFIG =
      "kafkastore.ssl.truststore.type";
  public static final String KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_CONFIG =
      "kafkastore.ssl.trustmanager.algorithm";
  public static final String KAFKASTORE_SSL_KEYSTORE_PASSWORD_CONFIG =
      "kafkastore.ssl.keystore.password";
  public static final String KAFKASTORE_SSL_KEYSTORE_TYPE_CONFIG =
      "kafkastore.ssl.keystore.type";
  public static final String KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_CONFIG =
      "kafkastore.ssl.keymanager.algorithm";
  public static final String KAFKASTORE_SSL_KEY_PASSWORD_CONFIG =
      "kafkastore.ssl.key.password";
  public static final String KAFKASTORE_SSL_ENABLED_PROTOCOLS_CONFIG =
      "kafkastore.ssl.enabled.protocols";
  public static final String KAFKASTORE_SSL_PROTOCOL_CONFIG =
      "kafkastore.ssl.protocol";
  public static final String KAFKASTORE_SSL_PROVIDER_CONFIG =
      "kafkastore.ssl.provider";
  public static final String KAFKASTORE_SSL_CIPHER_SUITES_CONFIG =
      "kafkastore.ssl.cipher.suites";
  public static final String KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG =
      "kafkastore.ssl.endpoint.identification.algorithm";
  public static final String KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_CONFIG =
      "kafkastore.sasl.kerberos.service.name";
  public static final String KAFKASTORE_SASL_MECHANISM_CONFIG =
      "kafkastore.sasl.mechanism";
  public static final String KAFKASTORE_SASL_KERBEROS_KINIT_CMD_CONFIG =
      "kafkastore.sasl.kerberos.kinit.cmd";
  public static final String KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG =
      "kafkastore.sasl.kerberos.min.time.before.relogin";
  public static final String KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG =
      "kafkastore.sasl.kerberos.ticket.renew.jitter";
  public static final String KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG =
      "kafkastore.sasl.kerberos.ticket.renew.window.factor";
  public static final String SCHEMAREGISTRY_RESOURCE_EXTENSION_CONFIG =
      "schema.registry.resource.extension.class";
  public static final String SCHEMAREGISTRY_INTER_INSTANCE_PROTOCOL_CONFIG =
      "schema.registry.inter.instance.protocol";

  protected static final String SCHEMAREGISTRY_GROUP_ID_DOC =
      "Use this setting to override the group.id for the Kafka group used when Kafka is used for "
      + "master election.\n"
      + "Without this configuration, group.id will be \"schema-registry\". If you want to run "
      + "more than one schema registry cluster against a single Kafka cluster you should make "
      + "this setting unique for each cluster.";

  protected static final String KAFKASTORE_CONNECTION_URL_DOC =
      "Zookeeper URL for the Kafka cluster";
  protected static final String KAFKASTORE_BOOTSTRAP_SERVERS_DOC =
      "A list of Kafka brokers to connect to. For example, "
      + "`PLAINTEXT://hostname:9092,SSL://hostname2:9092`\n"
      + "\n"
      + "The effect of this setting depends on whether you specify `kafkastore.connection.url`."
      + "\n"
      + "If `kafkastore.connection.url` is not specified, then the Kafka cluster containing these "
      + "bootstrap servers will be used both to coordinate schema registry instances (master "
      + "election) and store schema data."
      + "\n"
      + "If `kafkastore.connection.url` is specified, then this setting is used to control how "
      + "the schema registry connects to Kafka to store schema data and is particularly important "
      + "when Kafka security is enabled. When this configuration is not specified, the Schema "
      + "Registry's internal Kafka clients will get their Kafka bootstrap server list from "
      + "ZooKeeper (configured with `kafkastore.connection.url`). In that case, all available "
      + "listeners matching the `kafkastore.security.protocol` setting will be used."
      + "\n"
      + "By specifiying this configuration, you can control which endpoints are used to connect "
      + "to Kafka. Kafka may expose multiple endpoints that all will be stored in ZooKeeper, but "
      + "the Schema Registry may need to be configured with just one of those endpoints, for "
      + "example to control which security protocol it uses.";
  protected static final String KAFKASTORE_GROUP_ID_DOC =
      "Use this setting to override the group.id for the KafkaStore consumer.\n"
      + "This setting can become important when security is enabled, to ensure stability over "
      + "the schema registry consumer's group.id\n"
      + "Without this configuration, group.id will be \"schema-registry-<host>-<port>\"";
  protected static final String SCHEMAREGISTRY_ZK_NAMESPACE_DOC =
      "The string that is used as the zookeeper namespace for storing schema registry "
      + "metadata. SchemaRegistry instances which are part of the same schema registry service "
      + "should have the same ZooKeeper namespace.";
  protected static final String KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC =
      "Zookeeper session timeout";
  protected static final String KAFKASTORE_TOPIC_DOC =
      "The durable single partition topic that acts"
      + "as the durable log for the data";
  protected static final String KAFKASTORE_TOPIC_REPLICATION_FACTOR_DOC =
      "The desired replication factor of the schema topic. The actual replication factor "
      + "will be the smaller of this value and the number of live Kafka brokers.";
  protected static final String KAFKASTORE_WRITE_RETRIES_DOC =
      "Retry a failed register schema request to the underlying Kafka store up to this many times, "
      + " for example in case of a Kafka broker failure";
  protected static final String KAFKASTORE_WRITE_RETRY_BACKOFF_MS_DOC =
      "The amount of time in milliseconds to wait before attempting to retry a failed write "
      + "to the Kafka store";
  protected static final String KAFKASTORE_INIT_TIMEOUT_DOC =
      "The timeout for initialization of the Kafka store, including creation of the Kafka topic "
      + "that stores schema data.";
  protected static final String KAFKASTORE_TIMEOUT_DOC =
      "The timeout for an operation on the Kafka store";
  protected static final String HOST_DOC =
      "The host name advertised in Zookeeper. Make sure to set this if running SchemaRegistry "
      + "with multiple nodes.";
  protected static final String
      ZOOKEEPER_SET_ACL_DOC =
      "Whether or not to set an ACL in ZooKeeper when znodes are created and ZooKeeper SASL "
      + "authentication is "
      + "configured. IMPORTANT: if set to `true`, the SASL principal must be the same as the Kafka"
      + " brokers.";
  protected static final String COMPATIBILITY_DOC =
      "The Avro compatibility type. Valid values are: "
      + "none (new schema can be any valid Avro schema), "
      + "backward (new schema can read data produced by latest registered schema), "
      + "forward (latest registered schema can read data produced by the new schema), "
      + "full (new schema is backward and forward compatible with latest registered schema)";
  protected static final String MASTER_ELIGIBILITY_DOC =
      "If true, this node can participate in master election. In a multi-colo setup, turn this off "
      + "for clusters in the slave data center.";
  protected static final String KAFKASTORE_SECURITY_PROTOCOL_DOC =
      "The security protocol to use when connecting with Kafka, the underlying persistent storage. "
      + "Values can be `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`.";
  protected static final String KAFKASTORE_SSL_TRUSTSTORE_LOCATION_DOC =
      "The location of the SSL trust store file.";
  protected static final String KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_DOC =
      "The password to access the trust store.";
  protected static final String KAFAKSTORE_SSL_TRUSTSTORE_TYPE_DOC =
      "The file format of the trust store.";
  protected static final String KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_DOC =
      "The algorithm used by the trust manager factory for SSL connections.";
  protected static final String KAFKASTORE_SSL_KEYSTORE_LOCATION_DOC =
      "The location of the SSL keystore file.";
  protected static final String KAFKASTORE_SSL_KEYSTORE_PASSWORD_DOC =
      "The password to access the keystore.";
  protected static final String KAFAKSTORE_SSL_KEYSTORE_TYPE_DOC =
      "The file format of the keystore.";
  protected static final String KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_DOC =
      "The algorithm used by key manager factory for SSL connections.";
  protected static final String KAFKASTORE_SSL_KEY_PASSWORD_DOC =
      "The password of the key contained in the keystore.";
  protected static final String KAFAKSTORE_SSL_ENABLED_PROTOCOLS_DOC =
      "Protocols enabled for SSL connections.";
  protected static final String KAFAKSTORE_SSL_PROTOCOL_DOC =
      "The SSL protocol used.";
  protected static final String KAFAKSTORE_SSL_PROVIDER_DOC =
      "The name of the security provider used for SSL.";
  protected static final String KAFKASTORE_SSL_CIPHER_SUITES_DOC =
      "A list of cipher suites used for SSL.";
  protected static final String
      KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC =
      "The endpoint identification algorithm to validate the server hostname using the server "
      + "certificate.";
  public static final String
      KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_DOC =
      "The Kerberos principal name that the Kafka client runs as. This can be defined either in "
      + "the JAAS "
      + "config file or here.";
  public static final String KAFKASTORE_SASL_MECHANISM_DOC =
      "The SASL mechanism used for Kafka connections. GSSAPI is the default.";
  public static final String KAFKASTORE_SASL_KERBEROS_KINIT_CMD_DOC =
      "The Kerberos kinit command path.";
  public static final String KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC =
      "The login time between refresh attempts.";
  public static final String KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_DOC =
      "The percentage of random jitter added to the renewal time.";
  public static final String
      KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC =
      "Login thread will sleep until the specified window factor of time from last refresh to "
      + "ticket's expiry has "
      + "been reached, at which time it will try to renew the ticket.";
  protected static final String SCHEMAREGISTRY_RESOURCE_EXTENSION_DOC =
      "Fully qualified class name of a valid implementation of the interface "
      + "SchemaRegistryResourceExtension. This can be used to inject user defined resources like "
      + "filters. Typically used to add custom capability like logging, security, etc  ";
  protected static final String SCHEMAREGISTRY_INTER_INSTANCE_PROTOCOL_DOC =
      "The protocol used while making calls between the instances of schema registry. The slave "
      + "to master node calls for writes and deletes will use the specified protocol. The default "
      + "value would be `http`. When `https` is set, `ssl.keystore.` and "
      + "`ssl.truststore.` configs are used while making the call.";

  private static final boolean ZOOKEEPER_SET_ACL_DEFAULT = false;
  private static final String COMPATIBILITY_DEFAULT = "backward";
  private static final String METRICS_JMX_PREFIX_DEFAULT_OVERRIDE = "kafka.schema.registry";

  // TODO: move to Apache's ConfigDef
  private static final ConfigDef config;

  public static final String HTTPS = "https";
  public static final String HTTP = "http";

  private Properties originalProperties;


  static {
    config = baseSchemaRegistryConfigDef();

  }

  public static ConfigDef baseSchemaRegistryConfigDef() {

    return baseConfigDef()
        .defineOverride(PORT_CONFIG, ConfigDef.Type.INT, SCHEMAREGISTRY_PORT_DEFAULT,
            ConfigDef.Importance.LOW, PORT_CONFIG_DOC
        )
        .defineOverride(LISTENERS_CONFIG, ConfigDef.Type.LIST, SCHEMAREGISTRY_LISTENERS_DEFAULT,
            ConfigDef.Importance.HIGH, LISTENERS_DOC + "\n\n"
                                       + "Schema Registry identities are stored in "
                                       + "ZooKeeper and are made up of a hostname and "
                                       + "port. "
                                       + "If multiple listeners are configured, the "
                                       + "first listener's port is used for its "
                                       + "identity."
        )
        .defineOverride(RESPONSE_MEDIATYPE_PREFERRED_CONFIG, ConfigDef.Type.LIST,
            PREFERRED_RESPONSE_TYPES,
            ConfigDef.Importance.HIGH,
            RESPONSE_MEDIATYPE_PREFERRED_CONFIG_DOC
        )
        .defineOverride(RESPONSE_MEDIATYPE_DEFAULT_CONFIG, ConfigDef.Type.STRING,
            SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT,
            ConfigDef.Importance.HIGH,
            RESPONSE_MEDIATYPE_DEFAULT_CONFIG_DOC
        )
        .define(KAFKASTORE_CONNECTION_URL_CONFIG, ConfigDef.Type.STRING, "",
            ConfigDef.Importance.HIGH, KAFKASTORE_CONNECTION_URL_DOC
        )
        .define(KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, "",
            ConfigDef.Importance.MEDIUM,
            KAFKASTORE_BOOTSTRAP_SERVERS_DOC
        )
        .define(SCHEMAREGISTRY_ZK_NAMESPACE, ConfigDef.Type.STRING,
                DEFAULT_SCHEMAREGISTRY_ZK_NAMESPACE,
                ConfigDef.Importance.LOW, SCHEMAREGISTRY_ZK_NAMESPACE_DOC
        )
        .define(SCHEMAREGISTRY_GROUP_ID_CONFIG, ConfigDef.Type.STRING, "schema-registry",
                ConfigDef.Importance.MEDIUM, SCHEMAREGISTRY_GROUP_ID_DOC
        )
        .define(KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG, ConfigDef.Type.INT, 30000, atLeast(0),
            ConfigDef.Importance.LOW, KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC
        )
        .define(KAFKASTORE_TOPIC_CONFIG, ConfigDef.Type.STRING, DEFAULT_KAFKASTORE_TOPIC,
            ConfigDef.Importance.HIGH, KAFKASTORE_TOPIC_DOC
        )
        .define(KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG, ConfigDef.Type.INT,
            DEFAULT_KAFKASTORE_TOPIC_REPLICATION_FACTOR,
            ConfigDef.Importance.HIGH, KAFKASTORE_TOPIC_REPLICATION_FACTOR_DOC
        )
        .define(KAFKASTORE_INIT_TIMEOUT_CONFIG, ConfigDef.Type.INT, 60000, atLeast(0),
            ConfigDef.Importance.MEDIUM, KAFKASTORE_INIT_TIMEOUT_DOC
        )
        .define(KAFKASTORE_TIMEOUT_CONFIG, ConfigDef.Type.INT, 500, atLeast(0),
            ConfigDef.Importance.MEDIUM, KAFKASTORE_TIMEOUT_DOC
        )
        .define(HOST_NAME_CONFIG, ConfigDef.Type.STRING, getDefaultHost(),
            ConfigDef.Importance.HIGH, HOST_DOC
        )
        .define(COMPATIBILITY_CONFIG, ConfigDef.Type.STRING, COMPATIBILITY_DEFAULT,
            ConfigDef.Importance.HIGH, COMPATIBILITY_DOC
        )
        .define(ZOOKEEPER_SET_ACL_CONFIG, ConfigDef.Type.BOOLEAN, ZOOKEEPER_SET_ACL_DEFAULT,
            ConfigDef.Importance.HIGH, ZOOKEEPER_SET_ACL_DOC
        )
        .define(MASTER_ELIGIBILITY, ConfigDef.Type.BOOLEAN, DEFAULT_MASTER_ELIGIBILITY,
            ConfigDef.Importance.MEDIUM, MASTER_ELIGIBILITY_DOC
        )
        .defineOverride(METRICS_JMX_PREFIX_CONFIG, ConfigDef.Type.STRING,
            METRICS_JMX_PREFIX_DEFAULT_OVERRIDE, ConfigDef.Importance.LOW,
            METRICS_JMX_PREFIX_DOC
        )
        .define(KAFKASTORE_SECURITY_PROTOCOL_CONFIG, ConfigDef.Type.STRING,
            SecurityProtocol.PLAINTEXT.toString(), ConfigDef.Importance.MEDIUM,
            KAFKASTORE_SECURITY_PROTOCOL_DOC
        )
        .define(KAFKASTORE_SSL_TRUSTSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING,
            "", ConfigDef.Importance.HIGH,
            KAFKASTORE_SSL_TRUSTSTORE_LOCATION_DOC
        )
        .define(KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_CONFIG, ConfigDef.Type.STRING,
            "", ConfigDef.Importance.HIGH,
            KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_DOC
        )
        .define(KAFKASTORE_SSL_TRUSTSTORE_TYPE_CONFIG, ConfigDef.Type.STRING,
            "JKS", ConfigDef.Importance.MEDIUM,
            KAFAKSTORE_SSL_TRUSTSTORE_TYPE_DOC
        )
        .define(KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_CONFIG, ConfigDef.Type.STRING,
            "PKIX", ConfigDef.Importance.LOW,
            KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_DOC
        )
        .define(KAFKASTORE_SSL_KEYSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING,
            "", ConfigDef.Importance.HIGH,
            KAFKASTORE_SSL_KEYSTORE_LOCATION_DOC
        )
        .define(KAFKASTORE_SSL_KEYSTORE_PASSWORD_CONFIG, ConfigDef.Type.STRING,
            "", ConfigDef.Importance.HIGH,
            KAFKASTORE_SSL_KEYSTORE_PASSWORD_DOC
        )
        .define(KAFKASTORE_SSL_KEYSTORE_TYPE_CONFIG, ConfigDef.Type.STRING,
            "JKS", ConfigDef.Importance.MEDIUM,
            KAFAKSTORE_SSL_KEYSTORE_TYPE_DOC
        )
        .define(KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_CONFIG, ConfigDef.Type.STRING,
            "SunX509", ConfigDef.Importance.LOW,
            KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_DOC
        )
        .define(KAFKASTORE_SSL_KEY_PASSWORD_CONFIG, ConfigDef.Type.STRING,
            "", ConfigDef.Importance.HIGH,
            KAFKASTORE_SSL_KEY_PASSWORD_DOC
        )
        .define(KAFKASTORE_SSL_ENABLED_PROTOCOLS_CONFIG, ConfigDef.Type.STRING,
            "TLSv1.2,TLSv1.1,TLSv1", ConfigDef.Importance.MEDIUM,
            KAFAKSTORE_SSL_ENABLED_PROTOCOLS_DOC
        )
        .define(KAFKASTORE_SSL_PROTOCOL_CONFIG, ConfigDef.Type.STRING,
            "TLS", ConfigDef.Importance.MEDIUM,
            KAFAKSTORE_SSL_PROTOCOL_DOC
        )
        .define(KAFKASTORE_SSL_PROVIDER_CONFIG, ConfigDef.Type.STRING,
            "", ConfigDef.Importance.MEDIUM,
            KAFAKSTORE_SSL_PROVIDER_DOC
        )
        .define(KAFKASTORE_SSL_CIPHER_SUITES_CONFIG, ConfigDef.Type.STRING,
            "", ConfigDef.Importance.LOW,
            KAFKASTORE_SSL_CIPHER_SUITES_DOC
        )
        .define(KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ConfigDef.Type.STRING,
            "", ConfigDef.Importance.LOW,
            KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC
        )
        .define(KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_CONFIG, ConfigDef.Type.STRING,
            "", ConfigDef.Importance.MEDIUM,
            KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_DOC
        )
        .define(KAFKASTORE_SASL_MECHANISM_CONFIG, ConfigDef.Type.STRING,
            "GSSAPI", ConfigDef.Importance.MEDIUM,
            KAFKASTORE_SASL_MECHANISM_DOC
        )
        .define(KAFKASTORE_SASL_KERBEROS_KINIT_CMD_CONFIG, ConfigDef.Type.STRING,
            "/usr/bin/kinit", ConfigDef.Importance.LOW,
            KAFKASTORE_SASL_KERBEROS_KINIT_CMD_DOC
        )
        .define(KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG, ConfigDef.Type.LONG,
            60000, ConfigDef.Importance.LOW,
            KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC
        )
        .define(KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG, ConfigDef.Type.DOUBLE,
            0.05, ConfigDef.Importance.LOW,
            KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_DOC
        )
        .define(KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG, ConfigDef.Type.DOUBLE,
            0.8, ConfigDef.Importance.LOW,
            KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC
        )
        .define(KAFKASTORE_GROUP_ID_CONFIG, ConfigDef.Type.STRING, "",
            ConfigDef.Importance.LOW, KAFKASTORE_GROUP_ID_DOC
        )
        .define(SCHEMAREGISTRY_RESOURCE_EXTENSION_CONFIG, ConfigDef.Type.STRING, "",
            ConfigDef.Importance.LOW, SCHEMAREGISTRY_RESOURCE_EXTENSION_DOC
        )
        .define(SCHEMAREGISTRY_INTER_INSTANCE_PROTOCOL_CONFIG, ConfigDef.Type.STRING, HTTP,
            ConfigDef.Importance.LOW, SCHEMAREGISTRY_INTER_INSTANCE_PROTOCOL_DOC);

  }

  private final AvroCompatibilityLevel compatibilityType;

  public SchemaRegistryConfig(String propsFile) throws RestConfigException {
    this(getPropsFromFile(propsFile));
  }

  public SchemaRegistryConfig(Properties props) throws RestConfigException {
    this(config, props);
  }

  public SchemaRegistryConfig(ConfigDef configDef, Properties props) throws RestConfigException {
    super(configDef, props);
    this.originalProperties = props;
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

  public Properties originalProperties() {
    return originalProperties;
  }

  public AvroCompatibilityLevel compatibilityType() {
    return compatibilityType;
  }

  public boolean useKafkaCoordination() {
    boolean haveStoreConnectionUrl = !getString(KAFKASTORE_CONNECTION_URL_CONFIG).isEmpty();
    boolean haveBootstrapServers = !getList(KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG).isEmpty();
    if (!haveStoreConnectionUrl && ! haveBootstrapServers) {
      throw new ConfigException(
          "Must configure at least one bootstrap connection method, either "
          + KAFKASTORE_CONNECTION_URL_CONFIG + " or " + KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG
      );
    }
    return !haveStoreConnectionUrl;
  }

  public String bootstrapBrokers() {
    int zkSessionTimeoutMs = getInt(KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG);

    List<String> bootstrapServersConfig = getList(KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG);
    List<String> endpoints;

    if (bootstrapServersConfig.isEmpty()) {
      ZkUtils zkUtils = null;
      try {
        zkUtils =
            ZkUtils.apply(getString(KAFKASTORE_CONNECTION_URL_CONFIG),
                zkSessionTimeoutMs,
                zkSessionTimeoutMs,
                checkZkAclConfig()
            );
        Seq<Broker> brokerSeq = zkUtils.getAllBrokersInCluster();
        endpoints = brokersToEndpoints(JavaConversions.seqAsJavaList(brokerSeq));
      } finally {
        if (zkUtils != null) {
          zkUtils.close();
        }
        log.debug("Kafka store zookeeper client shut down");
      }
    } else {
      endpoints = bootstrapServersConfig;
    }
    return endpointsToBootstrapServers(
        endpoints,
        this.getString(KAFKASTORE_SECURITY_PROTOCOL_CONFIG)
    );
  }

  /**
   * Checks if the user has configured ZooKeeper ACLs or not. Throws an exception if the ZooKeeper
   * client is set to create znodes with an ACL, yet the JAAS config is not present. Otherwise,
   * returns whether or not the user has enabled ZooKeeper ACLs.
   */
  public boolean checkZkAclConfig() {
    if (getBoolean(ZOOKEEPER_SET_ACL_CONFIG) && !JaasUtils.isZkSecurityEnabled()) {
      throw new ConfigException(ZOOKEEPER_SET_ACL_CONFIG
                                + " is set to true but ZooKeeper's "
                                + "JAAS SASL configuration is not configured.");
    }
    return getBoolean(ZOOKEEPER_SET_ACL_CONFIG);
  }

  static List<String> brokersToEndpoints(List<Broker> brokers) {
    final List<String> endpoints = new LinkedList<>();
    for (Broker broker : brokers) {
      for (EndPoint ep : JavaConversions.asJavaCollection(broker.endPoints())) {
        String
            hostport =
            ep.host() == null ? ":" + ep.port() : Utils.formatAddress(ep.host(), ep.port());
        String endpoint = ep.securityProtocol() + "://" + hostport;

        endpoints.add(endpoint);
      }
    }

    return endpoints;
  }

  static String endpointsToBootstrapServers(List<String> endpoints, String securityProtocol) {
    final Set<String> supportedSecurityProtocols = new HashSet<>(SecurityProtocol.names());
    if (!supportedSecurityProtocols.contains(securityProtocol.toUpperCase(Locale.ROOT))) {
      throw new ConfigException(
          "Only PLAINTEXT, SSL, SASL_PLAINTEXT, and SASL_SSL Kafka endpoints are supported.");
    }

    final String securityProtocolUrlPrefix = securityProtocol + "://";
    final StringBuilder sb = new StringBuilder();
    for (String endpoint : endpoints) {
      if (!endpoint.startsWith(securityProtocolUrlPrefix)) {
        log.warn(
            "Ignoring Kafka broker endpoint " + endpoint + " that does not match the setting for "
            + KAFKASTORE_SECURITY_PROTOCOL_CONFIG + "=" + securityProtocol);
        continue;
      }

      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(endpoint);
    }

    if (sb.length() == 0) {
      throw new ConfigException("No supported Kafka endpoints are configured. Either "
                                + KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG
                                + " must have at least one endpoint matching "
                                + KAFKASTORE_SECURITY_PROTOCOL_CONFIG
                                + " or broker endpoints loaded from ZooKeeper "
                                + "must have at least one endpoint matching "
                                + KAFKASTORE_SECURITY_PROTOCOL_CONFIG + ".");
    }

    return sb.toString();
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }

}
