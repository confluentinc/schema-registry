/**
 * Copyright 2014-2016 Confluent Inc.
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

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigDef.*;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.protocol.SecurityProtocol;

public class KafkaStoreConfig extends AbstractConfig {
  public static final String KAFKASTORE_CONFIG_PREFIX = "kafkastore.";

  public static final String KAFKASTORE_CONNECTION_URL_CONFIG = "connection.url";
  public static final String KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

  public static final String KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG = "zk.session.timeout.ms";

  public static final String KAFKASTORE_TOPIC_CONFIG = "topic";
  public static final String DEFAULT_KAFKASTORE_TOPIC = "_schemas";
  public static final String KAFKASTORE_KAFKA_GROUPID = "groupid";

  public static final String KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG = "topic.replication.factor";
  public static final int DEFAULT_KAFKASTORE_TOPIC_REPLICATION_FACTOR = 3;

  public static final String KAFKASTORE_TIMEOUT_CONFIG = "timeout.ms";

  public static final String KAFKASTORE_INIT_TIMEOUT_CONFIG = "init.timeout.ms";

  public static final String KAFKASTORE_SECURITY_PROTOCOL_CONFIG = "security.protocol";
  public static final String KAFKASTORE_SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
  public static final String KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
  public static final String KAFKASTORE_SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";
  public static final String KAFKASTORE_SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
  public static final String KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_CONFIG = "ssl.trustmanager.algorithm";
  public static final String KAFKASTORE_SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
  public static final String KAFKASTORE_SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
  public static final String KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_CONFIG = "ssl.keymanager.algorithm";
  public static final String KAFKASTORE_SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";
  public static final String KAFKASTORE_SSL_ENABLED_PROTOCOLS_CONFIG = "ssl.enabled.protocols";
  public static final String KAFKASTORE_SSL_PROTOCOL_CONFIG = "ssl.protocol";
  public static final String KAFKASTORE_SSL_PROVIDER_CONFIG = "ssl.provider";
  public static final String KAFKASTORE_SSL_CIPHER_SUITES_CONFIG = "ssl.cipher.suites";
  public static final String KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = "ssl.endpoint.identification.algorithm";
  public static final String KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_CONFIG = "sasl.kerberos.service.name";
  public static final String KAFKASTORE_SASL_MECHANISM_CONFIG = "sasl.mechanism";
  public static final String KAFKASTORE_SASL_KERBEROS_KINIT_CMD_CONFIG = "sasl.kerberos.kinit.cmd";
  public static final String KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG = "sasl.kerberos.min.time.before.relogin";
  public static final String KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG = "sasl.kerberos.ticket.renew.jitter";
  public static final String KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG = "sasl.kerberos.ticket.renew.window.factor";

  protected static final String KAFKASTORE_CONNECTION_URL_DOC =
      "Zookeeper url for the Kafka cluster";
  protected static final String KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC =
      "Zookeeper session timeout";
  protected static final String KAFKASTORE_TOPIC_DOC =
      "The durable single partition topic that acts" +
      "as the durable log for the data";
  protected static final String KAFKASTORE_KAFKA_GROUPID_DOC =
      "The group ID used to register the consumer with Kafka. Must be unique to this instance";
  protected static final String KAFKASTORE_TOPIC_REPLICATION_FACTOR_DOC =
      "The desired replication factor of the schema topic. The actual replication factor " +
      "will be the smaller of this value and the number of live Kafka brokers.";
  protected static final String KAFKASTORE_TIMEOUT_DOC =
      "The timeout for an operation on the Kafka store";
  protected static final String KAFKASTORE_INIT_TIMEOUT_DOC =
      "The timeout for initialization of the Kafka store, including creation of the Kafka topic " +
      "that stores schema data.";
  protected static final String KAFKASTORE_SECURITY_PROTOCOL_DOC =
      "The security protocol to use when connecting with Kafka, the underlying persistent storage. " +
      "Values can be `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`.";
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
  protected static final String KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC =
      "The endpoint identification algorithm to validate the server hostname using the server certificate.";
  public static final String KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_DOC =
      "The Kerberos principal name that the Kafka client runs as. This can be defined either in the JAAS " +
      "config file or here.";
  public static final String KAFKASTORE_SASL_MECHANISM_DOC =
      "The SASL mechanism used for Kafka connections. GSSAPI is the default.";
  public static final String KAFKASTORE_SASL_KERBEROS_KINIT_CMD_DOC =
      "The Kerberos kinit command path.";
  public static final String KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC =
      "The login time between refresh attempts.";
  public static final String KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_DOC =
      "The percentage of random jitter added to the renewal time.";
  public static final String KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC =
      "Login thread will sleep until the specified window factor of time from last refresh to ticket's expiry has " +
      "been reached, at which time it will try to renew the ticket.";

  private static final ConfigDef configDef;
  static {
    configDef = new ConfigDef()
        .define(KAFKASTORE_CONNECTION_URL_CONFIG, Type.STRING, Importance.HIGH, KAFKASTORE_CONNECTION_URL_DOC)
        .define(KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, Type.LIST, "", Importance.MEDIUM, KAFKASTORE_CONNECTION_URL_DOC)
        .define(KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG, Type.INT, 30000, atLeast(0), Importance.LOW,
            KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC)
        .define(KAFKASTORE_TOPIC_CONFIG, Type.STRING, DEFAULT_KAFKASTORE_TOPIC, Importance.HIGH, KAFKASTORE_TOPIC_DOC)
        .define(KAFKASTORE_KAFKA_GROUPID, Type.STRING, Importance.HIGH, KAFKASTORE_KAFKA_GROUPID_DOC)
        .define(KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG, Type.INT, DEFAULT_KAFKASTORE_TOPIC_REPLICATION_FACTOR,
            Importance.HIGH, KAFKASTORE_TOPIC_REPLICATION_FACTOR_DOC)
        .define(KAFKASTORE_TIMEOUT_CONFIG, Type.INT, 500, atLeast(0), Importance.MEDIUM, KAFKASTORE_TIMEOUT_DOC)
        .define(KAFKASTORE_INIT_TIMEOUT_CONFIG, Type.INT, 60000, atLeast(0), Importance.MEDIUM,
            KAFKASTORE_INIT_TIMEOUT_DOC)
        .define(KAFKASTORE_SECURITY_PROTOCOL_CONFIG, Type.STRING, SecurityProtocol.PLAINTEXT.toString(),
            Importance.MEDIUM, KAFKASTORE_SECURITY_PROTOCOL_DOC)
        .define(KAFKASTORE_SSL_TRUSTSTORE_LOCATION_CONFIG, Type.STRING, "", Importance.HIGH,
            KAFKASTORE_SSL_TRUSTSTORE_LOCATION_DOC)
        .define(KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_CONFIG, Type.STRING, "", Importance.HIGH,
            KAFKASTORE_SSL_TRUSTSTORE_PASSWORD_DOC)
        .define(KAFKASTORE_SSL_TRUSTSTORE_TYPE_CONFIG, Type.STRING, "JKS", Importance.MEDIUM,
            KAFAKSTORE_SSL_TRUSTSTORE_TYPE_DOC)
        .define(KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_CONFIG, Type.STRING, "PKIX", Importance.LOW,
            KAFKASTORE_SSL_TRUSTMANAGER_ALGORITHM_DOC)
        .define(KAFKASTORE_SSL_KEYSTORE_LOCATION_CONFIG, Type.STRING, "", Importance.HIGH,
            KAFKASTORE_SSL_KEYSTORE_LOCATION_DOC)
        .define(KAFKASTORE_SSL_KEYSTORE_PASSWORD_CONFIG, Type.STRING, "", Importance.HIGH,
            KAFKASTORE_SSL_KEYSTORE_PASSWORD_DOC)
        .define(KAFKASTORE_SSL_KEYSTORE_TYPE_CONFIG, Type.STRING, "JKS", Importance.MEDIUM,
            KAFAKSTORE_SSL_KEYSTORE_TYPE_DOC)
        .define(KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_CONFIG, Type.STRING, "SunX509", Importance.LOW,
            KAFKASTORE_SSL_KEYMANAGER_ALGORITHM_DOC)
        .define(KAFKASTORE_SSL_KEY_PASSWORD_CONFIG, Type.STRING, "", Importance.HIGH, KAFKASTORE_SSL_KEY_PASSWORD_DOC)
        .define(KAFKASTORE_SSL_ENABLED_PROTOCOLS_CONFIG, Type.STRING, "TLSv1.2,TLSv1.1,TLSv1", Importance.MEDIUM,
            KAFAKSTORE_SSL_ENABLED_PROTOCOLS_DOC)
        .define(KAFKASTORE_SSL_PROTOCOL_CONFIG, Type.STRING, "TLS", Importance.MEDIUM, KAFAKSTORE_SSL_PROTOCOL_DOC)
        .define(KAFKASTORE_SSL_PROVIDER_CONFIG, Type.STRING, "", Importance.MEDIUM, KAFAKSTORE_SSL_PROVIDER_DOC)
        .define(KAFKASTORE_SSL_CIPHER_SUITES_CONFIG, Type.STRING, "", Importance.LOW, KAFKASTORE_SSL_CIPHER_SUITES_DOC)
        .define(KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, Type.STRING, "", Importance.LOW,
            KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
        .define(KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_CONFIG, Type.STRING, "", Importance.MEDIUM,
            KAFKASTORE_SASL_KERBEROS_SERVICE_NAME_DOC)
        .define(KAFKASTORE_SASL_MECHANISM_CONFIG, Type.STRING, "GSSAPI", Importance.MEDIUM,
            KAFKASTORE_SASL_MECHANISM_DOC)
        .define(KAFKASTORE_SASL_KERBEROS_KINIT_CMD_CONFIG, Type.STRING, "/usr/bin/kinit", Importance.LOW,
            KAFKASTORE_SASL_KERBEROS_KINIT_CMD_DOC)
        .define(KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG, Type.LONG, 60000, Importance.LOW,
            KAFKASTORE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
        .define(KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG, Type.DOUBLE, 0.05, Importance.LOW,
            KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
        .define(KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG, Type.DOUBLE, 0.8, Importance.LOW,
            KAFKASTORE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC);
  }

  public KafkaStoreConfig(Properties props) {
    super(configDef, props);
  }

  public KafkaStoreConfig(Map<String, Object> values) {
    super(configDef, values);
  }
}
