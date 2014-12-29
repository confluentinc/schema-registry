package io.confluent.kafka.schemaregistry.storage;

import java.util.Map;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;

import static io.confluent.common.config.ConfigDef.Importance;
import static io.confluent.common.config.ConfigDef.Range.atLeast;
import static io.confluent.common.config.ConfigDef.Type;

public class KafkaStoreConfig extends AbstractConfig {

  /**
   * <code>kafkastore.connection.url</code>
   */
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
  /**
   * <code>kafkastore.timeout.ms</code>
   */
  public static final String KAFKASTORE_TIMEOUT_CONFIG = "kafkastore.timeout.ms";
  protected static final String KAFKASTORE_CONNECTION_URL_DOC =
      "Zookeeper url for the Kafka cluster";
  protected static final String KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC =
      "Zookeeper session timeout";
  protected static final String KAFKASTORE_TOPIC_DOC =
      "The durable single partition topic that acts" +
      "as the durable log for the data";
  protected static final String KAFKASTORE_TIMEOUT_DOC =
      "The timeout for an operation on the Kafka" +
      " store";

  private static final ConfigDef config = new ConfigDef()
      .define(KAFKASTORE_CONNECTION_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
              KAFKASTORE_CONNECTION_URL_DOC)
      .define(KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG, Type.INT, 10000, atLeast(0),
              Importance.LOW,
              KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC)
      .define(KAFKASTORE_TOPIC_CONFIG, Type.STRING, Importance.HIGH, KAFKASTORE_TOPIC_DOC)
      .define(KAFKASTORE_TIMEOUT_CONFIG, Type.INT, 500, atLeast(0), Importance.MEDIUM,
              KAFKASTORE_TIMEOUT_DOC);

  public KafkaStoreConfig(ConfigDef arg0, Map<?, ?> arg1) {
    super(arg0, arg1);
  }

  KafkaStoreConfig(Map<? extends Object, ? extends Object> props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toHtmlTable());
  }

}
