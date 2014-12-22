package io.confluent.kafka.schemaregistry.storage;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class SchemaRegistryConfig extends KafkaStoreConfig {

  /**
   * <code>advertised.host</code>
   */
  public static final String ADVERTISED_HOST_CONFIG = "advertised.host";

  /**
   * <code>port</code>
   */
  public static final String PORT_CONFIG = "port";

  protected static final String ADVERTISED_HOST_DOC =
      "Host to bind the HTTP servlet";
  protected static final String PORT_DOC =
      "Port to bind the HTTP servlet";

  private static final ConfigDef config = KafkaStoreConfig.config
      .define(ADVERTISED_HOST_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
              ADVERTISED_HOST_DOC)
      .define(PORT_CONFIG, ConfigDef.Type.INT, 8080, ConfigDef.Importance.LOW, PORT_DOC);

  public SchemaRegistryConfig(ConfigDef arg0, Map<?, ?> arg1) {
    super(arg0, arg1);
  }

  public SchemaRegistryConfig(Map<? extends Object, ? extends Object> props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toHtmlTable());
  }

}
