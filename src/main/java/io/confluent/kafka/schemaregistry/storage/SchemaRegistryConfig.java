package io.confluent.kafka.schemaregistry.storage;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class SchemaRegistryConfig extends KafkaStoreConfig {
    private static final ConfigDef config = new ConfigDef()
        .define(KAFKASTORE_CONNECTION_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
            KAFKASTORE_CONNECTION_URL_DOC)
        .define(KAFKASTORE_ZK_SESSION_TIMEOUT_MS_CONFIG, ConfigDef.Type.INT, 10000, atLeast(0),
            ConfigDef.Importance.LOW, KAFKASTORE_ZK_SESSION_TIMEOUT_MS_DOC)
        .define(KAFKASTORE_TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
            KAFKASTORE_TOPIC_DOC)
        .define(KAFKASTORE_TIMEOUT_CONFIG, ConfigDef.Type.INT, 500, atLeast(0),
            ConfigDef.Importance.MEDIUM, KAFKASTORE_TIMEOUT_DOC);

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
