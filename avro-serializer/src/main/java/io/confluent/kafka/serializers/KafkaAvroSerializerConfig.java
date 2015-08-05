package io.confluent.kafka.serializers;

import io.confluent.common.config.ConfigDef;

import java.util.Map;

public class KafkaAvroSerializerConfig extends AbstractKafkaAvroSerDeConfig {

    public static final String ENABLE_AUTO_SCHEMA_REGISTRATION_CONFIG = "enable.auto.schema.registration";
    public static final boolean ENABLE_AUTO_SCHEMA_REGISTRATION_DEFAULT = true;
    public static final String ENABLE_AUTO_SCHEMA_REGISTRATION_DOC =
            "If true new schema will be created if one does not already exist, else a version of schema needs to exist" +
                    " without which user gets a SerializationException";

    private static ConfigDef config;

    static {
        config = baseConfigDef()
                .define(ENABLE_AUTO_SCHEMA_REGISTRATION_CONFIG, ConfigDef.Type.BOOLEAN, ENABLE_AUTO_SCHEMA_REGISTRATION_DEFAULT,
                        ConfigDef.Importance.LOW, ENABLE_AUTO_SCHEMA_REGISTRATION_DOC);
    }

    public KafkaAvroSerializerConfig(Map<?, ?> props) {
        super(config, props);
    }
}
