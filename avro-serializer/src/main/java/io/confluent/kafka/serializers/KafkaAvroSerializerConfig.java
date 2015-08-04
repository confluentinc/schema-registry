/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafka.serializers;

import io.confluent.common.config.ConfigDef;

import java.util.Map;

/**
 * Created by Priti on 7/29/15.
 */
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
