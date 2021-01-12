/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.serializers;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class KafkaAvroSerializerConfig extends AbstractKafkaSchemaSerDeConfig {

  public static final String AVRO_REFLECTION_ALLOW_NULL_CONFIG = "avro.reflection.allow.null";
  public static final boolean AVRO_REFLECTION_ALLOW_NULL_DEFAULT = false;
  public static final String AVRO_REFLECTION_ALLOW_NULL_DOC =
      "If true, allows null field values used in ReflectionAvroSerializer";

  private static ConfigDef config;

  static {
    config = baseConfigDef();
    config = baseConfigDef()
        .define(AVRO_REFLECTION_ALLOW_NULL_CONFIG, ConfigDef.Type.BOOLEAN,
            AVRO_REFLECTION_ALLOW_NULL_DEFAULT, ConfigDef.Importance.LOW,
            AVRO_REFLECTION_ALLOW_NULL_DOC);
  }

  public KafkaAvroSerializerConfig(Map<?, ?> props) {
    super(config, props);
  }
}
