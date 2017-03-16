/**
 * Copyright 2015 Confluent Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
 * Deserializer configuration for {@link KafkaJsonDeserializer}. It's slightly odd
 * to have this extend {@link KafkaJsonDecoderConfig} (in most other cases the
 * Decoder variants extend from the Deserializer version), but the reason is that
 * {@link kafka.serializer.Decoder} doesn't provide the ability to distinguish
 * between constructing serializers for keys or for values. So the type configuration
 * options do not work with {@link KafkaJsonDecoder}.
 */
public class KafkaJsonDeserializerConfig extends KafkaJsonDecoderConfig {

  public static final String JSON_KEY_TYPE = "json.key.type";
  public static final String JSON_KEY_TYPE_DEFAULT = Object.class.getName();
  public static final String JSON_KEY_TYPE_DOC =
      "Classname of the type that the message key should be deserialized to";

  public static final String JSON_VALUE_TYPE = "json.value.type";
  public static final String JSON_VALUE_TYPE_DEFAULT = Object.class.getName();
  public static final String JSON_VALUE_TYPE_DOC =
      "Classname of the type that the message value should be deserialized to";

  private static ConfigDef config;

  static {
    config = baseConfig()
        .define(JSON_KEY_TYPE, ConfigDef.Type.CLASS, JSON_KEY_TYPE_DEFAULT,
                ConfigDef.Importance.MEDIUM, JSON_KEY_TYPE_DOC)
        .define(JSON_VALUE_TYPE, ConfigDef.Type.CLASS, JSON_VALUE_TYPE_DEFAULT,
                ConfigDef.Importance.MEDIUM, JSON_VALUE_TYPE_DOC);
  }

  public KafkaJsonDeserializerConfig(Map<?, ?> props) {
    super(config, props);
  }

}
