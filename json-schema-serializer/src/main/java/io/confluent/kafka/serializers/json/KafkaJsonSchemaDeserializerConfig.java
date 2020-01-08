/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */


package io.confluent.kafka.serializers.json;

import java.util.Map;

import io.confluent.common.config.ConfigDef;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public class KafkaJsonSchemaDeserializerConfig extends AbstractKafkaSchemaSerDeConfig {

  public static final String FAIL_UNKNOWN_PROPERTIES = "json.fail.unknown.properties";
  public static final boolean FAIL_UNKNOWN_PROPERTIES_DEFAULT = true;
  public static final String FAIL_UNKNOWN_PROPERTIES_DOC = "Whether to fail deserialization if "
      + "unknown JSON properties are encountered";

  public static final String FAIL_INVALID_SCHEMA = "json.fail.invalid.schema";
  public static final boolean FAIL_INVALID_SCHEMA_DEFAULT = false;
  public static final String FAIL_INVALID_SCHEMA_DOC = "Whether to fail deserialization if the "
      + "payload does not match the schema";

  public static final String JSON_KEY_TYPE = "json.key.type";
  public static final String JSON_KEY_TYPE_DEFAULT = Object.class.getName();
  public static final String JSON_KEY_TYPE_DOC = "Classname of the type that the message key "
      + "should be deserialized to";

  public static final String JSON_VALUE_TYPE = "json.value.type";
  public static final String JSON_VALUE_TYPE_DEFAULT = Object.class.getName();
  public static final String JSON_VALUE_TYPE_DOC = "Classname of the type that the message value "
      + "should be deserialized to";

  private static ConfigDef config;

  static {
    config = baseConfigDef().define(FAIL_UNKNOWN_PROPERTIES,
        ConfigDef.Type.BOOLEAN,
        FAIL_UNKNOWN_PROPERTIES_DEFAULT,
        ConfigDef.Importance.LOW,
        FAIL_UNKNOWN_PROPERTIES_DOC
    ).define(FAIL_INVALID_SCHEMA,
        ConfigDef.Type.BOOLEAN,
        FAIL_INVALID_SCHEMA_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        FAIL_INVALID_SCHEMA_DOC
    ).define(JSON_KEY_TYPE,
        ConfigDef.Type.CLASS,
        JSON_KEY_TYPE_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        JSON_KEY_TYPE_DOC
    ).define(JSON_VALUE_TYPE,
        ConfigDef.Type.CLASS,
        JSON_VALUE_TYPE_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        JSON_VALUE_TYPE_DOC
    );
  }

  public KafkaJsonSchemaDeserializerConfig(Map<?, ?> props) {
    super(config, props);
  }

}
