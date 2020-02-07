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

public class KafkaJsonSchemaSerializerConfig extends AbstractKafkaSchemaSerDeConfig {

  public static final String FAIL_INVALID_SCHEMA = "json.fail.invalid.schema";
  public static final boolean FAIL_INVALID_SCHEMA_DEFAULT = false;
  public static final String FAIL_INVALID_SCHEMA_DOC = "Whether to fail deserialization if the "
      + "payload does not match the schema";

  public static final String JSON_INDENT_OUTPUT = "json.indent.output";
  public static final boolean JSON_INDENT_OUTPUT_DEFAULT = false;
  public static final String JSON_INDENT_OUTPUT_DOC = "Whether JSON output should be indented "
      + "(\"pretty-printed\")";

  private static ConfigDef config;

  static {
    config = baseConfigDef().define(FAIL_INVALID_SCHEMA,
        ConfigDef.Type.BOOLEAN,
        FAIL_INVALID_SCHEMA_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        FAIL_INVALID_SCHEMA_DOC
    ).define(JSON_INDENT_OUTPUT,
        ConfigDef.Type.BOOLEAN,
        JSON_INDENT_OUTPUT_DEFAULT,
        ConfigDef.Importance.LOW,
        JSON_INDENT_OUTPUT_DOC
    );
  }

  public KafkaJsonSchemaSerializerConfig(Map<?, ?> props) {
    super(config, props);
  }

}
