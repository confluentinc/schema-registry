/*
 * Copyright 2014-2025 Confluent Inc.
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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class KafkaJsonSerializerConfig extends AbstractConfig {

  public static final String JSON_INDENT_OUTPUT = "json.indent.output";
  public static final boolean JSON_INDENT_OUTPUT_DEFAULT = false;
  public static final String JSON_INDENT_OUTPUT_DOC =
      "Whether JSON output should be indented (\"pretty-printed\")";

  public static final String WRITE_DATES_AS_ISO8601 = "json.write.dates.iso8601";
  public static final boolean WRITE_DATES_AS_ISO8601_DEFAULT = false;
  public static final String WRITE_DATES_AS_ISO8601_DOC = "Whether to write dates as "
      + "ISO 8601 strings";

  private static ConfigDef config;

  static {
    config = new ConfigDef().define(JSON_INDENT_OUTPUT,
        ConfigDef.Type.BOOLEAN,
        JSON_INDENT_OUTPUT_DEFAULT,
        ConfigDef.Importance.LOW,
        JSON_INDENT_OUTPUT_DOC
    ).define(WRITE_DATES_AS_ISO8601,
        ConfigDef.Type.BOOLEAN,
        WRITE_DATES_AS_ISO8601_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        WRITE_DATES_AS_ISO8601_DOC
    );
  }

  public KafkaJsonSerializerConfig(Map<?, ?> props) {
    super(config, props);
  }

}
