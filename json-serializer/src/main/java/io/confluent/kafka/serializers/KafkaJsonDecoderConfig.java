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

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;

import java.util.Map;

public class KafkaJsonDecoderConfig extends AbstractConfig {

  public static final String FAIL_UNKNOWN_PROPERTIES = "json.fail.unknown.properties";
  public static final boolean FAIL_UNKNOWN_PROPERTIES_DEFAULT = true;
  public static final String FAIL_UNKNOWN_PROPERTIES_DOC =
      "Whether to fail deserialization if unknown JSON properties are encountered";

  public KafkaJsonDecoderConfig(Map<?, ?> props) {
    super(baseConfig(), props);
  }

  protected KafkaJsonDecoderConfig(ConfigDef config, Map<?, ?> props) {
    super(config, props);
  }

  protected static ConfigDef baseConfig() {
    return new ConfigDef()
        .define(FAIL_UNKNOWN_PROPERTIES, ConfigDef.Type.BOOLEAN, FAIL_UNKNOWN_PROPERTIES_DEFAULT,
                ConfigDef.Importance.LOW, FAIL_UNKNOWN_PROPERTIES_DOC);
  }

}
