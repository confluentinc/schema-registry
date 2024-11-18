/*
 * Copyright 2023 Confluent Inc.
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
 *
 */

package io.confluent.kafka.serializers;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class WrapperKeySerializerConfig extends AbstractConfig {

  public static final String WRAPPED_KEY_SERIALIZER = "wrapped.key.serializer";
  public static final String WRAPPED_KEY_SERIALIZER_DOC =
      "Serializer class to which calls will be delegated";

  private static final ConfigDef config;

  static {
    config = new ConfigDef().define(
        WRAPPED_KEY_SERIALIZER,
        ConfigDef.Type.CLASS,
        Object.class,
        ConfigDef.Importance.HIGH,
        WRAPPED_KEY_SERIALIZER_DOC
    );
  }

  public WrapperKeySerializerConfig(Map<?, ?> props) {
    super(config, props);
  }
}
