/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.kafka.serializers.wrapper;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class CompositeDeserializerConfig extends AbstractConfig {

  public static final String OLD_DESERIALIZER = "old.deserializer";
  public static final String OLD_DESERIALIZER_DOC =
      "Old deserializer that is schema-unaware";

  public static final String CONFLUENT_DESERIALIZER = "confluent.deserializer";
  public static final String CONFLUENT_DESERIALIZER_DOC =
      "Confluent deserializer, one of Avro, Protobuf, JSON Schema";

  private static final ConfigDef config;

  static {
    config = new ConfigDef().define(
        OLD_DESERIALIZER,
        ConfigDef.Type.CLASS,
        Object.class,
        ConfigDef.Importance.HIGH,
        OLD_DESERIALIZER_DOC
    ).define(
        CONFLUENT_DESERIALIZER,
        ConfigDef.Type.CLASS,
        Object.class,
        ConfigDef.Importance.HIGH,
        CONFLUENT_DESERIALIZER_DOC
    );
  }

  public CompositeDeserializerConfig(Map<?, ?> props) {
    super(config, props);
  }
}
