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

package io.confluent.connect.json;

import java.util.Map;

import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public class JsonSchemaConverterConfig extends AbstractKafkaSchemaSerDeConfig {

  public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
  public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
  private static final String SCHEMAS_CACHE_SIZE_DOC = "The maximum number of schemas that can be"
      + " cached in this converter instance.";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = baseConfigDef();
    CONFIG.define(
        SCHEMAS_CACHE_SIZE_CONFIG,
        Type.INT,
        SCHEMAS_CACHE_SIZE_DEFAULT,
        Importance.HIGH,
        SCHEMAS_CACHE_SIZE_DOC
    );
  }

  public static ConfigDef configDef() {
    return CONFIG;
  }

  public JsonSchemaConverterConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }

  public int schemaCacheSize() {
    return getInt(SCHEMAS_CACHE_SIZE_CONFIG);
  }
}
