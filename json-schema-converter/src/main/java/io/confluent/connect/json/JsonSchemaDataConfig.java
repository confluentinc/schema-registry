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

import java.util.HashMap;
import java.util.Map;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;

public class JsonSchemaDataConfig extends AbstractConfig {

  public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
  public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
  public static final String SCHEMAS_CACHE_SIZE_DOC = "Size of the converted schemas cache";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef().define(SCHEMAS_CACHE_SIZE_CONFIG,
        ConfigDef.Type.INT,
        SCHEMAS_CACHE_SIZE_DEFAULT,
        ConfigDef.Importance.LOW,
        SCHEMAS_CACHE_SIZE_DOC
    );
  }

  public JsonSchemaDataConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }

  public int schemaCacheSize() {
    return getInt(SCHEMAS_CACHE_SIZE_CONFIG);
  }

  public static class Builder {

    private final Map<String, Object> props = new HashMap<>();

    public Builder with(String key, Object value) {
      props.put(key, value);
      return this;
    }

    public JsonSchemaDataConfig build() {
      return new JsonSchemaDataConfig(props);
    }
  }
}
