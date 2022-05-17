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
 *
 */

package io.confluent.connect.protobuf;

import java.util.HashMap;
import java.util.Map;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;

public class ProtobufDataConfig extends AbstractConfig {

  public static final String ENHANCED_PROTOBUF_SCHEMA_SUPPORT_CONFIG =
      "enhanced.protobuf.schema.support";
  public static final boolean ENHANCED_PROTOBUF_SCHEMA_SUPPORT_DEFAULT = false;
  public static final String ENHANCED_PROTOBUF_SCHEMA_SUPPORT_DOC =
      "Toggle for enabling/disabling enhanced protobuf schema support: "
          + "package name preservation";

  public static final String SCRUB_INVALID_NAMES_CONFIG = "scrub.invalid.names";
  public static final boolean SCRUB_INVALID_NAMES_DEFAULT = false;
  public static final String SCRUB_INVALID_NAMES_DOC =
      "Whether to scrub invalid names by replacing invalid characters with valid ones";

  public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.config";
  public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
  public static final String SCHEMAS_CACHE_SIZE_DOC = "Size of the converted schemas cache";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
        .define(ENHANCED_PROTOBUF_SCHEMA_SUPPORT_CONFIG,
            ConfigDef.Type.BOOLEAN,
            ENHANCED_PROTOBUF_SCHEMA_SUPPORT_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            ENHANCED_PROTOBUF_SCHEMA_SUPPORT_DOC)
        .define(SCRUB_INVALID_NAMES_CONFIG, ConfigDef.Type.BOOLEAN, SCRUB_INVALID_NAMES_DEFAULT,
            ConfigDef.Importance.MEDIUM, SCRUB_INVALID_NAMES_DOC)
        .define(SCHEMAS_CACHE_SIZE_CONFIG,
            ConfigDef.Type.INT,
            SCHEMAS_CACHE_SIZE_DEFAULT,
            ConfigDef.Importance.LOW,
            SCHEMAS_CACHE_SIZE_DOC
        );
  }

  public ProtobufDataConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }

  public boolean isEnhancedProtobufSchemaSupport() {
    return this.getBoolean(ENHANCED_PROTOBUF_SCHEMA_SUPPORT_CONFIG);
  }

  public boolean isScrubInvalidNames() {
    return this.getBoolean(SCRUB_INVALID_NAMES_CONFIG);
  }

  public int schemaCacheSize() {
    return this.getInt(SCHEMAS_CACHE_SIZE_CONFIG);
  }

  public static class Builder {

    private final Map<String, Object> props = new HashMap<>();

    public Builder with(String key, Object value) {
      props.put(key, value);
      return this;
    }

    public ProtobufDataConfig build() {
      return new ProtobufDataConfig(props);
    }
  }
}
