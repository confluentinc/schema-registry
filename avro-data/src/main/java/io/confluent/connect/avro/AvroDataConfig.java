/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.connect.avro;

import io.confluent.connect.schema.AbstractDataConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;


public class AvroDataConfig extends AbstractDataConfig {

  public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG = "enhanced.avro.schema.support";
  public static final boolean ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT = false;
  public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_DOC =
      "Toggle for enabling/disabling enhanced avro schema support: Enum symbol preservation and "
      + "Package Name awareness";

  public static final String SCRUB_INVALID_NAMES_CONFIG = "scrub.invalid.names";
  public static final boolean SCRUB_INVALID_NAMES_DEFAULT = false;
  public static final String SCRUB_INVALID_NAMES_DOC =
      "Whether to scrub invalid names by replacing invalid characters with valid ones";

  public static final String CONNECT_META_DATA_CONFIG = "connect.meta.data";
  public static final boolean CONNECT_META_DATA_DEFAULT = true;
  public static final String CONNECT_META_DATA_DOC =
      "Toggle for enabling/disabling connect converter to add its meta data to the output schema "
      + "or not";

  public static final String ALLOW_OPTIONAL_MAP_KEYS_CONFIG = "allow.optional.map.keys";
  public static final boolean ALLOW_OPTIONAL_MAP_KEYS_DEFAULT = false;
  public static final String ALLOW_OPTIONAL_MAP_KEYS_DOC =
      "Allow optional string map key when converting from Connect Schema to Avro Schema.";

  public static final String FLATTEN_SINGLETON_UNION_CONFIG = "flatten.singleton.union";
  public static final boolean FLATTEN_SINGLETON_UNION_DEFAULT = true;
  public static final String FLATTEN_SINGLETON_UNION_DOC = "Whether to flatten singleton unions";

  @Deprecated
  public static final String DISCARD_TYPE_DOC_DEFAULT_CONFIG = "discard.type.doc.default";
  public static final boolean DISCARD_TYPE_DOC_DEFAULT_DEFAULT = false;
  public static final String DISCARD_TYPE_DOC_DEFAULT_DOC =
      "Only Avro field doc and default will be retained during conversion of Avro "
          + "and Connect schema.";

  public static ConfigDef baseConfigDef() {
    return AbstractDataConfig.baseConfigDef()
        .define(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG,
                ConfigDef.Type.BOOLEAN,
                ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                ENHANCED_AVRO_SCHEMA_SUPPORT_DOC)
        .define(SCRUB_INVALID_NAMES_CONFIG, ConfigDef.Type.BOOLEAN, SCRUB_INVALID_NAMES_DEFAULT,
                ConfigDef.Importance.MEDIUM, SCRUB_INVALID_NAMES_DOC)
        .define(CONNECT_META_DATA_CONFIG, ConfigDef.Type.BOOLEAN, CONNECT_META_DATA_DEFAULT,
                ConfigDef.Importance.LOW, CONNECT_META_DATA_DOC)
        .define(DISCARD_TYPE_DOC_DEFAULT_CONFIG,
                ConfigDef.Type.BOOLEAN,
                DISCARD_TYPE_DOC_DEFAULT_DEFAULT,
                ConfigDef.Importance.LOW,
                DISCARD_TYPE_DOC_DEFAULT_DOC)
        .define(ALLOW_OPTIONAL_MAP_KEYS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                ALLOW_OPTIONAL_MAP_KEYS_DEFAULT,
                ConfigDef.Importance.LOW,
                ALLOW_OPTIONAL_MAP_KEYS_DOC)
        .define(FLATTEN_SINGLETON_UNION_CONFIG,
                ConfigDef.Type.BOOLEAN,
                FLATTEN_SINGLETON_UNION_DEFAULT,
                ConfigDef.Importance.LOW,
                FLATTEN_SINGLETON_UNION_DOC);
  }

  public AvroDataConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }

  public boolean isEnhancedAvroSchemaSupport() {
    return this.getBoolean(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG);
  }

  public boolean isConnectMetaData() {
    return this.getBoolean(CONNECT_META_DATA_CONFIG);
  }

  public boolean isScrubInvalidNames() {
    return this.getBoolean(SCRUB_INVALID_NAMES_CONFIG);
  }

  public boolean isDiscardTypeDocDefault() {
    return this.getBoolean(DISCARD_TYPE_DOC_DEFAULT_CONFIG);
  }

  public boolean isAllowOptionalMapKeys() {
    return this.getBoolean(ALLOW_OPTIONAL_MAP_KEYS_CONFIG);
  }

  public boolean isFlattenSingletonUnion() {
    return this.getBoolean(FLATTEN_SINGLETON_UNION_CONFIG);
  }

  public static class Builder {

    private final Map<String, Object> props = new HashMap<>();

    public Builder with(String key, Object value) {
      props.put(key, value);
      return this;
    }

    public AvroDataConfig build() {
      return new AvroDataConfig(props);
    }
  }
}
