/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.connect.schema;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class AbstractDataConfig extends AbstractConfig {

  public static final String GENERALIZED_SUM_TYPE_SUPPORT_CONFIG = "generalized.sum.type.support";
  public static final boolean GENERALIZED_SUM_TYPE_SUPPORT_DEFAULT = false;
  public static final String GENERALIZED_SUM_TYPE_SUPPORT_DOC =
      "Toggle for enabling/disabling generalized sum type support: interoperability of enum/union "
          + "with other schema formats";

  public static final String IGNORE_DEFAULT_FOR_NULLABLES_CONFIG =
      "ignore.default.for.nullables";
  public static final boolean IGNORE_DEFAULT_FOR_NULLABLES_DEFAULT = false;
  public static final String IGNORE_DEFAULT_FOR_NULLABLES_DOC =
      "Whether to ignore the default for nullable fields when the value is null";

  public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.config";
  public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
  public static final String SCHEMAS_CACHE_SIZE_DOC = "Size of the converted schemas cache";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
        .define(GENERALIZED_SUM_TYPE_SUPPORT_CONFIG,
            ConfigDef.Type.BOOLEAN,
            GENERALIZED_SUM_TYPE_SUPPORT_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            GENERALIZED_SUM_TYPE_SUPPORT_DOC)
        .define(IGNORE_DEFAULT_FOR_NULLABLES_CONFIG,
            ConfigDef.Type.BOOLEAN,
            IGNORE_DEFAULT_FOR_NULLABLES_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            IGNORE_DEFAULT_FOR_NULLABLES_DOC)
        .define(SCHEMAS_CACHE_SIZE_CONFIG,
            ConfigDef.Type.INT,
            SCHEMAS_CACHE_SIZE_DEFAULT,
            ConfigDef.Importance.LOW,
            SCHEMAS_CACHE_SIZE_DOC
        );
  }

  public AbstractDataConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }

  public AbstractDataConfig(ConfigDef definition, Map<?, ?> props) {
    super(definition, props);
  }

  public boolean isGeneralizedSumTypeSupport() {
    return this.getBoolean(GENERALIZED_SUM_TYPE_SUPPORT_CONFIG);
  }

  public boolean ignoreDefaultForNullables() {
    return this.getBoolean(IGNORE_DEFAULT_FOR_NULLABLES_CONFIG);
  }

  public int schemaCacheSize() {
    return Math.max(1, this.getInt(SCHEMAS_CACHE_SIZE_CONFIG));
  }
}
