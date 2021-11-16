/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class SchemaRegistryClientConfig {

  public static final String CLIENT_NAMESPACE = "schema.registry.";

  public static final String BASIC_AUTH_CREDENTIALS_SOURCE = "basic.auth.credentials.source";
  /**
   * @deprecated use {@link #USER_INFO_CONFIG} instead
   */
  @Deprecated
  public static final String SCHEMA_REGISTRY_USER_INFO_CONFIG =
      "schema.registry.basic.auth.user.info";
  public static final String USER_INFO_CONFIG = "basic.auth.user.info";

  public static final String BEARER_AUTH_CREDENTIALS_SOURCE = "bearer.auth.credentials.source";
  public static final String BEARER_AUTH_TOKEN_CONFIG = "bearer.auth.token";

  public static final String PROXY_HOST = "proxy.host";
  public static final String PROXY_PORT = "proxy.port";

  public static final String MAX_CACHE_SIZE_CONFIG = "schema.registry.max.cache.size";
  public static final String MISSING_ID_QUERY_RANGE_CONFIG = "missing.id.query.range";
  public static final String MISSING_ID_CACHE_TTL_CONFIG = "missing.id.cache.ttl.sec";
  public static final String MISSING_SCHEMA_CACHE_TTL_CONFIG = "missing.schema.cache.ttl.sec";

  public static void withClientSslSupport(ConfigDef configDef, String namespace) {
    org.apache.kafka.common.config.ConfigDef sslConfigDef = new org.apache.kafka.common.config
        .ConfigDef();
    sslConfigDef.withClientSslSupport();

    for (org.apache.kafka.common.config.ConfigDef.ConfigKey configKey
        : sslConfigDef.configKeys().values()) {
      configDef.define(namespace + configKey.name,
          typeFor(configKey.type),
          configKey.defaultValue,
          importanceFor(configKey.importance),
          configKey.documentation);
    }
  }

  private static ConfigDef.Type typeFor(org.apache.kafka.common.config.ConfigDef.Type type) {
    return ConfigDef.Type.valueOf(type.name());
  }

  private static ConfigDef.Importance importanceFor(
      org.apache.kafka.common.config.ConfigDef.Importance importance) {
    return ConfigDef.Importance.valueOf(importance.name());
  }

  public static int getMissingIdQueryRange(Map<String, ?> configs) {
    int missingIdQueryRange = 200;
    if (configs != null && configs.containsKey(MISSING_ID_QUERY_RANGE_CONFIG)) {
      missingIdQueryRange = (int)configs.get(MISSING_ID_QUERY_RANGE_CONFIG);
    }

    return missingIdQueryRange;
  }

  public static long getMissingIdTTL(Map<String, ?> configs) {
    long missingIdTTL = 0L;
    if (configs != null && configs.containsKey(MISSING_ID_CACHE_TTL_CONFIG)) {
      missingIdTTL = (long)configs.get(MISSING_ID_CACHE_TTL_CONFIG);
    }

    return missingIdTTL;
  }

  public static long getMissingSchemaTTL(Map<String, ?> configs) {
    long missingSchemaTTL = 0L;
    if (configs != null && configs.containsKey(MISSING_SCHEMA_CACHE_TTL_CONFIG)) {
      missingSchemaTTL = (long)configs.get(MISSING_SCHEMA_CACHE_TTL_CONFIG);
    }

    return missingSchemaTTL;
  }

  public static int getMaxCacheSize(Map<String, ?> configs) {
    int maxCacheSize = 0;
    if (configs != null && configs.containsKey(MAX_CACHE_SIZE_CONFIG)) {
      maxCacheSize = (int)configs.get(MAX_CACHE_SIZE_CONFIG);
    }

    return maxCacheSize;
  }
}
