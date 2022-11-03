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

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;

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

  public static final String MISSING_CACHE_SIZE_CONFIG = "missing.cache.size";
  public static final String MISSING_ID_CACHE_TTL_CONFIG = "missing.id.cache.ttl.sec";
  public static final String MISSING_SCHEMA_CACHE_TTL_CONFIG = "missing.schema.cache.ttl.sec";


  //OAuth AUTHORIZATION SERVER related configs
  public static final String BEARER_AUTH_ISSUER_ENDPOINT_URL = "bearer.auth.issuer.endpoint.url";
  public static final String BEARER_AUTH_CLIENT_ID = "bearer.auth.client.id";
  public static final String BEARER_AUTH_CLIENT_SECRET = "bearer.auth.client.secret";
  public static final String BEARER_AUTH_SCOPE = "bearer.auth.scope";
  public static final String BEARER_AUTH_SCOPE_CLAIM_NAME = "bearer.auth.scope.claim.name";
  public static final String BEARER_AUTH_SCOPE_CLAIM_NAME_DEFAULT =
      SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
  public static final String BEARER_AUTH_SUB_CLAIM_NAME = "bearer.auth.sub.claim.name";
  public static final String BEARER_AUTH_SUB_CLAIM_NAME_DEFAULT =
      SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME;

  //OAuth configs required by SR
  public static final String BEARER_AUTH_LOGICAL_CLUSTER = "bearer.auth.logical.cluster";
  public static final String BEARER_AUTH_IDENTITY_POOL_ID = "bearer.auth.identity.pool";

  //OAuth config related to token cache
  public static final String BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS =
      "bearer.auth.cache.expiry.buffer.seconds";
  public static final short BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DEFAULT = 300;


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

  public static long getMissingIdTTL(Map<String, ?> configs) {
    return configs != null && configs.containsKey(MISSING_ID_CACHE_TTL_CONFIG)
        ? (Long) configs.get(MISSING_ID_CACHE_TTL_CONFIG)
        : 0L;
  }

  public static long getMissingSchemaTTL(Map<String, ?> configs) {
    return configs != null && configs.containsKey(MISSING_SCHEMA_CACHE_TTL_CONFIG)
        ? (Long) configs.get(MISSING_SCHEMA_CACHE_TTL_CONFIG)
        : 0L;
  }

  public static int getMaxMissingCacheSize(Map<String, ?> configs) {
    return configs != null && configs.containsKey(MISSING_CACHE_SIZE_CONFIG)
        ? (Integer) configs.get(MISSING_CACHE_SIZE_CONFIG)
        : 10000;
  }

  public static short getBearerAuthCacheExpiryBufferSeconds(Map<String, ?> configs) {
    return configs != null && configs.containsKey(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS)
        ? (Short) configs.get(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS)
        : BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DEFAULT;
  }

  public static String getBearerAuthScopeClaimName(Map<String, ?> configs) {
    return configs != null && configs.containsKey(BEARER_AUTH_SCOPE_CLAIM_NAME)
        ? (String) configs.get(BEARER_AUTH_SCOPE_CLAIM_NAME)
        : BEARER_AUTH_SCOPE_CLAIM_NAME_DEFAULT;
  }

  public static String getBearerAuthSubClaimName(Map<String, ?> configs) {
    return configs != null && configs.containsKey(BEARER_AUTH_SUB_CLAIM_NAME)
        ? (String) configs.get(BEARER_AUTH_SUB_CLAIM_NAME)
        : BEARER_AUTH_SUB_CLAIM_NAME_DEFAULT;
  }

}
