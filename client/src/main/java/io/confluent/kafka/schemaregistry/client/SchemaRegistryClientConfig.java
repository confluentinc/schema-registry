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
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Map;
import java.util.stream.Collectors;


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
  public static final String HTTP_CONNECT_TIMEOUT_MS = "http.connect.timeout.ms";
  public static final int HTTP_CONNECT_TIMEOUT_MS_DEFAULT = 60000;
  public static final String HTTP_READ_TIMEOUT_MS = "http.read.timeout.ms";
  public static final int HTTP_READ_TIMEOUT_MS_DEFAULT = 60000;

  public static final String MAX_RETRIES_CONFIG = "max.retries";
  public static final int MAX_RETRIES_DEFAULT = 3;
  public static final String RETRIES_WAIT_MS_CONFIG = "retries.wait.ms";
  public static final int RETRIES_WAIT_MS_DEFAULT = 1000;
  public static final String RETRIES_MAX_WAIT_MS_CONFIG = "retries.max.wait.ms";
  public static final int RETRIES_MAX_WAIT_MS_DEFAULT = 20000;

  public static final String BEARER_AUTH_CREDENTIALS_SOURCE = "bearer.auth.credentials.source";
  public static final String BEARER_AUTH_TOKEN_CONFIG = "bearer.auth.token";

  public static final String PROXY_HOST = "proxy.host";
  public static final String PROXY_PORT = "proxy.port";

  public static final String LATEST_CACHE_TTL_CONFIG = "latest.cache.ttl.sec";
  public static final long LATEST_CACHE_TTL_DEFAULT = 60;

  public static final String MISSING_CACHE_SIZE_CONFIG = "missing.cache.size";
  public static final String MISSING_ID_CACHE_TTL_CONFIG = "missing.id.cache.ttl.sec";
  public static final String MISSING_VERSION_CACHE_TTL_CONFIG = "missing.version.cache.ttl.sec";
  public static final String MISSING_SCHEMA_CACHE_TTL_CONFIG = "missing.schema.cache.ttl.sec";

  public static final String URL_RANDOMIZE = "url.randomize";
  public static final boolean URL_RANDOMIZE_DEFAULT = false;

  public static final String USE_APACHE_HTTP_CLIENT = "use.apache.http.client";
  public static final boolean USE_APACHE_HTTP_CLIENT_DEFAULT = false;

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
  public static final String BEARER_AUTH_IDENTITY_POOL_ID = "bearer.auth.identity.pool.id";

  //OAuth config related to token cache
  public static final String BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS =
      "bearer.auth.cache.expiry.buffer.seconds";
  public static final short BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DEFAULT = 300;

  //Custom bearer Auth related
  public static final String BEARER_AUTH_CUSTOM_PROVIDER_CLASS =
      "bearer.auth.custom.provider.class";

  public static final String SSL_PREFIX = "ssl.";

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

  public static Integer getHttpConnectTimeoutMs(Map<String, ?> configs) {
    return configs != null && configs.containsKey(HTTP_CONNECT_TIMEOUT_MS)
        ? Integer.parseInt(configs.get(HTTP_CONNECT_TIMEOUT_MS).toString())
        : HTTP_CONNECT_TIMEOUT_MS_DEFAULT;
  }

  public static Integer getHttpReadTimeoutMs(Map<String, ?> configs) {
    return configs != null && configs.containsKey(HTTP_READ_TIMEOUT_MS)
        ? Integer.parseInt(configs.get(HTTP_READ_TIMEOUT_MS).toString())
        : HTTP_READ_TIMEOUT_MS_DEFAULT;
  } 

  public static Integer getMaxRetries(Map<String, ?> configs) {
    return configs != null && configs.containsKey(MAX_RETRIES_CONFIG)
        ? Integer.parseInt(configs.get(MAX_RETRIES_CONFIG).toString())
        : MAX_RETRIES_DEFAULT;
  }

  public static Integer getRetriesWaitMs(Map<String, ?> configs) {
    return configs != null && configs.containsKey(RETRIES_WAIT_MS_CONFIG)
        ? Integer.parseInt(configs.get(RETRIES_WAIT_MS_CONFIG).toString())
        : RETRIES_WAIT_MS_DEFAULT;
  }

  public static Integer getRetriesMaxWaitMs(Map<String, ?> configs) {
    return configs != null && configs.containsKey(RETRIES_MAX_WAIT_MS_CONFIG)
        ? Integer.parseInt(configs.get(RETRIES_MAX_WAIT_MS_CONFIG).toString())
        : RETRIES_MAX_WAIT_MS_DEFAULT;
  }

  public static long getLatestTTL(Map<String, ?> configs) {
    return configs != null && configs.containsKey(LATEST_CACHE_TTL_CONFIG)
        ? Long.parseLong(configs.get(LATEST_CACHE_TTL_CONFIG).toString())
        : LATEST_CACHE_TTL_DEFAULT;
  }

  public static long getMissingIdTTL(Map<String, ?> configs) {
    return configs != null && configs.containsKey(MISSING_ID_CACHE_TTL_CONFIG)
        ? Long.parseLong(configs.get(MISSING_ID_CACHE_TTL_CONFIG).toString())
        : 0L;
  }

  public static long getMissingVersionTTL(Map<String, ?> configs) {
    return configs != null && configs.containsKey(MISSING_VERSION_CACHE_TTL_CONFIG)
        ? Long.parseLong(configs.get(MISSING_VERSION_CACHE_TTL_CONFIG).toString())
        : 0L;
  }

  public static long getMissingSchemaTTL(Map<String, ?> configs) {
    return configs != null && configs.containsKey(MISSING_SCHEMA_CACHE_TTL_CONFIG)
        ? Long.parseLong((configs.get(MISSING_SCHEMA_CACHE_TTL_CONFIG).toString()))
        : 0L;
  }

  public static int getMaxMissingCacheSize(Map<String, ?> configs) {
    return configs != null && configs.containsKey(MISSING_CACHE_SIZE_CONFIG)
        ? Integer.parseInt(configs.get(MISSING_CACHE_SIZE_CONFIG).toString())
        : 10000;
  }

  public static short getBearerAuthCacheExpiryBufferSeconds(Map<String, ?> configs) {
    return configs != null && configs.containsKey(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS)
        ? Short.parseShort(((Short) 
        configs.get(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS)).toString())
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

  public static Map<String, Object> getClientSslConfig(Map<String, ?> configs) {
    return configs.entrySet().stream()
        .filter(e -> e.getKey().startsWith(SSL_PREFIX))
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
  }

  public static boolean getUrlRandomize(Map<String, ?> configs) {
    return configs != null && configs.containsKey(URL_RANDOMIZE)
        ? Boolean.parseBoolean(configs.get(URL_RANDOMIZE).toString())
        : URL_RANDOMIZE_DEFAULT;
  }

  public static boolean useApacheHttpClient(Map<String, ?> configs) {
    return configs != null && configs.containsKey(USE_APACHE_HTTP_CLIENT)
        ? Boolean.parseBoolean(configs.get(USE_APACHE_HTTP_CLIENT).toString())
        : USE_APACHE_HTTP_CLIENT_DEFAULT;
  }
}