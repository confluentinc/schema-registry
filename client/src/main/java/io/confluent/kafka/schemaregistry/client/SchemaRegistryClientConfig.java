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

public class SchemaRegistryClientConfig {

  public static final String CLIENT_NAMESPACE = "schema.registry.";

  public static final String BASIC_AUTH_CREDENTIALS_SOURCE = "basic.auth.credentials.source";
  @Deprecated
  public static final String SCHEMA_REGISTRY_USER_INFO_CONFIG =
      "schema.registry.basic.auth.user.info";
  public static final String USER_INFO_CONFIG = "basic.auth.user.info";

  public static final String BEARER_AUTH_CREDENTIALS_SOURCE = "bearer.auth.credentials.source";
  public static final String BEARER_AUTH_TOKEN_CONFIG = "bearer.auth.token";

  public static final String PROXY_HOST = "proxy.host";
  public static final String PROXY_PORT = "proxy.port";

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

}
