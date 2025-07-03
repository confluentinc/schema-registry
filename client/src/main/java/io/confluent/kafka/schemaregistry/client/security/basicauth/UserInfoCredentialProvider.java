/*
 * Copyright 2014-2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.security.basicauth;

import org.apache.kafka.common.config.ConfigException;

import java.net.URL;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;

public class UserInfoCredentialProvider implements BasicAuthCredentialProvider {

  private String userInfo;

  @Override
  public String alias() {
    return "USER_INFO";
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // If deprecated name is defined, use that
    userInfo = (String) configs.get(SchemaRegistryClientConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG);
    if (userInfo != null && !userInfo.isEmpty()) {
      return;
    }

    userInfo = (String) configs.get(SchemaRegistryClientConfig.USER_INFO_CONFIG);
    if (userInfo != null && !userInfo.isEmpty()) {
      return;
    }

    throw new ConfigException("UserInfo must be provided when basic.auth.credentials.source is "
                              + "set to USER_INFO");
  }

  @Override
  public String getUserInfo(URL url) {
    return userInfo;
  }
}
