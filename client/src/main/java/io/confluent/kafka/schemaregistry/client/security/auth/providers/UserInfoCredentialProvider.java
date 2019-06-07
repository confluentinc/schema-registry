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

package io.confluent.kafka.schemaregistry.client.security.auth.providers;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

public class UserInfoCredentialProvider implements BasicAuthCredentialProvider {

  private String userInfo;

  @Override
  public String alias() {
    return "USER_INFO";
  }

  @Override
  public void configure(Map<String, ?> configs) {
    userInfo = (String) configs.get(SchemaRegistryClientConfig.USER_INFO_CONFIG);
    if (userInfo != null && !userInfo.isEmpty()) {
      return;
    }

    throw new ConfigException(SchemaRegistryClientConfig.USER_INFO_CONFIG
            + " must be provided when "
            + SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE + " is set to "
            + BuiltInAuthProviders.BasicAuthCredentialProviders.USER_INFO.name());
  }

  @Override
  public String getUserInfo() {
    return userInfo;
  }
}
