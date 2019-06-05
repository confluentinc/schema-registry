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

package io.confluent.kafka.schemaregistry.client.security.bearerauth;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.apache.kafka.common.config.ConfigException;

import java.net.URL;
import java.util.Map;

public class StaticTokenCredentialProvider implements BearerAuthCredentialProvider {

  private String userInfo;

  @Override
  public String alias() {
    return "STATIC_TOKEN";
  }

  @Override
  public void configure(Map<String, ?> configs) {
    userInfo = (String) configs.get(SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG);
    if (userInfo != null && !userInfo.isEmpty()) {
      return;
    }

    throw new ConfigException(String.format(
        "Token must be provided via %s config when %s is set to %s",
        SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG,
        SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE,
        alias()
    ));
  }

  @Override
  public String getBearerToken(URL url) {
    return userInfo;
  }
}
