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

package io.confluent.kafka.schemaregistry.client.security.basicauth;

import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;

import java.net.URL;
import java.util.Map;

public class UsernamePasswordCredentialProvider extends AbstractBasicAuthCredentialProvider {

  private String userInfo;

  @Override
  public void configure(Map<String, ?> configs) throws ConfigException {
    String user = (String) configs.get(SchemaRegistryClientConfig.SCHEMA_REGISTRY_USERNAME_CONFIG);
    String pass = (String) configs.get(SchemaRegistryClientConfig.SCHEMA_REGISTRY_PASSWORD_CONFIG);
    if (user == null || user.isEmpty() || pass == null || pass.isEmpty()) {
      throw new ConfigException("Username and password must be provided when "
          + "basic.auth.credentials.source is set to USERNAME_PASSWORD");
    }
    userInfo = user + ":" + pass;
  }

  @Override
  public String getUserInfo(URL url) {
    return userInfo;
  }

}
