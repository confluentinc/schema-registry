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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class HttpBasicCredentialProvider implements HttpCredentialProvider {
  private String userInfo;

  public HttpBasicCredentialProvider() { }

  public HttpBasicCredentialProvider(String userInfo) {
    this.userInfo = encodeUserInfo(userInfo);
  }

  private String encodeUserInfo(String userInfo) {
    return Base64.getEncoder().encodeToString(userInfo.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (this.userInfo != null) {
      throw new IllegalStateException("HttpBasicCredentialProvider already initialized");
    }

    String basicAuthProvider =
            (String) configs.get(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE);

    BasicAuthCredentialProvider basicAuthCredentialProvider =
            BuiltInAuthProviders.loadBasicAuthCredentialProvider(basicAuthProvider);

    basicAuthCredentialProvider.configure(configs);
    this.userInfo = encodeUserInfo(basicAuthCredentialProvider.getUserInfo());
  }

  @Override
  public String getScheme() {
    return "Basic";
  }

  @Override
  public String getCredentials() {
    return this.userInfo;
  }
}
