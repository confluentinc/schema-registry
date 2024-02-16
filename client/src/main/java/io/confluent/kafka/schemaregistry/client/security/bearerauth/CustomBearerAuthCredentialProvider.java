/*
 * Copyright 2023 Confluent Inc.
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
import java.net.URL;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.secured.ConfigurationUtils;

public class CustomBearerAuthCredentialProvider implements BearerAuthCredentialProvider {

  BearerAuthCredentialProvider customBearerAuthCredentialProvider;
  private String targetSchemaRegistry;
  private String targetIdentityPoolId;

  @Override
  public String alias() {
    return "CUSTOM";
  }

  @Override
  public String getBearerToken(URL url) {
    return this.customBearerAuthCredentialProvider.getBearerToken(url);
  }

  @Override
  public String getTargetSchemaRegistry() {
    return this.customBearerAuthCredentialProvider.getTargetSchemaRegistry() != null
        ? this.customBearerAuthCredentialProvider.getTargetSchemaRegistry()
        : this.targetSchemaRegistry;
  }

  @Override
  public String getTargetIdentityPoolId() {
    return this.customBearerAuthCredentialProvider.getTargetIdentityPoolId() != null
        ? this.customBearerAuthCredentialProvider.getTargetIdentityPoolId()
        : this.targetIdentityPoolId;
  }

  @Override
  public void configure(Map<String, ?> map) {
    ConfigurationUtils cu = new ConfigurationUtils(map);
    String className = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_CUSTOM_PROVIDER_CLASS);
    try {
      this.customBearerAuthCredentialProvider =
          (BearerAuthCredentialProvider) Class.forName(className)
          .getDeclaredConstructor()
          .newInstance();
    } catch (Exception e) {
      throw new ConfigException(String.format(
          "Unable to instantiate an object of class %s, failed with exception: ",
          SchemaRegistryClientConfig.BEARER_AUTH_CUSTOM_PROVIDER_CLASS
      ) + e.getMessage());
    }
    targetSchemaRegistry = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, false);
    targetIdentityPoolId = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID, false);
    this.customBearerAuthCredentialProvider.configure(map);
  }
}
