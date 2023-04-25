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

import org.apache.kafka.common.config.ConfigException;

import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;

public class StaticTokenCredentialProviderTest {

  @Test
  public void testBearerToken() throws MalformedURLException {
    Map<String, Object> clientConfig = new HashMap<>();
    clientConfig.put(SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG, "auth-token");
    clientConfig.put(SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, "lsrc-xyz123");
    StaticTokenCredentialProvider provider = new StaticTokenCredentialProvider();
    provider.configure(clientConfig);
    Assert.assertEquals("auth-token", provider.getBearerToken(new URL("http://localhost")));
    Assert.assertEquals("lsrc-xyz123", provider.getTargetSchemaRegistry());
    Assert.assertNull(provider.getTargetIdentityPoolId());
  }

  @Test(expected = ConfigException.class)
  public void testNulBearerToken() throws MalformedURLException {
    Map<String, Object> clientConfig = new HashMap<>();
    StaticTokenCredentialProvider provider = new StaticTokenCredentialProvider();
    provider.configure(clientConfig);
  }

}
