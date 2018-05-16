/*
 * Copyright 2017 Confluent Inc.
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
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class UsernamePasswordCredentialProviderTest {

  @Test
  public void testUrlUserInfo() throws MalformedURLException {
    Map<String, Object> clientConfig = new HashMap<>();
    clientConfig.put(SchemaRegistryClientConfig.SCHEMA_REGISTRY_USERNAME_CONFIG, "user");
    clientConfig.put(SchemaRegistryClientConfig.SCHEMA_REGISTRY_PASSWORD_CONFIG, "password");
    UsernamePasswordCredentialProvider provider = new UsernamePasswordCredentialProvider();
    provider.configure(clientConfig);
    Assert.assertEquals("user:password",
        provider.getUserInfo(new URL("http://localhost")));
  }

  @Test
  public void testSpecialCharsInUrlUserInfo() throws MalformedURLException {
    Map<String, Object> clientConfig = new HashMap<>();
    clientConfig.put(SchemaRegistryClientConfig.SCHEMA_REGISTRY_USERNAME_CONFIG, "ü$ër");
    clientConfig.put(SchemaRegistryClientConfig.SCHEMA_REGISTRY_PASSWORD_CONFIG, "ϱα$swo|2d");
    UsernamePasswordCredentialProvider provider = new UsernamePasswordCredentialProvider();
    provider.configure(clientConfig);
    Assert.assertEquals("ü$ër:ϱα$swo|2d",
        provider.getUserInfo(new URL("http://localhost")));
  }

  @Test(expected = ConfigException.class)
  public void testMissingUserNameAndPassword() {
    Map<String, Object> clientConfig = new HashMap<>();
    UsernamePasswordCredentialProvider provider = new UsernamePasswordCredentialProvider();
    provider.configure(clientConfig);
  }

}
