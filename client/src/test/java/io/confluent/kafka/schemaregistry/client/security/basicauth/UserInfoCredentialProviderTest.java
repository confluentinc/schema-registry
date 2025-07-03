/*
 * Copyright 2017-2019 Confluent Inc.
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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;

@RunWith(Parameterized.class)
public class UserInfoCredentialProviderTest {

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { SchemaRegistryClientConfig.USER_INFO_CONFIG },
        { SchemaRegistryClientConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG }
    });
  }

  @Parameter
  public String userInfoConfigName;

  @Test
  public void testUserInfo() throws MalformedURLException {
    Map<String, Object> clientConfig = new HashMap<>();
    clientConfig.put(userInfoConfigName, "user:password");
    UserInfoCredentialProvider provider = new UserInfoCredentialProvider();
    provider.configure(clientConfig);
    Assert.assertEquals("user:password", provider.getUserInfo(new URL("http://localhost")));
  }

  @Test(expected = ConfigException.class)
  public void testNullUserInfo() throws MalformedURLException {
    Map<String, Object> clientConfig = new HashMap<>();
    UserInfoCredentialProvider provider = new UserInfoCredentialProvider();
    provider.configure(clientConfig);
  }

}
