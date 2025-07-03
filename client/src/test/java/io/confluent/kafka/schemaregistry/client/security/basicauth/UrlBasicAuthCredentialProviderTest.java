/*
 * Copyright 2015-2018 Confluent Inc.
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

import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;

public class UrlBasicAuthCredentialProviderTest {

  @Test
  public void testUrlUserInfo() throws MalformedURLException {
    Map<String, Object> clientConfig = new HashMap<>();
    UrlBasicAuthCredentialProvider provider = new UrlBasicAuthCredentialProvider();
    provider.configure(clientConfig);
    Assert.assertEquals("user:password",
        provider.getUserInfo(new URL("http://user:password@localhost")));
  }

  @Test
  public void testNullUserInfo() throws MalformedURLException {
    Map<String, Object> clientConfig = new HashMap<>();
    UrlBasicAuthCredentialProvider provider = new UrlBasicAuthCredentialProvider();
    provider.configure(clientConfig);
    Assert.assertNull(provider.getUserInfo(new URL("http://localhost")));
  }

}
