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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BearerAuthCredentialProviderFactoryTest {
  private Map<String, String> CONFIG_MAP = new HashMap<>();

  @Before
  public void setup() throws IOException {
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG, "auth-token");
  }

  @Test
  public void testSuccess() {
    assertInstance(BearerAuthCredentialProviderFactory.getBearerAuthCredentialProvider(
            "STATIC_TOKEN", CONFIG_MAP), StaticTokenCredentialProvider.class);
  }

  @Test
  public void testUnknownProvider() {
    Assert.assertNull(BearerAuthCredentialProviderFactory.getBearerAuthCredentialProvider(
            "UNKNOWN", CONFIG_MAP));
  }

  public void assertInstance(BearerAuthCredentialProvider instance,
                             Class<? extends BearerAuthCredentialProvider> klass) {
    Assert.assertNotNull(instance);
    Assert.assertEquals(klass, instance.getClass());
  }
}
