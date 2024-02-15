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

package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth;

import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OauthCredentialProviderTest {

  @Mock
  CachedOauthTokenRetriever tokenRetriever;

  @InjectMocks
  OauthCredentialProvider oAuthCredentialProvider = new OauthCredentialProvider();

  private String tokenString = "dummy-token";

  private Map<String, Object> CONFIG_MAP;

  @Before
  public void InitializeConfigMap() {
    CONFIG_MAP = new HashMap<>();
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, "lsrc-dummy");
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID, "my-pool-id");
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_SCOPE, "test-scope");
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET, "mysecret");
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID, "myid");
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL, "https://okta.com");
  }

  @Test
  public void TestGetBearerToken() throws MalformedURLException {
    when(tokenRetriever.getToken()).thenReturn(tokenString);
    Assert.assertEquals(tokenString,
        oAuthCredentialProvider.getBearerToken(new URL("https://dummy.com")));
  }

  @Test
  public void TestConfigureInsufficientConfigs() {
    List<String> optionalConfigs = Arrays.asList(SchemaRegistryClientConfig.BEARER_AUTH_SCOPE,
        SchemaRegistryClientConfig.BEARER_AUTH_SCOPE_CLAIM_NAME,
        SchemaRegistryClientConfig.BEARER_AUTH_SUB_CLAIM_NAME,
        SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID,
        SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER,
        "schema.registry.ssl.truststore.location");
    for (String missingKey : CONFIG_MAP.keySet()) {
      // ignoring optional keys
      if (optionalConfigs.contains(missingKey)) {
        continue;
      }

      Assert.assertThrows(
          String.format("The OAuth configuration option %s value must be non-null", missingKey),
          ConfigException.class,
          () -> {
            oAuthCredentialProvider.configure(getInsufficentConfigs(missingKey));
          });

    }
  }

  @Test
  public void testClientSslConfigurations() throws MalformedURLException {

    Map<String, Object> CONFIG_WITH_SSL = new HashMap<>(CONFIG_MAP);
    CONFIG_WITH_SSL.put("ssl.truststore.location", "truststore.jks");
    CONFIG_WITH_SSL.put("ssl.truststore.password", "password");

    // SSL configurations should get loaded if present in configuration
    Assert.assertThrows("Message", KafkaException.class,
        () -> {
          oAuthCredentialProvider.configure(CONFIG_WITH_SSL);
        });

  }

  private Map<String, Object> getInsufficentConfigs(String missingConfig) {
    Map<String, Object> insufficentCofigs = new HashMap<>(CONFIG_MAP);
    insufficentCofigs.remove(missingConfig);
    return insufficentCofigs;
  }

}
