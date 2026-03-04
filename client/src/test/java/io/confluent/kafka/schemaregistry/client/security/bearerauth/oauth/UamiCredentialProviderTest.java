/*
 * Copyright 2026 Confluent Inc.
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

import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class UamiCredentialProviderTest {

  // Injected so that getBearerToken() tests don't need a real IMDS endpoint
  @Mock
  CachedOauthTokenRetriever cachedTokenRetriever;

  @InjectMocks
  UamiCredentialProvider provider = new UamiCredentialProvider();

  private static final String LSRC_ID = "lsrc-dummy";
  private static final String POOL_ID = "my-pool-id";
  private static final String QUERY =
      "api-version=2025-04-07&resource=https%3A%2F%2Fconfluent.azure.com&client_id=uami-client-id";

  private Map<String, Object> configMap;

  @Before
  public void setUp() {
    configMap = new HashMap<>();
    configMap.put(SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, LSRC_ID);
    configMap.put(SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID, POOL_ID);
    configMap.put(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_QUERY, QUERY);
  }

  @Test
  public void testAlias() {
    Assert.assertEquals("UAMI", provider.alias());
  }

  @Test
  public void testGetBearerTokenDelegatesToRetriever() throws MalformedURLException {
    when(cachedTokenRetriever.getToken()).thenReturn("uami-bearer-token");

    String token = provider.getBearerToken(new URL("https://dummy.schema-registry.com"));

    Assert.assertEquals("uami-bearer-token", token);
  }

  @Test
  public void testConfigureSetsTargetSchemaRegistry() {
    provider.configure(configMap);

    Assert.assertEquals(LSRC_ID, provider.getTargetSchemaRegistry());
  }

  @Test
  public void testConfigureSetsTargetIdentityPoolId() {
    provider.configure(configMap);

    Assert.assertEquals(POOL_ID, provider.getTargetIdentityPoolId());
  }

  @Test
  public void testTargetFieldsNullWhenNotConfigured() {
    Map<String, Object> minimalConfig = new HashMap<>();
    minimalConfig.put(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_QUERY, QUERY);

    provider.configure(minimalConfig);

    Assert.assertNull(provider.getTargetSchemaRegistry());
    Assert.assertNull(provider.getTargetIdentityPoolId());
  }

  @Test
  public void testConfigureThrowsOnMissingQuery() {
    Map<String, Object> insufficient = new HashMap<>(configMap);
    insufficient.remove(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_QUERY);

    Assert.assertThrows(ConfigException.class, () -> provider.configure(insufficient));
  }

  @Test
  public void testConfigureSucceedsWithDefaultImdsEndpoint() {
    // No BEARER_AUTH_UAMI_ENDPOINT_URL set — must use the DEFAULT_IMDS_ENDPOINT without throwing
    provider.configure(configMap);
  }

  @Test
  public void testConfigureSucceedsWithCustomEndpointUrl() {
    configMap.put(SchemaRegistryClientConfig.BEARER_AUTH_UAMI_ENDPOINT_URL,
        "http://localhost:40342/metadata/identity/oauth2/token");

    provider.configure(configMap);
  }

  @Test
  public void testConfigureSucceedsWithCustomScopeClaimName() {
    // Azure tokens typically use 'scp' rather than 'scope'
    configMap.put(SchemaRegistryClientConfig.BEARER_AUTH_SCOPE_CLAIM_NAME, "scp");

    provider.configure(configMap);
  }

  @Test
  public void testConfigureSucceedsWithCustomSubClaimName() {
    // Azure tokens may use 'oid' (object ID) as the subject claim
    configMap.put(SchemaRegistryClientConfig.BEARER_AUTH_SUB_CLAIM_NAME, "oid");

    provider.configure(configMap);
  }

  @Test
  public void testConfigureSucceedsWithCustomCacheExpiryBuffer() {
    configMap.put(SchemaRegistryClientConfig.BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS,
        (short) 60);

    provider.configure(configMap);
  }

  // ---- getUamiEndpointUrl() tests ----

  @Test
  public void testGetUamiEndpointUrlReturnsConfiguredValue() {
    String custom = "http://localhost:40342/metadata/identity/oauth2/token";
    Assert.assertEquals(custom, SchemaRegistryClientConfig.getUamiEndpointUrl(custom));
  }

  @Test
  public void testGetUamiEndpointUrlReturnsDefaultForNull() {
    Assert.assertEquals(
        SchemaRegistryClientConfig.BEARER_AUTH_UAMI_ENDPOINT_URL_DEFAULT,
        SchemaRegistryClientConfig.getUamiEndpointUrl(null));
  }

  @Test
  public void testGetUamiEndpointUrlReturnsDefaultForEmptyString() {
    Assert.assertEquals(
        SchemaRegistryClientConfig.BEARER_AUTH_UAMI_ENDPOINT_URL_DEFAULT,
        SchemaRegistryClientConfig.getUamiEndpointUrl(""));
  }
}
