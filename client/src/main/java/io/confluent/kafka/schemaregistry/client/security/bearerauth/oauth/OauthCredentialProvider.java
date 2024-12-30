/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpAccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.LoginAccessTokenValidator;

import javax.net.ssl.SSLSocketFactory;
import java.net.URL;
import java.util.Map;

/**
 * <code>OAuthCredentialProvider</code> is a <code>BearerAuthCredentialProvider</code>
 * implementation used for configuring OAuth in schema registry. This can be used when we want to
 * set up OAuth Independently to that of kafka.
 *
 * @author Varun PV
 */
public class OauthCredentialProvider implements BearerAuthCredentialProvider {

  private CachedOauthTokenRetriever tokenRetriever;
  private String targetSchemaRegistry;
  private String targetIdentityPoolId;

  @Override
  public String alias() {
    return "OAUTHBEARER";
  }

  @Override
  public String getBearerToken(URL url) {
    return tokenRetriever.getToken();
  }

  @Override
  public String getTargetSchemaRegistry() {
    return this.targetSchemaRegistry;
  }

  @Override
  public String getTargetIdentityPoolId() {
    return this.targetIdentityPoolId;
  }

  Map<String, ?> config;

  @Override
  public void configure(Map<String, ?> map) {
    this.config = map;
    ConfigurationUtils cu = new ConfigurationUtils(map);
    targetSchemaRegistry = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, false);
    targetIdentityPoolId = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID, false);

    tokenRetriever = new CachedOauthTokenRetriever();
    tokenRetriever.configure(getTokenRetriever(cu), getTokenValidator(map),
        getOauthTokenCache(map));
  }

  private OauthTokenCache getOauthTokenCache(Map<String, ?> map) {
    short cacheExpiryBufferSeconds = SchemaRegistryClientConfig
        .getBearerAuthCacheExpiryBufferSeconds(map);
    return new OauthTokenCache(cacheExpiryBufferSeconds);
  }

  private AccessTokenRetriever getTokenRetriever(ConfigurationUtils cu) {

    String clientId = cu.validateString(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID);
    String clientSecret = cu.validateString(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET);
    String scope = cu.validateString(SchemaRegistryClientConfig.BEARER_AUTH_SCOPE, false);

    //Keeping following configs needed by HttpAccessTokenRetriever as constants and not exposed to
    //users for modifications
    Long retryBackoffMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS;
    Long retryBackoffMaxMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
    Integer loginConnectTimeoutMs = null;
    Integer loginReadTimeoutMs = null;
    // Get client ssl configs if configured
    JaasOptionsUtils jou = new JaasOptionsUtils(
        SchemaRegistryClientConfig.getClientSslConfig(this.config));

    SSLSocketFactory sslSocketFactory = null;
    URL url = cu.validateUrl(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL);
    if (jou.shouldCreateSSLSocketFactory(url)) {
      sslSocketFactory = jou.createSSLSocketFactory();
    }

    return new HttpAccessTokenRetriever(clientId, clientSecret, scope, sslSocketFactory,
        url.toString(), retryBackoffMs, retryBackoffMaxMs, loginConnectTimeoutMs, loginReadTimeoutMs
    );
  }

  private AccessTokenValidator getTokenValidator(Map<String, ?> configs) {
    String scopeClaimName = SchemaRegistryClientConfig.getBearerAuthScopeClaimName(configs);
    String subClaimName = SchemaRegistryClientConfig.getBearerAuthSubClaimName(configs);
    return new LoginAccessTokenValidator(scopeClaimName, subClaimName);
  }

}
