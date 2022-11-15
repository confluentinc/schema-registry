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
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.secured.HttpAccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.secured.JaasOptionsUtils;
import org.apache.kafka.common.security.oauthbearer.secured.LoginAccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler;

public class SaslOAuthCredentialProvider implements BearerAuthCredentialProvider {

  public final String SASL_IDENTITY_POOL_CONFIG = "extension_logicalCluster";
  private CachedOauthTokenRetriever tokenRetriever;
  private String targetSchemaRegistry;
  private String targetIdentityPoolId;

  @Override
  public String alias() {
    return "SASL_OAUTHBEARER_INHERIT";
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

  @Override
  public void configure(Map<String, ?> configs) {
    Map<String, Object> updatedConfigs = getConfigsForJaasUtil(configs);
    JaasContext jaasContext = JaasContext.loadClientContext(updatedConfigs);
    List<AppConfigurationEntry> appConfigurationEntries = jaasContext.configurationEntries();
    Map<String, ?> jaasconfig;
    if (Objects.requireNonNull(appConfigurationEntries).size() == 1
        && appConfigurationEntries.get(0) != null) {
      jaasconfig = Collections.unmodifiableMap(
          ((AppConfigurationEntry) appConfigurationEntries.get(0)).getOptions());
    } else {
      throw new ConfigException(
          String.format("Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
              appConfigurationEntries.size()));
    }

    ConfigurationUtils cu = new ConfigurationUtils(configs);
    JaasOptionsUtils jou = new JaasOptionsUtils((Map<String, Object>) jaasconfig);

    targetSchemaRegistry = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER);

    // if the schema registry oauth configs are set it is higher preference
    if (cu.get(SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID) != null) {
      targetIdentityPoolId = cu.validateString(
          SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID);
    } else {
      targetIdentityPoolId = jou.validateString(SASL_IDENTITY_POOL_CONFIG);
    }

    tokenRetriever = new CachedOauthTokenRetriever();
    tokenRetriever.configure(getTokenRetriever(cu, jou), getTokenValidator(),
        getOauthTokenCache(configs));
  }


  private OauthTokenCache getOauthTokenCache(Map<String, ?> map) {
    short cacheExpiryBufferSeconds = SchemaRegistryClientConfig.
        getBearerAuthCacheExpiryBufferSeconds(map);
    return new OauthTokenCache(cacheExpiryBufferSeconds);
  }

  private AccessTokenRetriever getTokenRetriever(ConfigurationUtils cu, JaasOptionsUtils jou) {
    String clientId;
    if (cu.get(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID) != null) {
       clientId = cu.validateString(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID);
    } else {
      clientId = jou.validateString(OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG);
    }
    String clientSecret;

    if(cu.get(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET) != null) {
      clientSecret = cu.validateString(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET);
    } else {
      clientSecret = jou.validateString(OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG);
    }

    String scope;
    if(cu.get(SchemaRegistryClientConfig.BEARER_AUTH_SCOPE) != null) {
      scope = cu.validateString(SchemaRegistryClientConfig.BEARER_AUTH_SCOPE);
    } else {
      scope = jou.validateString(OAuthBearerLoginCallbackHandler.SCOPE_CONFIG, false);
    }

    //Keeping following configs needed by HttpAccessTokenRetriever as constants and not exposed to
    //users for modifications
    Long retryBackoffMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS;
    Long retryBackoffMaxMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
    Integer loginConnectTimeoutMs = null;
    Integer loginReadTimeoutMs = null;

    SSLSocketFactory sslSocketFactory = null;

    URL url;
    if(cu.get(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL) != null) {
       url = cu.validateUrl(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL);
    } else {
      url = cu.validateUrl(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
    }

    if (jou.shouldCreateSSLSocketFactory(url)) {
      sslSocketFactory = jou.createSSLSocketFactory();
    }

    return new HttpAccessTokenRetriever(clientId, clientSecret, scope, sslSocketFactory,
        url.toString(), retryBackoffMs, retryBackoffMaxMs, loginConnectTimeoutMs, loginReadTimeoutMs
    );
  }

  private AccessTokenValidator getTokenValidator() {
    //Keeping following configs needed by LoginAccessTokenValidator as constants and not exposed to
    //users for modifications
    String scopeClaimName = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
    String subClaimName = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME;
    return new LoginAccessTokenValidator(scopeClaimName, subClaimName);
  }

  Map<String, Object> getConfigsForJaasUtil(Map<String, ?> configs) {
    Map<String, Object> updatedConfigs = new HashMap<>(configs);
    if (updatedConfigs.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
      Object saslJaasConfig = updatedConfigs.get(SaslConfigs.SASL_JAAS_CONFIG);
      if (saslJaasConfig instanceof String) {
        updatedConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, new Password((String) saslJaasConfig));
      }
    }
    return updatedConfigs;
  }
}
