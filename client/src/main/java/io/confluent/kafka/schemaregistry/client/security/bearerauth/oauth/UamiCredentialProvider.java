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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;

import java.net.URL;
import java.util.Map;

/**
 * <code>UamiCredentialProvider</code> is a {@link BearerAuthCredentialProvider} that authenticates
 * to Schema Registry using an Azure User Assigned Managed Identity (UAMI).
 *
 * <p>It retrieves a bearer token from the Azure Instance Metadata Service (IMDS) using
 * {@link UamiJwtRetriever}, caches the token via {@link CachedOauthTokenRetriever}, and
 * proactively refreshes it before expiry using {@link OauthTokenCache}.
 *
 * <p>Required configuration:
 * <ul>
 *   <li>{@link SchemaRegistryClientConfig#BEARER_AUTH_ISSUER_ENDPOINT_QUERY} - The pre-formatted
 *       query string appended to the IMDS endpoint URL (e.g.
 *       {@code api-version=2025-04-07&resource=https%3A%2F%2Fconfluent.azure.com&client_id=...}).
 *       </li>
 * </ul>
 *
 * <p>Optional configuration:
 * <ul>
 *   <li>{@link SchemaRegistryClientConfig#BEARER_AUTH_UAMI_ENDPOINT_URL} - IMDS endpoint URL.
 *       Defaults to {@link UamiJwtRetriever#DEFAULT_IMDS_ENDPOINT}. Override for Azure Arc or
 *       non-standard environments.</li>
 *   <li>{@link SchemaRegistryClientConfig#BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS} - Seconds
 *       before token expiry to treat the cached token as stale. Defaults to 300.</li>
 *   <li>{@link SchemaRegistryClientConfig#BEARER_AUTH_SCOPE_CLAIM_NAME} - JWT claim used for
 *       scope. Defaults to {@code scope}; set to {@code scp} for standard Azure tokens.</li>
 *   <li>{@link SchemaRegistryClientConfig#BEARER_AUTH_SUB_CLAIM_NAME} - JWT claim used for
 *       subject. Defaults to {@code sub}.</li>
 *   <li>{@link SchemaRegistryClientConfig#BEARER_AUTH_LOGICAL_CLUSTER} - Target Schema Registry
 *       logical cluster ID.</li>
 *   <li>{@link SchemaRegistryClientConfig#BEARER_AUTH_IDENTITY_POOL_ID} - Target identity pool
 *       ID.</li>
 * </ul>
 *
 * <p>To activate, set:
 * <pre>
 *   bearer.auth.credentials.source=UAMI
 *   bearer.auth.issuer.endpoint.query=api-version=2025-04-07&amp;resource=...&amp;client_id=...
 * </pre>
 */
public class UamiCredentialProvider implements BearerAuthCredentialProvider {

  private CachedOauthTokenRetriever cachedTokenRetriever;
  private String targetSchemaRegistry;
  private String targetIdentityPoolId;

  @Override
  public String alias() {
    return "UAMI";
  }

  @Override
  public String getBearerToken(URL url) {
    return cachedTokenRetriever.getToken();
  }

  @Override
  public String getTargetSchemaRegistry() {
    return targetSchemaRegistry;
  }

  @Override
  public String getTargetIdentityPoolId() {
    return targetIdentityPoolId;
  }

  @Override
  public void configure(Map<String, ?> map) {
    ConfigurationUtils cu = new ConfigurationUtils(map);
    targetSchemaRegistry = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, false);
    targetIdentityPoolId = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID, false);

    cachedTokenRetriever = new CachedOauthTokenRetriever();
    cachedTokenRetriever.configure(
        buildJwtRetriever(cu),
        buildJwtValidator(map),
        buildTokenCache(map));
  }

  private UamiJwtRetriever buildJwtRetriever(ConfigurationUtils cu) {
    String query = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_QUERY);
    String endpointUrl = SchemaRegistryClientConfig.getUamiEndpointUrl(
        cu.validateString(SchemaRegistryClientConfig.BEARER_AUTH_UAMI_ENDPOINT_URL, false));
    return new UamiJwtRetriever(
        query,
        endpointUrl,
        SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS,
        SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS,
        null,
        null);
  }

  private ClientJwtValidator buildJwtValidator(Map<String, ?> map) {
    String scopeClaimName = SchemaRegistryClientConfig.getBearerAuthScopeClaimName(map);
    String subClaimName = SchemaRegistryClientConfig.getBearerAuthSubClaimName(map);
    return new ClientJwtValidator(scopeClaimName, subClaimName);
  }

  private OauthTokenCache buildTokenCache(Map<String, ?> map) {
    short cacheExpiryBufferSeconds =
        SchemaRegistryClientConfig.getBearerAuthCacheExpiryBufferSeconds(map);
    return new OauthTokenCache(cacheExpiryBufferSeconds);
  }
}
