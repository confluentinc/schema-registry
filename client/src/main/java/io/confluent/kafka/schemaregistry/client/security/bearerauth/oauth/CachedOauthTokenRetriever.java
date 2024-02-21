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
import io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth.exceptions.SchemaRegistryOauthTokenRetrieverException;
import java.io.IOException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.secured.ValidateException;

/**
 * <p>
 * <code>CachedOauthTokenRetriever</code> is an wrapper around {@link AccessTokenRetriever} that
 * will communicate with an OAuth/OIDC provider directly via HTTP to post client credentials ({@link
 * SchemaRegistryClientConfig#BEARER_AUTH_CLIENT_ID}/
 * {@link SchemaRegistryClientConfig#BEARER_AUTH_CLIENT_SECRET})
 * to a publicized token endpoint URL
 * ({@link SchemaRegistryClientConfig#BEARER_AUTH_ISSUER_ENDPOINT_URL})
 * inorder to fetch an access token.
 * </p>
 * <p>
 * This class adds caching mechanism over {@link AccessTokenRetriever} using {@link
 * OauthTokenCache}
 * </p>
 *
 * @author Varun PV
 */

class CachedOauthTokenRetriever {

  private AccessTokenRetriever accessTokenRetriever;
  private AccessTokenValidator accessTokenValidator;
  private OauthTokenCache oauthTokenCache;


  public void configure(AccessTokenRetriever accessTokenRetriever,
      AccessTokenValidator accessTokenValidator, OauthTokenCache oauthTokenCache) {
    this.accessTokenRetriever = accessTokenRetriever;
    this.accessTokenValidator = accessTokenValidator;
    this.oauthTokenCache = oauthTokenCache;

  }

  public String getToken() {
    if (oauthTokenCache.isTokenExpired()) {
      String token = null;
      try {
        token = accessTokenRetriever.retrieve();
      } catch (IOException | RuntimeException e) {
        throw new SchemaRegistryOauthTokenRetrieverException(
            "Failed to Retrieve OAuth Token for Schema Registry", e);
      }

      OAuthBearerToken oauthBearerToken;
      try {
        oauthBearerToken = accessTokenValidator.validate(token);
      } catch (ValidateException e) {
        throw new SchemaRegistryOauthTokenRetrieverException(
            "OAuth Token for Schema Registry is Invalid", e);
      }

      oauthTokenCache.setCurrentToken(oauthBearerToken);
    }
    return oauthTokenCache.getCurrentToken().value();
  }

}