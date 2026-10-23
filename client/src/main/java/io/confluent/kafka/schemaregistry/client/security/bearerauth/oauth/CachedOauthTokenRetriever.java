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
import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.JwtRetriever;
import org.apache.kafka.common.security.oauthbearer.JwtValidator;
import org.apache.kafka.common.security.oauthbearer.JwtValidatorException;

/**
 * <p>
 * <code>CachedOauthTokenRetriever</code> is a wrapper around {@link JwtRetriever} that
 * will communicate with an OAuth/OIDC provider directly via HTTP to post client credentials ({@link
 * SchemaRegistryClientConfig#BEARER_AUTH_CLIENT_ID}/
 * {@link SchemaRegistryClientConfig#BEARER_AUTH_CLIENT_SECRET})
 * to a publicized token endpoint URL
 * ({@link SchemaRegistryClientConfig#BEARER_AUTH_ISSUER_ENDPOINT_URL})
 * inorder to fetch an access token.
 * </p>
 * <p>
 * This class adds caching mechanism over {@link JwtRetriever} using {@link
 * OauthTokenCache}
 * </p>
 *
 * @author Varun PV
 */

public class CachedOauthTokenRetriever {

  private JwtRetriever tokenRetriever;
  private JwtValidator tokenValidator;
  private OauthTokenCache oauthTokenCache;


  public void configure(JwtRetriever tokenRetriever, JwtValidator tokenValidator,
      OauthTokenCache oauthTokenCache) {
    this.tokenRetriever = tokenRetriever;
    this.tokenValidator = tokenValidator;
    this.oauthTokenCache = oauthTokenCache;

  }

  public String getToken() {
    if (oauthTokenCache.isTokenExpired()) {
      String token = null;
      try {
        token = tokenRetriever.retrieve();
      } catch (JwtRetrieverException e) {
        throw new SchemaRegistryOauthTokenRetrieverException(
            "Failed to Retrieve OAuth Token for Schema Registry", e);
      }

      OAuthBearerToken oauthBearerToken;
      try {
        oauthBearerToken = tokenValidator.validate(token);
      } catch (JwtValidatorException e) {
        throw new SchemaRegistryOauthTokenRetrieverException(
            "OAuth Token for Schema Registry is Invalid", e);
      }

      oauthTokenCache.setCurrentToken(oauthBearerToken);
    }
    return oauthTokenCache.getCurrentToken().value();
  }

}