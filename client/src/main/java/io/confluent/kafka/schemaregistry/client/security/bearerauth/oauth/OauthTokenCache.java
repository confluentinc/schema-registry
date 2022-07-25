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

import java.time.Instant;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

public class OauthTokenCache {

  public static final float CACHE_PERCENTAGE_THRESHOLD = 0.8f;
  private OAuthBearerToken currentToken;
  private long expiresAtMs;

  public OAuthBearerToken getCurrentToken() {
    return currentToken;
  }

  public void setCurrentToken(OAuthBearerToken currentToken) {
    if (currentToken != null) {
      updateExpiryTime(currentToken.lifetimeMs() - currentToken.startTimeMs());
    }

    this.currentToken = currentToken;
  }

  private void updateExpiryTime(long lifespanMs) {
    this.expiresAtMs = (long) Math.floor(lifespanMs * CACHE_PERCENTAGE_THRESHOLD)
        + Instant.now().toEpochMilli();
  }

  public boolean isTokenExpired() {
    return currentToken == null || Instant.now().toEpochMilli() >= expiresAtMs;
  }

}