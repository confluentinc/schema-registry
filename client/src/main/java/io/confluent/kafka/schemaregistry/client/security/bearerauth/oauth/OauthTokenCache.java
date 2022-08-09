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
import java.util.Date;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>OauthTokenCache {</code> is a simple  {@link OAuthBearerToken} Cache.
 * Users can call {@link #isTokenExpired() isTokenExpired()} method to check if expired cache. If
 * the cache is expired then users can call {@link #setCurrentToken(OAuthBearerToken)
 * setCurrentToken(OAuthBearerToken)} to set a newly retrieved token. {@link
 * #calculateTokenExpiryTime calculateTokenRenRefreshTime()} is calculated from the token's  at time
 * of setting the token using {@link #setCurrentToken(OAuthBearerToken)
 * setCurrentToken(OAuthBearerToken)}. In order to encourage fetching a new token in advance before
 * the token expiry, {@link #calculateTokenExpiryTime calculateTokenRenRefreshTime()} is altered by
 * multiplying {@link #CACHE_EXPIRY_THRESHOLD CACHE_EXPIRY_THRESHOLD()}. Thus we consider a shorter
 * lifespan than actual.
 */

public class OauthTokenCache {

  public static final float CACHE_EXPIRY_THRESHOLD = 0.8f;
  private static final Logger log = LoggerFactory.getLogger(OauthTokenCache.class);
  private final short cacheExpiryBufferSeconds;
  private OAuthBearerToken currentToken;
  private long cacheExpiryMs;

  public OauthTokenCache(short cacheExpiryBufferSeconds) {
     /*
     setting a value of 0 so that isTokenExpired() returns true in when called the first time
     cacheExpiryMs time of 0L means this cache does not have a valid token.
    */
    this.cacheExpiryMs = 0L;
    this.cacheExpiryBufferSeconds = cacheExpiryBufferSeconds;
  }

  public OAuthBearerToken getCurrentToken() {
    return currentToken;
  }

  public void setCurrentToken(OAuthBearerToken currentToken) {
    if (currentToken == null) {
      // if token null, set cacheExpiryMs to initial state.
      cacheExpiryMs = 0L;
      return;
    }

    this.cacheExpiryMs = calculateTokenExpiryTime(currentToken);
    this.currentToken = currentToken;
  }

  private long calculateTokenExpiryTime(OAuthBearerToken currentToken) {
    long nowMs = Instant.now().toEpochMilli();
    long tokenExpiryMs = currentToken.lifetimeMs();
    // If the current time is greater than tokenExpiryMs we log the possible clock skew message return
    // the maximum time i.e expiresMs
    if (nowMs > tokenExpiryMs) {
      log.error(
          "Schema Registry OAuth Token [Principal={}]: Current clock: {} is later than expiry {}. This may indicate a clock skew problem."
              + " Check that this host's and remote host's clocks are in sync."
              + " This process is likely unable to authenticate SASL connections (for example, it is unlikely"
              + " to be able to authenticate a connection with a Schema Registry).",
          currentToken.principalName(), new Date(nowMs), new Date(tokenExpiryMs));

      return tokenExpiryMs;
    }

    Long optionalStartTime = currentToken.startTimeMs();
    long startMs = optionalStartTime != null ? optionalStartTime.longValue() : nowMs;

    long cacheExpiryMs;
    if (nowMs + 1000L * cacheExpiryBufferSeconds > tokenExpiryMs) {
      cacheExpiryMs = nowMs + (long) Math.floor((tokenExpiryMs - nowMs) * CACHE_EXPIRY_THRESHOLD);
      log.warn(
          "Schema Registry OAuth Token [Principal={}]: OAuth token expires at {}, so buffer times {} seconds"
              + "  cannot be accommodated.  OAuth token cache expires at {}.",
          currentToken.principalName(), new Date(tokenExpiryMs), cacheExpiryBufferSeconds,
          cacheExpiryMs);
      return cacheExpiryMs;
    }

    cacheExpiryMs = startMs + (long) ((tokenExpiryMs - startMs) * CACHE_EXPIRY_THRESHOLD);
    // Don't let it violate the requested end buffer time
    long beginningOfEndBufferTimeMs = tokenExpiryMs - cacheExpiryBufferSeconds * 1000;
    if (cacheExpiryMs > beginningOfEndBufferTimeMs) {
      log.info(
          "Schema Registry OAuth Token [Principal={}]: Proposed token Cache expiry time of {} extends into the desired buffer time of {} "
              + "seconds before token expiration, so invalidate token cache at the desired buffer begin point, at {}",
          currentToken.principalName(), new Date(cacheExpiryMs), cacheExpiryBufferSeconds,
          new Date(beginningOfEndBufferTimeMs));
      return beginningOfEndBufferTimeMs;
    }
    return cacheExpiryMs;

  }

  public boolean isTokenExpired() {
    return Instant.now().toEpochMilli() >= cacheExpiryMs;
  }

}