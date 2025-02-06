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
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.secured.BasicOAuthBearerToken;
import org.junit.Assert;
import org.junit.Test;

public class OauthTokenCacheTest {

  private short cacheExpiryBufferSeconds = 1;
  private OauthTokenCache oAuthTokenCache = new OauthTokenCache(cacheExpiryBufferSeconds);

  private String tokenString1 = "token1";


  @Test
  public void TestSetCurrentToken() {
    OAuthBearerToken token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        100L,
        "random",
        0L);

    oAuthTokenCache.setCurrentToken(token1);
    Assert.assertEquals(tokenString1, oAuthTokenCache.getCurrentToken().value());
  }

  @Test
  public void TestIsExpiredWithNull() {
    oAuthTokenCache.setCurrentToken(null);
    Assert.assertEquals(true, oAuthTokenCache.isTokenExpired());
  }

  @Test
  public void TestIsExpiredWithValidCache() throws InterruptedException {
    Long lifespan = 2L;
    OAuthBearerToken token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        Instant.now().plusSeconds(lifespan).toEpochMilli(),
        "random",
        Instant.now().toEpochMilli());
    oAuthTokenCache.setCurrentToken(token1);
    Assert.assertEquals(false, oAuthTokenCache.isTokenExpired());
    Thread.sleep(10);
    Assert.assertEquals(false, oAuthTokenCache.isTokenExpired());
  }

  @Test
  public void TestIsExpiredWithExpiredCache() throws InterruptedException {
    Long lifespanSeconds = 2L;
    OAuthBearerToken token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        Instant.now().plusSeconds(lifespanSeconds).toEpochMilli(),
        "random",
        Instant.now().toEpochMilli());
    oAuthTokenCache.setCurrentToken(token1);
    //sleeping till cache get expired
    Thread.sleep(
        (long) Math.floor(1000 * lifespanSeconds * OauthTokenCache.CACHE_EXPIRY_THRESHOLD));
    Assert.assertEquals(true, oAuthTokenCache.isTokenExpired());
  }

  @Test
  public void TestCalculateTokenExpiryTime() {
    //already expired token
    long tokenStartTimeMs = Instant.now().plusSeconds(-3).toEpochMilli();
    long tokenExpiryTimeMs = Instant.now().plusSeconds(-1).toEpochMilli();
    OAuthBearerToken token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        tokenExpiryTimeMs,
        "random",
        tokenStartTimeMs);

    Assert.assertEquals(tokenExpiryTimeMs, oAuthTokenCache.calculateTokenExpiryTime(token1));

    //cache expiry time before requested cacheExpiryBufferSeconds seconds
    short cacheExpiryBufferSeconds = 5;
    OauthTokenCache oAuthTokenCache = new OauthTokenCache(cacheExpiryBufferSeconds);
    long lifetimeSeconds = 60L;
    tokenStartTimeMs = Instant.now().toEpochMilli();
    tokenExpiryTimeMs = Instant.now().plusSeconds(lifetimeSeconds).toEpochMilli();
    token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        tokenExpiryTimeMs,
        "random",
        tokenStartTimeMs);

    long expectedCacheExpiryTimeMs = tokenStartTimeMs +
        (long) (OauthTokenCache.CACHE_EXPIRY_THRESHOLD * (tokenExpiryTimeMs - tokenStartTimeMs));
    Assert.assertEquals(expectedCacheExpiryTimeMs,
        oAuthTokenCache.calculateTokenExpiryTime(token1));

    //cache expiry should honor cacheExpiryBufferSeconds seconds
    lifetimeSeconds = 20L;
    tokenStartTimeMs = Instant.now().toEpochMilli();
    tokenExpiryTimeMs = Instant.now().plusSeconds(lifetimeSeconds).toEpochMilli();
    token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        tokenExpiryTimeMs,
        "random",
        tokenStartTimeMs);

    expectedCacheExpiryTimeMs = tokenExpiryTimeMs - TimeUnit.MILLISECONDS.convert(cacheExpiryBufferSeconds, TimeUnit.SECONDS);
    Assert.assertEquals(expectedCacheExpiryTimeMs,
        oAuthTokenCache.calculateTokenExpiryTime(token1));

  }

}