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

import java.util.Collections;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.secured.BasicOAuthBearerToken;
import org.junit.Assert;
import org.junit.Test;

public class OauthTokenCacheTest {

  private OauthTokenCache oAuthTokenCache = new OauthTokenCache();

  private String tokenString1 = "token1";
  OAuthBearerToken token1 = new BasicOAuthBearerToken(tokenString1,
      Collections.emptySet(),
      100L,
      "random",
      0L);

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
  public void TestIsExpiredWithValidToken() throws InterruptedException {
    OAuthBearerToken token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        100L,
        "random",
        0L);
    oAuthTokenCache.setCurrentToken(token1);
    Assert.assertEquals(false, oAuthTokenCache.isTokenExpired());
    Thread.sleep(10);
    Assert.assertEquals(false, oAuthTokenCache.isTokenExpired());
  }

  @Test
  public void TestIsExpiredWithExpiredToken() throws InterruptedException {
    OAuthBearerToken token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        95L,
        "random",
        0L);
    oAuthTokenCache.setCurrentToken(token1);
    //sleeping till token get expired
    Thread.sleep((long) Math.floor(100 * OauthTokenCache.CACHE_EXPIRY_THRESHOLD));
    Assert.assertEquals(true, oAuthTokenCache.isTokenExpired());
  }

}