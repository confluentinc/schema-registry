package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth;

import java.util.Collections;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.secured.BasicOAuthBearerToken;
import org.easymock.TestSubject;
import org.junit.Assert;
import org.junit.Test;

public class OAuthTokenCacheTest {

  @TestSubject
  private OAuthTokenCache oAuthTokenCache = new OAuthTokenCache();

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
    Assert.assertEquals(true, oAuthTokenCache.IsTokenExpired());
  }

  @Test
  public void TestIsExpiredWithValidToken() throws InterruptedException {
    OAuthBearerToken token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        100L,
        "random",
        0L);
    oAuthTokenCache.setCurrentToken(token1);
    Assert.assertEquals(false, oAuthTokenCache.IsTokenExpired());
    Thread.sleep(10);
    Assert.assertEquals(false, oAuthTokenCache.IsTokenExpired());
  }

  @Test
  public void TestIsExpiredWithExpiredToken() throws InterruptedException {
    OAuthBearerToken token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        100L,
        "random",
        0L);
    oAuthTokenCache.setCurrentToken(token1);
    //sleeping till token get expired
    Thread.sleep((long) Math.floor(100 * OAuthTokenCache.CACHE_PERCENTAGE_THRESHOLD));
    Assert.assertEquals(true, oAuthTokenCache.IsTokenExpired());
  }

}