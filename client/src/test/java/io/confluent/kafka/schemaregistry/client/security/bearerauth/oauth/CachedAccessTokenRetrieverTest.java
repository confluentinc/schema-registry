package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import java.io.IOException;
import java.util.Collections;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.secured.BasicOAuthBearerToken;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class CachedAccessTokenRetrieverTest {

  @Mock
  AccessTokenRetriever accessTokenRetriever;

  @Mock
  AccessTokenValidator accessTokenValidator;

  @TestSubject
  CachedAccessTokenRetriever cachedAccessTokenRetriever = new CachedAccessTokenRetriever();

  private String tokenString1 = "token1";
  private String tokenString2 = "token2";
  private OAuthBearerToken token1;
  private OAuthBearerToken token2;

  @Test
  public void TestGetTokenWithValidCache() throws IOException {

    //Token1 has validity of 100 ms
    token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        100L,
        "random",
        0L);

    token2 = new BasicOAuthBearerToken(tokenString2,
        Collections.emptySet(),
        100L,
        "random",
        0L);

    expect(accessTokenRetriever.retrieve()).andReturn(tokenString1).once();
    expect(accessTokenValidator.validate(tokenString1)).andReturn(token1).once();
    expect(accessTokenRetriever.retrieve()).andReturn(tokenString2).once();
    expect(accessTokenValidator.validate(tokenString1)).andReturn(token2).once();
    replay(accessTokenRetriever, accessTokenValidator);

    Assert.assertEquals(tokenString1, cachedAccessTokenRetriever.getToken());

    //Expects second call to retrieve token to get the cached token1 instead of
    // making a second network call and getting token2
    Assert.assertEquals(tokenString1, cachedAccessTokenRetriever.getToken());
  }

  @Test
  public void TestGetTokenWithExpiredCache() throws IOException {
    //Token1 has validity of 0 ms
    token1 = new BasicOAuthBearerToken(tokenString1,
        Collections.emptySet(),
        0,
        "random",
        0L);
    token2 = new BasicOAuthBearerToken(tokenString2,
        Collections.emptySet(),
        0,
        "random",
        0L);

    expect(accessTokenRetriever.retrieve()).andReturn(tokenString1).once();
    expect(accessTokenValidator.validate(tokenString1)).andReturn(token1).once();
    expect(accessTokenRetriever.retrieve()).andReturn(tokenString2).once();
    expect(accessTokenValidator.validate(tokenString2)).andReturn(token2).once();
    replay(accessTokenRetriever, accessTokenValidator);

    Assert.assertEquals(tokenString1, cachedAccessTokenRetriever.getToken());

    // since token has lifespan of 0s second, token cache get expired as soon it is received
    // and it will retrieve a new token , i.e token2
    Assert.assertEquals(tokenString2, cachedAccessTokenRetriever.getToken());
  }

}