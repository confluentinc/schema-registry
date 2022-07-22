package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import javax.net.ssl.SSLSocketFactory;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.secured.HttpAccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.secured.JaasOptionsUtils;
import org.apache.kafka.common.security.oauthbearer.secured.LoginAccessTokenValidator;

class CachedAccessTokenRetriever {

  //config keys of token retriever
  public static final String LOGIN_RETRY_BACKOFF_MAX_MS = "login.retry.backoff.max.ms";
  public static final String LOGIN_RETRY_BACKOFF_MS = "login.retry.backoff.ms";
  public static final String LOGIN_CONNECT_TIMEOUT_MS = "login.connect.timeout.ms";
  public static final String LOGIN_READ_TIMEOUT_MS = "login.read.backoff.ms";
  public static final String TOKEN_ENDPOINT_URL = "oauthbearer.token.endpoint.url";
  public static final String CLIENT_ID = "clientId";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String SCOPE = "scope";

  //config keys of token validator
  public static final String SCOPE_CLAIM_NAME = "scope.claim.name";
  public static final String SUB_CLAIM_NAME = "sub.claim.name";

  private AccessTokenRetriever accessTokenRetriever;
  private AccessTokenValidator accessTokenValidator;
  private OAuthTokenCache OAuthTokenCache;

  CachedAccessTokenRetriever() {
    this.OAuthTokenCache = new OAuthTokenCache();
  }

  // Expect the calling Class perform sanitation of the configs
  public void configure(Map<String, Object> configs) throws MalformedURLException {
    this.accessTokenRetriever = getTokenRetriever(configs);
    this.accessTokenValidator = getTokenValidator(configs);

  }

  public String getToken() throws IOException {
    if (OAuthTokenCache.IsTokenExpired()) {
      String token = accessTokenRetriever.retrieve();
      OAuthBearerToken oAuthBearerToken = accessTokenValidator.validate(token);
      OAuthTokenCache.setCurrentToken(oAuthBearerToken);
    }
    return OAuthTokenCache.getCurrentToken().value();
  }

  private AccessTokenRetriever getTokenRetriever(Map<String, Object> configs) {
    JaasOptionsUtils jou = new JaasOptionsUtils(configs);
    SSLSocketFactory sslSocketFactory = null;
    URL url = getValue(configs, TOKEN_ENDPOINT_URL);
    if (jou.shouldCreateSSLSocketFactory(url)) {
      sslSocketFactory = jou.createSSLSocketFactory();
    }
    String clientId = getValue(configs, CLIENT_ID);
    String clientSecret = getValue(configs, CLIENT_SECRET);
    String scope = getValue(configs, SCOPE);
    Long retryBackoffMs = getValue(configs, LOGIN_RETRY_BACKOFF_MS);
    Long retryBackoffMaxMs = getValue(configs, LOGIN_RETRY_BACKOFF_MAX_MS);
    Integer loginConnectTimeoutMs = getValue(configs, LOGIN_CONNECT_TIMEOUT_MS);
    Integer loginReadTimeoutMs = getValue(configs, LOGIN_READ_TIMEOUT_MS);

    return new HttpAccessTokenRetriever(clientId, clientSecret, scope, sslSocketFactory,
        url.toString(), retryBackoffMs, retryBackoffMaxMs, loginConnectTimeoutMs, loginReadTimeoutMs
    );
  }

  private AccessTokenValidator getTokenValidator(Map<String, Object> configs) {
    String scopeClaimName = getValue(configs, SCOPE_CLAIM_NAME);
    String subClaimName = getValue(configs, SUB_CLAIM_NAME);
    return new LoginAccessTokenValidator(scopeClaimName, subClaimName);
  }

  // Helper for type casting
  public <T> T getValue(Map<String, Object> config, String key) {
    return (T) config.get(key);
  }

}