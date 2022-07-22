package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth;

import java.time.Instant;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

public class OAuthTokenCache {

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

  public boolean IsTokenExpired() {
    return currentToken == null || Instant.now().toEpochMilli() >= expiresAtMs;
  }

}