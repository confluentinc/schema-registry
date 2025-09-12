/*
 * Copyright 2025 Confluent Inc.
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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.secured.UnretryableException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.Objects;

public class TokenAdapter {
  private static Logger log = LoggerFactory.getLogger(TokenAdapter.class);

  public interface TokenValidator {
    OAuthBearerToken validate(String accessToken) throws ValidateException;
  }

  public static class TokenValidatorImpl implements TokenValidator {
    private static String[] options = {
      "org.apache.kafka.common.security.oauthbearer.internals.secured.LoginAccessTokenValidator",
      "org.apache.kafka.common.security.oauthbearer.internals.secured.ClientJwtValidator",
    };

    private Object delegate = null;
    private static Class<?> delegateClass = null;

    static {
      for (String option : options) {
        try {
          delegateClass = Class.forName(option);
        } catch (ClassNotFoundException e) {
          log.debug("Unable to load token validator class", e);
        }
      }
    }

    public TokenValidatorImpl(String scopeClaimName,
                              String subClaimName) {
      Objects.requireNonNull(delegateClass);
      try {
        Constructor<?> cons = delegateClass.getConstructor(String.class, String.class);
        Object[] args = new Object[]{scopeClaimName, subClaimName};
        delegate = cons.newInstance(args);
      } catch (Exception e) {
        log.debug("failed to create token validator instance", e);
      }
      Objects.requireNonNull(delegate);
    }

    @Override
    public OAuthBearerToken validate(String accessToken) throws ValidateException {
      try {
        Method m = delegate.getClass().getMethod("validate", String.class);
        return (OAuthBearerToken) m.invoke(delegate, accessToken);
      } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
        log.debug("Could not call retrieve", e);
      }
      throw new ValidateException("Could not call validate method.");
    }
  }

  public interface TokenRetriever {
    String retrieve() throws IOException;
  }

  public static class TokenRetrieverImpl implements TokenRetriever {
    private static String[] options = {
      "org.apache.kafka.common.security.oauthbearer.internals.secured.HttpAccessTokenRetriever",
      "org.apache.kafka.common.security.oauthbearer.internals.secured.HttpJwtRetriever"
    };

    private Object delegate = null;
    private static Class<?> delegateClass = null;

    static {
      for (String option : options) {
        try {
          delegateClass = Class.forName(option);
        } catch (ClassNotFoundException e) {
          log.debug("Unable to load token retriever class", e);
        }
      }
    }

    public TokenRetrieverImpl(String clientId,
                              String clientSecret,
                              String scope,
                              SSLSocketFactory sslSocketFactory,
                              String tokenEndpointUrl,
                              long loginRetryBackoffMs,
                              long loginRetryBackoffMaxMs,
                              Integer loginConnectTimeoutMs,
                              Integer loginReadTimeoutMs,
                              boolean urlencodeHeader) {
      Objects.requireNonNull(delegateClass);
      try {
        Constructor<?> cons = delegateClass.getConstructor(String.class,
                                                            String.class,
                                                            String.class,
                                                            SSLSocketFactory.class,
                                                            String.class,
                                                            long.class,
                                                            long.class,
                                                            Integer.class,
                                                            Integer.class,
                                                            boolean.class);
        Object[] args = new Object[]{clientId, clientSecret, scope,
          sslSocketFactory, tokenEndpointUrl, loginRetryBackoffMaxMs,
          loginRetryBackoffMs, loginConnectTimeoutMs, loginReadTimeoutMs,
          urlencodeHeader};
        delegate = cons.newInstance(args);
      } catch (Exception e) {
        log.debug("failed to create http token retriever instance", e);
      }
      Objects.requireNonNull(delegate);
    }

    @Override
    public String retrieve() throws IOException {
      try {
        Method m = delegate.getClass().getMethod("retrieve");
        return (String) m.invoke(delegate);
      } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
        log.debug("Could not call retrieve", e);
      }
      throw new IOException("Could not call retrieve method.");
    }

    public static String post(HttpURLConnection con,
                              Map<String, String> headers,
                              String requestBody,
                              Integer connectTimeoutMs,
                              Integer readTimeoutMs) throws IOException, UnretryableException {
      try {
        Method m = delegateClass.getDeclaredMethod("post",
                                                    HttpURLConnection.class,
                                                    Map.class,
                                                    String.class,
                                                    Integer.class,
                                                    Integer.class);
        Object[] args = new Object[]{con, headers, requestBody, connectTimeoutMs, readTimeoutMs};
        return (String) m.invoke(null, args);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e1) {
        log.error("Could not call post method.", e1);
        throw new IOException("Could not call post method.", e1);
      }
    }
  }
}
