/*
 * Copyright 2026 Confluent Inc.
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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.oauthbearer.JwtRetriever;
import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.Retry;
import org.apache.kafka.common.security.oauthbearer.internals.secured.UnretryableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * <code>UamiJwtRetriever</code> is a {@link JwtRetriever} that retrieves a JWT access token from
 * the Azure Instance Metadata Service (IMDS) using a User Assigned Managed Identity (UAMI).
 *
 * <p>It issues a GET request to the configured IMDS endpoint with a user-supplied query string
 * appended, and retries on transient failures using exponential backoff.
 *
 * <p>The default endpoint is the standard Azure IMDS address
 * ({@link #DEFAULT_IMDS_ENDPOINT}), but can be overridden for Azure Arc or testing environments.
 */
public class UamiJwtRetriever implements JwtRetriever {

  private static final Logger log = LoggerFactory.getLogger(UamiJwtRetriever.class);

  public static final String METADATA_HEADER = "Metadata";
  public static final String DEFAULT_IMDS_ENDPOINT =
      "http://169.254.169.254/metadata/identity/oauth2/token";

  private final String query;
  private final String endpointUrl;
  private final long retryBackoffMs;
  private final long retryBackoffMaxMs;
  private final Integer connectTimeoutMs;
  private final Integer readTimeoutMs;

  public UamiJwtRetriever(
      String query,
      String endpointUrl,
      long retryBackoffMs,
      long retryBackoffMaxMs,
      Integer connectTimeoutMs,
      Integer readTimeoutMs) {
    this.query = Objects.requireNonNull(query, "query must be non-null");
    this.endpointUrl = Objects.requireNonNull(endpointUrl, "endpointUrl must be non-null");
    this.retryBackoffMs = retryBackoffMs;
    this.retryBackoffMaxMs = retryBackoffMaxMs;
    this.connectTimeoutMs = connectTimeoutMs;
    this.readTimeoutMs = readTimeoutMs;
  }

  /**
   * Issues a GET request to the IMDS endpoint and returns the raw JWT access token string.
   * Retries on transient (non-4xx) HTTP errors using exponential backoff.
   *
   * @return Non-null JWT access token string
   * @throws JwtRetrieverException on IO errors or a non-retryable HTTP response
   */
  @Override
  public String retrieve() throws JwtRetrieverException {
    String requestUrl = buildRequestUrl();
    Retry<String> retry = new Retry<>(retryBackoffMs, retryBackoffMaxMs);

    String responseBody;
    try {
      responseBody = retry.execute(() -> {
        HttpURLConnection con = null;
        try {
          con = (HttpURLConnection) new URL(requestUrl).openConnection();
          return get(con, connectTimeoutMs, readTimeoutMs);
        } catch (IOException e) {
          throw new ExecutionException(e);
        } finally {
          if (con != null) {
            con.disconnect();
          }
        }
      });
      return HttpJwtRetriever.parseAccessToken(responseBody);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw new JwtRetrieverException(e.getCause());
      } else {
        throw new KafkaException(e.getCause());
      }
    } catch (IOException e) {
      throw new JwtRetrieverException(e);
    }
  }

  // package-private for testing
  static String get(HttpURLConnection con, Integer connectTimeoutMs, Integer readTimeoutMs)
      throws IOException, UnretryableException {
    log.debug("get - starting IMDS request for {}", con.getURL());
    con.setRequestMethod("GET");
    con.setRequestProperty("Accept", "application/json");
    con.setRequestProperty(METADATA_HEADER, "true");
    con.setRequestProperty("Cache-Control", "no-cache");
    con.setUseCaches(false);

    if (connectTimeoutMs != null) {
      con.setConnectTimeout(connectTimeoutMs);
    }
    if (readTimeoutMs != null) {
      con.setReadTimeout(readTimeoutMs);
    }

    log.debug("get - connecting to {}", con.getURL());
    con.connect();
    return HttpJwtRetriever.handleOutput(con);
  }

  // package-private for testing
  String buildRequestUrl() {
    return endpointUrl + "?" + query;
  }
}
