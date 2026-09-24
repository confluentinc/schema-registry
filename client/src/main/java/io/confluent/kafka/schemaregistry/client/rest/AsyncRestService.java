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

package io.confluent.kafka.schemaregistry.client.rest;

import static io.confluent.kafka.schemaregistry.client.rest.RestService.DEFAULT_REQUEST_PROPERTIES;
import static java.lang.String.format;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProviderFactory;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProviderFactory;
import io.confluent.kafka.schemaregistry.utils.ExceptionUtils;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Non-blocking REST access layer for sending requests to the schema registry, backed by
 * Apache HttpClient's async client. The async counterpart of {@link RestService}.
 *
 * <p>Unlike {@link RestService}, all configuration is supplied at construction and cannot be
 * changed afterwards, so the underlying client (and its I/O reactor threads) is built and
 * started once and only torn down by {@link #close()}.
 *
 * <p>Response parsing, retries and URL failover run on the supplied {@link Executor}, never on
 * the HTTP client's I/O threads. The first attempt of each request resolves auth headers on the
 * calling thread, which may block if the configured bearer token provider needs to fetch a token.
 */
public class AsyncRestService implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(AsyncRestService.class);

  private static final TypeReference<RegisterSchemaResponse> REGISTER_RESPONSE_TYPE =
      new TypeReference<RegisterSchemaResponse>() {
      };
  private static final TypeReference<Config> GET_CONFIG_RESPONSE_TYPE =
      new TypeReference<Config>() {
      };
  private static final TypeReference<SchemaString> GET_SCHEMA_BY_ID_RESPONSE_TYPE =
      new TypeReference<SchemaString>() {
      };
  private static final TypeReference<Schema> GET_SCHEMA_RESPONSE_TYPE =
      new TypeReference<Schema>() {
      };
  private static final TypeReference<CompatibilityCheckResponse>
      COMPATIBILITY_CHECK_RESPONSE_TYPE_REFERENCE =
      new TypeReference<CompatibilityCheckResponse>() {
      };

  private static final int JSON_PARSE_ERROR_CODE = 50005;
  private static final ObjectMapper jsonDeserializer = JacksonMapper.INSTANCE;

  private static final String AUTHORIZATION_HEADER = "Authorization";
  private static final String TARGET_SR_CLUSTER = "target-sr-cluster";
  private static final String TARGET_IDENTITY_POOL_ID = "Confluent-Identity-Pool-Id";

  private final UrlList baseUrls;
  private final Map<String, String> httpHeaders;
  private final int maxRetries;
  private final RetryExecutor retryExecutor;
  private final BasicAuthCredentialProvider basicAuthCredentialProvider;
  private final BearerAuthCredentialProvider bearerAuthCredentialProvider;
  private final Executor executor;
  private final CloseableHttpAsyncClient httpClient;

  /**
   * Creates and starts the service, using the common fork-join pool to run callbacks.
   *
   * @param baseUrls          schema registry URLs to fail over between
   * @param configs           client configs, without the {@code schema.registry.} prefix
   * @param sslContext        TLS context, or null to use the JVM default
   * @param hostnameVerifier  hostname verifier, or null to use the default
   * @param httpHeaders       extra headers to send on every request, or null
   */
  public AsyncRestService(UrlList baseUrls,
                          Map<String, ?> configs,
                          SSLContext sslContext,
                          HostnameVerifier hostnameVerifier,
                          Map<String, String> httpHeaders) {
    this(baseUrls, configs, sslContext, hostnameVerifier, httpHeaders, ForkJoinPool.commonPool());
  }

  /**
   * Creates and starts the service.
   *
   * @param executor  runs response parsing, retries and failover
   */
  public AsyncRestService(UrlList baseUrls,
                          Map<String, ?> configs,
                          SSLContext sslContext,
                          HostnameVerifier hostnameVerifier,
                          Map<String, String> httpHeaders,
                          Executor executor) {
    this.baseUrls = baseUrls;
    this.httpHeaders = httpHeaders;
    this.executor = executor;

    this.maxRetries = SchemaRegistryClientConfig.getMaxRetries(configs);
    this.retryExecutor = new RetryExecutor(
        maxRetries,
        SchemaRegistryClientConfig.getRetriesWaitMs(configs),
        SchemaRegistryClientConfig.getRetriesMaxWaitMs(configs));

    String basicCredentialsSource = configs == null ? null
        : (String) configs.get(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE);
    String bearerCredentialsSource = configs == null ? null
        : (String) configs.get(SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE);
    if (isNonEmpty(basicCredentialsSource) && isNonEmpty(bearerCredentialsSource)) {
      throw new ConfigException(format(
          "Only one of '%s' and '%s' may be specified",
          SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
          SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE));
    }
    this.basicAuthCredentialProvider = isNonEmpty(basicCredentialsSource)
        ? BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider(
            basicCredentialsSource, configs)
        : null;
    this.bearerAuthCredentialProvider = isNonEmpty(bearerCredentialsSource)
        ? BearerAuthCredentialProviderFactory.getBearerAuthCredentialProvider(
            bearerCredentialsSource, configs)
        : null;

    if (SchemaRegistryClientConfig.getUrlRandomize(configs)) {
      baseUrls.randomizeIndex();
    }

    this.httpClient = createHttpClient(configs, sslContext, hostnameVerifier);
    // Unlike the classic client, the async client must be started before it can send requests
    this.httpClient.start();
  }

  private static CloseableHttpAsyncClient createHttpClient(Map<String, ?> configs,
                                                           SSLContext sslContext,
                                                           HostnameVerifier hostnameVerifier) {
    RequestConfig requestConfig = RequestConfig.custom()
        .setResponseTimeout(Timeout.ofMilliseconds(
            SchemaRegistryClientConfig.getHttpReadTimeoutMs(configs)))
        .build();

    PoolingAsyncClientConnectionManagerBuilder connectionManagerBuilder =
        PoolingAsyncClientConnectionManagerBuilder.create()
            .setDefaultConnectionConfig(ConnectionConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(
                    SchemaRegistryClientConfig.getHttpConnectTimeoutMs(configs)))
                .build());

    if (sslContext != null) {
      connectionManagerBuilder.setTlsStrategy(ClientTlsStrategyBuilder.create()
          .setSslContext(sslContext)
          .setHostnameVerifier(hostnameVerifier)
          .buildAsync());
    }

    HttpAsyncClientBuilder httpClientBuilder = HttpAsyncClients.custom()
        .setDefaultRequestConfig(requestConfig)
        .setConnectionManager(connectionManagerBuilder.build());

    HttpHost proxy = proxy(configs);
    if (proxy != null) {
      httpClientBuilder.setProxy(proxy);
    }
    return httpClientBuilder.build();
  }

  private static HttpHost proxy(Map<String, ?> configs) {
    if (configs == null) {
      return null;
    }
    String proxyHost = (String) configs.get(SchemaRegistryClientConfig.PROXY_HOST);
    Object proxyPortVal = configs.get(SchemaRegistryClientConfig.PROXY_PORT);
    Integer proxyPort = proxyPortVal instanceof String
        ? Integer.valueOf((String) proxyPortVal)
        : (Integer) proxyPortVal;
    if (!isNonEmpty(proxyHost) || proxyPort == null || proxyPort <= 0) {
      return null;
    }
    try {
      URI uri = new URI(proxyHost);
      String scheme = uri.getScheme() != null ? uri.getScheme() : "http";
      String host = uri.getHost() != null ? uri.getHost() : proxyHost;
      return new HttpHost(scheme, host, proxyPort);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid proxy host: " + proxyHost);
    }
  }

  public UrlList getBaseUrls() {
    return baseUrls;
  }

  public CompletableFuture<SchemaString> getId(int id, String subject) {
    UriBuilder builder = UriBuilder.fromPath("/schemas/ids/{id}")
        .queryParam("fetchMaxId", false);
    if (subject != null) {
      builder.queryParam("subject", subject);
    }
    String path = builder.build(id).toString();

    return httpRequest(path, "GET", null, GET_SCHEMA_BY_ID_RESPONSE_TYPE);
  }

  public CompletableFuture<SchemaString> getByGuid(String guid, String format) {
    UriBuilder builder = UriBuilder.fromPath("/schemas/guids/{guid}");
    if (format != null) {
      builder.queryParam("format", format);
    }
    String path = builder.build(guid).toString();

    return httpRequest(path, "GET", null, GET_SCHEMA_BY_ID_RESPONSE_TYPE);
  }

  public CompletableFuture<Schema> getVersion(String subject, int version,
                                              boolean lookupDeletedSchema) {
    String path = UriBuilder.fromPath("/subjects/{subject}/versions/{version}")
        .queryParam("deleted", lookupDeletedSchema)
        .build(subject, version).toString();

    return httpRequest(path, "GET", null, GET_SCHEMA_RESPONSE_TYPE);
  }

  public CompletableFuture<Schema> getLatestVersion(String subject) {
    String path = UriBuilder.fromPath("/subjects/{subject}/versions/latest")
        .build(subject).toString();

    return httpRequest(path, "GET", null, GET_SCHEMA_RESPONSE_TYPE);
  }

  public CompletableFuture<Schema> getLatestWithMetadata(String subject,
                                                         Map<String, String> metadata,
                                                         boolean lookupDeletedSchema) {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/metadata");
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      builder.queryParam("key", entry.getKey());
      builder.queryParam("value", entry.getValue());
    }
    builder.queryParam("deleted", lookupDeletedSchema);
    String path = builder.build(subject).toString();

    return httpRequest(path, "GET", null, GET_SCHEMA_RESPONSE_TYPE);
  }

  public CompletableFuture<RegisterSchemaResponse> registerSchema(
      RegisterSchemaRequest registerSchemaRequest, String subject, boolean normalize) {
    String path = UriBuilder.fromPath("/subjects/{subject}/versions")
        .queryParam("normalize", normalize)
        .build(subject).toString();

    return postJson(path, registerSchemaRequest, REGISTER_RESPONSE_TYPE);
  }

  public CompletableFuture<Schema> lookUpSubjectVersion(
      RegisterSchemaRequest registerSchemaRequest, String subject, boolean normalize,
      boolean lookupDeletedSchema) {
    String path = UriBuilder.fromPath("/subjects/{subject}")
        .queryParam("normalize", normalize)
        .queryParam("deleted", lookupDeletedSchema)
        .build(subject).toString();

    return postJson(path, registerSchemaRequest, GET_SCHEMA_RESPONSE_TYPE);
  }

  /**
   * Returns an empty list if the schema is compatible, otherwise the reasons it is not.
   */
  public CompletableFuture<List<String>> testCompatibility(
      RegisterSchemaRequest registerSchemaRequest, String subject, String version,
      boolean normalize, boolean verbose) {
    String path;
    if (version != null) {
      path = UriBuilder.fromPath("/compatibility/subjects/{subject}/versions/{version}")
          .queryParam("normalize", normalize)
          .queryParam("verbose", verbose)
          .build(subject, version).toString();
    } else {
      path = UriBuilder.fromPath("/compatibility/subjects/{subject}/versions/")
          .queryParam("normalize", normalize)
          .queryParam("verbose", verbose)
          .build(subject).toString();
    }

    return postJson(path, registerSchemaRequest, COMPATIBILITY_CHECK_RESPONSE_TYPE_REFERENCE)
        .thenApply(response -> {
          if (response.getIsCompatible()) {
            return Collections.<String>emptyList();
          }
          return Optional.ofNullable(response.getMessages())
              .filter(it -> !it.isEmpty())
              .orElseGet(() -> Collections.singletonList("Schemas are incompatible"));
        });
  }

  public CompletableFuture<Config> getConfig(String subject, boolean defaultToGlobal) {
    String path = subject != null
        ? UriBuilder.fromPath("/config/{subject}")
        .queryParam("defaultToGlobal", defaultToGlobal).build(subject).toString()
        : UriBuilder.fromPath("/config")
        .queryParam("defaultToGlobal", defaultToGlobal).build().toString();

    return httpRequest(path, "GET", null, GET_CONFIG_RESPONSE_TYPE);
  }

  private <T> CompletableFuture<T> postJson(String path,
                                            RegisterSchemaRequest request,
                                            TypeReference<T> responseFormat) {
    byte[] requestBodyData;
    try {
      requestBodyData = request.toJson().getBytes(StandardCharsets.UTF_8);
    } catch (IOException e) {
      return CompletableFuture.failedFuture(e);
    }
    return httpRequest(path, "POST", requestBodyData, responseFormat);
  }

  /**
   * Sends a request, retrying against the current URL and then failing over to the next one,
   * following the same policy as {@link RestService#httpRequest}.
   */
  private <T> CompletableFuture<T> httpRequest(String path,
                                               String method,
                                               byte[] requestBodyData,
                                               TypeReference<T> responseFormat) {
    return httpRequestWithFailover(0, path, method, requestBodyData, responseFormat);
  }

  private <T> CompletableFuture<T> httpRequestWithFailover(int urlAttempt,
                                                           String path,
                                                           String method,
                                                           byte[] requestBodyData,
                                                           TypeReference<T> responseFormat) {
    String baseUrl = baseUrls.current();
    String requestUrl = RestService.buildRequestUrl(baseUrl, path);
    return sendWithRetries(0,
        () -> sendHttpRequest(requestUrl, method, requestBodyData, responseFormat))
        .<CompletableFuture<T>>handleAsync((result, error) -> {
          if (error == null) {
            return CompletableFuture.completedFuture(result);
          }
          Throwable cause = unwrap(error);
          if (isNonRetriableException(cause) || urlAttempt >= baseUrls.size() - 1) {
            return CompletableFuture.<T>failedFuture(cause);
          }
          log.warn("Request to URL {} failed with error: {}. "
                  + "Failing over to next URL if available...",
              requestUrl, cause.toString());
          baseUrls.fail(baseUrl);
          return httpRequestWithFailover(
              urlAttempt + 1, path, method, requestBodyData, responseFormat);
        }, executor)
        .thenCompose(Function.identity());
  }

  /**
   * Async equivalent of {@link RetryExecutor#retry}: same retriability rules and backoff, but
   * waits between attempts by scheduling rather than sleeping.
   */
  private <T> CompletableFuture<T> sendWithRetries(int attempt,
                                                   Supplier<CompletableFuture<T>> send) {
    return send.get()
        .<CompletableFuture<T>>handleAsync((result, error) -> {
          if (error == null) {
            return CompletableFuture.completedFuture(result);
          }
          Throwable cause = unwrap(error);
          if (!isRetriableAttempt(cause) || attempt >= maxRetries) {
            return CompletableFuture.<T>failedFuture(cause);
          }
          long delayMs = retryExecutor.computeDelayBeforeNextRetry(attempt).toMillis();
          log.debug("Retriable error on attempt {}/{}: {}. Retrying in {} ms...",
              attempt + 1, maxRetries + 1, cause.toString(), delayMs);
          Executor delayed = CompletableFuture.delayedExecutor(
              delayMs, TimeUnit.MILLISECONDS, executor);
          return CompletableFuture.runAsync(() -> { }, delayed)
              .thenCompose(ignored -> sendWithRetries(attempt + 1, send));
        }, executor)
        .thenCompose(Function.identity());
  }

  private <T> CompletableFuture<T> sendHttpRequest(String requestUrl,
                                                   String method,
                                                   byte[] requestBodyData,
                                                   TypeReference<T> responseFormat) {
    CompletableFuture<SimpleHttpResponse> responseFuture = new CompletableFuture<>();
    try {
      SimpleRequestBuilder builder = SimpleRequestBuilder.create(method).setUri(requestUrl);

      Map<String, String> headers = getAuthHeaders(new URL(requestUrl));
      headers.putAll(DEFAULT_REQUEST_PROPERTIES);
      if (httpHeaders != null) {
        headers.putAll(httpHeaders);
      }
      headers.forEach(builder::setHeader);

      if (requestBodyData != null) {
        // Content type comes from the headers set above, as in RestService
        builder.setBody(requestBodyData, null);
      }

      log.debug("Sending {} with input {} to {} using Apache Http Async Client",
          method,
          requestBodyData == null ? "null" : new String(requestBodyData, StandardCharsets.UTF_8),
          requestUrl);

      httpClient.execute(builder.build(), new FutureCallback<SimpleHttpResponse>() {
        @Override
        public void completed(SimpleHttpResponse response) {
          responseFuture.complete(response);
        }

        @Override
        public void failed(Exception e) {
          log.error("Failed to send HTTP request to endpoint: {}", requestUrl, e);
          responseFuture.completeExceptionally(e);
        }

        @Override
        public void cancelled() {
          responseFuture.cancel(false);
        }
      });
    } catch (IOException | RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
    return responseFuture.thenApplyAsync(
        response -> readResponse(response, responseFormat), executor);
  }

  private static <T> T readResponse(SimpleHttpResponse response,
                                    TypeReference<T> responseFormat) {
    int responseCode = response.getCode();
    // JSON is UTF-8; getBodyText() falls back to ISO-8859-1 when the response has no charset
    byte[] responseBytes = response.getBodyBytes();
    String responseBody = responseBytes == null
        ? null
        : new String(responseBytes, StandardCharsets.UTF_8);
    try {
      if (responseCode == 200 || responseCode == 207) {
        return jsonDeserializer.readValue(responseBody, responseFormat);
      } else if (responseCode == 204) {
        return null;
      }
      ErrorMessage errorMessage;
      if (responseBody != null && !responseBody.isEmpty()) {
        try {
          errorMessage = jsonDeserializer.readValue(responseBody, ErrorMessage.class);
        } catch (JsonProcessingException e) {
          errorMessage = new ErrorMessage(JSON_PARSE_ERROR_CODE, format(
              "Unable to parse error message from schema registry: '(%s)'",
              responseBody));
        }
      } else {
        errorMessage = new ErrorMessage(JSON_PARSE_ERROR_CODE, "Error");
      }
      throw new RestClientException(errorMessage.getMessage(), responseCode,
          errorMessage.getErrorCode());
    } catch (IOException | RestClientException e) {
      // Checked exceptions can't escape a lambda; unwrap() strips this again
      throw new CompletionException(e);
    }
  }

  /**
   * Whether a failed attempt should be retried against the same URL, matching
   * {@link RetryExecutor#retry}.
   */
  private boolean isRetriableAttempt(Throwable e) {
    if (e instanceof RestClientException) {
      return retryExecutor.isRetriable((RestClientException) e);
    }
    return e instanceof IOException;
  }

  /**
   * Whether a failed request should not be retried against the next base URL, matching
   * {@link RestService}'s failover.
   */
  private boolean isNonRetriableException(Throwable e) {
    if (e instanceof RestClientException) {
      return !retryExecutor.isRetriable((RestClientException) e);
    }
    return !ExceptionUtils.isNetworkConnectionException(e);
  }

  private static Throwable unwrap(Throwable e) {
    while ((e instanceof CompletionException || e instanceof ExecutionException)
        && e.getCause() != null) {
      e = e.getCause();
    }
    return e;
  }

  private Map<String, String> getAuthHeaders(URL url) {
    Map<String, String> headers = new HashMap<>();

    if (basicAuthCredentialProvider != null) {
      String userInfo = basicAuthCredentialProvider.getUserInfo(url);
      if (userInfo != null) {
        String authHeader = Base64.getEncoder().encodeToString(
            userInfo.getBytes(StandardCharsets.UTF_8));
        headers.put(AUTHORIZATION_HEADER, "Basic " + authHeader);
      }
    }

    if (bearerAuthCredentialProvider != null) {
      String bearerToken = bearerAuthCredentialProvider.getBearerToken(url);
      if (bearerToken != null) {
        headers.put(AUTHORIZATION_HEADER, "Bearer " + bearerToken);
      }

      String targetIdentityPoolId = bearerAuthCredentialProvider.getTargetIdentityPoolId();
      if (targetIdentityPoolId != null) {
        headers.put(TARGET_IDENTITY_POOL_ID, targetIdentityPoolId);
      }

      String targetSchemaRegistry = bearerAuthCredentialProvider.getTargetSchemaRegistry();
      if (targetSchemaRegistry != null) {
        headers.put(TARGET_SR_CLUSTER, targetSchemaRegistry);
      }
    }

    return headers;
  }

  private static boolean isNonEmpty(String s) {
    return s != null && !s.isEmpty();
  }

  /**
   * Shuts down the I/O reactor threads. Requests still in flight are cancelled.
   */
  @Override
  public void close() throws IOException {
    try {
      if (bearerAuthCredentialProvider != null) {
        bearerAuthCredentialProvider.close();
      }
    } finally {
      httpClient.close();
    }
  }
}
