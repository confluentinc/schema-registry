/*
 * Copyright 2018 Confluent Inc.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryServerVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ServerClusterId;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProviderFactory;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;

import java.util.Set;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.Mode;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProviderFactory;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

/**
 * Rest access layer for sending requests to the schema registry.
 */
public class RestService implements Closeable, Configurable {

  private static final Logger log = LoggerFactory.getLogger(RestService.class);
  private static final TypeReference<RegisterSchemaResponse> REGISTER_RESPONSE_TYPE =
      new TypeReference<RegisterSchemaResponse>() {
      };
  private static final TypeReference<Config> GET_CONFIG_RESPONSE_TYPE =
      new TypeReference<Config>() {
      };
  private static final TypeReference<Mode> GET_MODE_RESPONSE_TYPE =
      new TypeReference<Mode>() {
      };
  private static final TypeReference<List<Schema>> GET_SCHEMAS_RESPONSE_TYPE =
      new TypeReference<List<Schema>>() {
      };
  private static final TypeReference<List<ExtendedSchema>> GET_EXTENDED_SCHEMAS_RESPONSE_TYPE =
      new TypeReference<List<ExtendedSchema>>() {
      };
  private static final TypeReference<SchemaString> GET_SCHEMA_BY_ID_RESPONSE_TYPE =
      new TypeReference<SchemaString>() {
      };
  private static final TypeReference<List<String>> GET_SCHEMA_TYPES_TYPE =
      new TypeReference<List<String>>() {
      };
  private static final TypeReference<JsonNode> GET_SCHEMA_ONLY_BY_VERSION_RESPONSE_TYPE =
      new TypeReference<JsonNode>() {
      };
  private static final TypeReference<Schema> GET_SCHEMA_RESPONSE_TYPE =
      new TypeReference<Schema>() {
      };
  private static final TypeReference<List<Integer>> GET_REFERENCED_BY_RESPONSE_TYPE =
      new TypeReference<List<Integer>>() {
      };
  private static final TypeReference<List<Integer>> ALL_VERSIONS_RESPONSE_TYPE =
      new TypeReference<List<Integer>>() {
      };
  private static final TypeReference<List<String>> ALL_CONTEXTS_RESPONSE_TYPE =
      new TypeReference<List<String>>() {
      };
  private static final TypeReference<List<String>> ALL_TOPICS_RESPONSE_TYPE =
      new TypeReference<List<String>>() {
      };
  private static final TypeReference<List<SubjectVersion>> GET_VERSIONS_RESPONSE_TYPE =
      new TypeReference<List<SubjectVersion>>() {
      };
  private static final TypeReference<CompatibilityCheckResponse>
      COMPATIBILITY_CHECK_RESPONSE_TYPE_REFERENCE =
      new TypeReference<CompatibilityCheckResponse>() {
      };
  private static final TypeReference<Schema>
      SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE =
      new TypeReference<Schema>() {
      };
  private static final TypeReference<ConfigUpdateRequest>
      UPDATE_CONFIG_RESPONSE_TYPE_REFERENCE =
      new TypeReference<ConfigUpdateRequest>() {
      };
  private static final TypeReference<ModeUpdateRequest>
      UPDATE_MODE_RESPONSE_TYPE_REFERENCE =
      new TypeReference<ModeUpdateRequest>() {
      };
  private static final TypeReference<Integer> DELETE_SUBJECT_VERSION_RESPONSE_TYPE =
      new TypeReference<Integer>() {
      };
  private static final TypeReference<? extends List<Integer>> DELETE_SUBJECT_RESPONSE_TYPE =
      new TypeReference<List<Integer>>() {
      };
  private static final TypeReference<Mode> DELETE_SUBJECT_MODE_RESPONSE_TYPE =
      new TypeReference<Mode>() {
      };
  private static final TypeReference<Config> DELETE_SUBJECT_CONFIG_RESPONSE_TYPE =
      new TypeReference<Config>() {
      };
  private static final TypeReference<ServerClusterId> GET_CLUSTER_ID_RESPONSE_TYPE =
      new TypeReference<ServerClusterId>() {
      };
  private static final TypeReference<SchemaRegistryServerVersion> GET_SR_VERSION_RESPONSE_TYPE =
      new TypeReference<SchemaRegistryServerVersion>() {
      };


  private static final int JSON_PARSE_ERROR_CODE = 50005;
  private static ObjectMapper jsonDeserializer = JacksonMapper.INSTANCE;

  private static final String AUTHORIZATION_HEADER = "Authorization";
  private static final String TARGET_SR_CLUSTER = "target-sr-cluster";
  private static final String TARGET_IDENTITY_POOL_ID = "Confluent-Identity-Pool-Id";
  public static final String X_FORWARD_HEADER = "X-Forward";

  public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;

  static {
    DEFAULT_REQUEST_PROPERTIES =
        Collections.singletonMap("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
  }

  private UrlList baseUrls;
  private SSLSocketFactory sslSocketFactory;
  private int httpConnectTimeoutMs;
  private int httpReadTimeoutMs;
  private HostnameVerifier hostnameVerifier;
  private BasicAuthCredentialProvider basicAuthCredentialProvider;
  private BearerAuthCredentialProvider bearerAuthCredentialProvider;
  private Map<String, String> httpHeaders;
  private Proxy proxy;
  private boolean isForward;

  public RestService(UrlList baseUrls) {
    this(baseUrls, false);
  }

  public RestService(List<String> baseUrls) {
    this(new UrlList(baseUrls));
  }

  public RestService(String baseUrlConfig) {
    this(parseBaseUrl(baseUrlConfig));
  }

  public RestService(String baseUrlConfig, boolean isForward) {
    this(new UrlList(parseBaseUrl(baseUrlConfig)), isForward);
  }

  public RestService(UrlList baseUrls, boolean isForward) {
    this.baseUrls = baseUrls;
    this.isForward = isForward;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    setHttpConnectTimeoutMs(SchemaRegistryClientConfig.getHttpConnectTimeoutMs(configs));
    setHttpReadTimeoutMs(SchemaRegistryClientConfig.getHttpReadTimeoutMs(configs));

    String basicCredentialsSource =
        (String) configs.get(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE);
    String bearerCredentialsSource =
        (String) configs.get(SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE);

    if (isNonEmpty(basicCredentialsSource) && isNonEmpty(bearerCredentialsSource)) {
      throw new ConfigException(String.format(
          "Only one of '%s' and '%s' may be specified",
          SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
          SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE
      ));

    } else if (isNonEmpty(basicCredentialsSource)) {
      BasicAuthCredentialProvider basicAuthCredentialProvider =
          BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider(
              basicCredentialsSource,
              configs
          );
      setBasicAuthCredentialProvider(basicAuthCredentialProvider);

    } else if (isNonEmpty(bearerCredentialsSource)) {
      BearerAuthCredentialProvider bearerAuthCredentialProvider =
          BearerAuthCredentialProviderFactory.getBearerAuthCredentialProvider(
              bearerCredentialsSource,
              configs
          );
      setBearerAuthCredentialProvider(bearerAuthCredentialProvider);
    }

    String proxyHost = (String) configs.get(SchemaRegistryClientConfig.PROXY_HOST);
    Object proxyPortVal = configs.get(SchemaRegistryClientConfig.PROXY_PORT);
    Integer proxyPort = proxyPortVal instanceof String
                        ? Integer.valueOf((String) proxyPortVal)
                        : (Integer) proxyPortVal;

    if (isValidProxyConfig(proxyHost, proxyPort)) {
      setProxy(proxyHost, proxyPort);
    }
  }

  private static boolean isNonEmpty(String s) {
    return s != null && !s.isEmpty();
  }

  private static boolean isValidProxyConfig(String proxyHost, Integer proxyPort) {
    return isNonEmpty(proxyHost) && proxyPort != null && proxyPort > 0;
  }

  public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
    this.sslSocketFactory = sslSocketFactory;
  }

  public void setHostnameVerifier(HostnameVerifier hostnameVerifier) {
    this.hostnameVerifier = hostnameVerifier;
  }

  public void setHttpConnectTimeoutMs(int httpConnectTimeoutMs) {
    this.httpConnectTimeoutMs = httpConnectTimeoutMs;
  }

  public void setHttpReadTimeoutMs(int httpReadTimeoutMs) {
    this.httpReadTimeoutMs = httpReadTimeoutMs;
  }

  /**
   * @param requestUrl        HTTP connection will be established with this url.
   * @param method            HTTP method ("GET", "POST", "PUT", etc.)
   * @param requestBodyData   Bytes to be sent in the request body.
   * @param requestProperties HTTP header properties.
   * @param responseFormat    Expected format of the response to the HTTP request.
   * @param <T>               The type of the deserialized response to the HTTP request.
   * @return The deserialized response to the HTTP request, or null if no data is expected.
   */
  private <T> T sendHttpRequest(String requestUrl, String method, byte[] requestBodyData,
                                Map<String, String> requestProperties,
                                TypeReference<T> responseFormat)
      throws IOException, RestClientException {
    String requestData = requestBodyData == null
                         ? "null"
                         : new String(requestBodyData, StandardCharsets.UTF_8);
    log.debug(String.format("Sending %s with input %s to %s",
                            method, requestData,
                            requestUrl));

    HttpURLConnection connection = null;
    try {
      URL url = url(requestUrl);

      connection = buildConnection(url, method, requestProperties);

      if (requestBodyData != null) {
        connection.setDoOutput(true);
        try (OutputStream os = connection.getOutputStream()) {
          os.write(requestBodyData);
          os.flush();
        } catch (IOException e) {
          log.error("Failed to send HTTP request to endpoint: {}", url, e);
          throw e;
        }
      }

      int responseCode = connection.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        InputStream is = connection.getInputStream();
        T result = jsonDeserializer.readValue(is, responseFormat);
        is.close();
        return result;
      } else if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
        return null;
      } else {
        ErrorMessage errorMessage;
        try (InputStream es = connection.getErrorStream()) {
          if (es != null) {
            String errorString = CharStreams.toString(new InputStreamReader(es, Charsets.UTF_8));
            try {
              errorMessage = jsonDeserializer.readValue(errorString, ErrorMessage.class);
            } catch (JsonProcessingException e) {
              errorMessage = new ErrorMessage(JSON_PARSE_ERROR_CODE, errorString);
            }
          } else {
            errorMessage = new ErrorMessage(JSON_PARSE_ERROR_CODE, "Error");
          }
        }
        throw new RestClientException(errorMessage.getMessage(), responseCode,
                                      errorMessage.getErrorCode());
      }

    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private HttpURLConnection buildConnection(URL url, String method, Map<String,
                                            String> requestProperties)
      throws IOException {
    HttpURLConnection connection = null;
    if (proxy == null) {
      connection = (HttpURLConnection) url.openConnection();
    } else {
      connection = (HttpURLConnection) url.openConnection(proxy);
    }

    connection.setConnectTimeout(this.httpConnectTimeoutMs);
    connection.setReadTimeout(this.httpReadTimeoutMs);

    setupSsl(connection);
    connection.setRequestMethod(method);
    setAuthRequestHeaders(connection);
    setCustomHeaders(connection);
    // connection.getResponseCode() implicitly calls getInputStream, so always set to true.
    // On the other hand, leaving this out breaks nothing.
    connection.setDoInput(true);

    for (Map.Entry<String, String> entry : requestProperties.entrySet()) {
      connection.setRequestProperty(entry.getKey(), entry.getValue());
    }

    connection.setUseCaches(false);

    return connection;
  }

  private void setupSsl(HttpURLConnection connection) {
    if (connection instanceof HttpsURLConnection && sslSocketFactory != null) {
      ((HttpsURLConnection) connection).setSSLSocketFactory(sslSocketFactory);
      if (hostnameVerifier != null) {
        ((HttpsURLConnection) connection).setHostnameVerifier(hostnameVerifier);
      }
    }
  }

  /**
   * Send an HTTP request.
   *
   * @param path              The relative path
   * @param method            HTTP method ("GET", "POST", "PUT", etc.)
   * @param requestBodyData   Bytes to be sent in the request body.
   * @param requestProperties HTTP header properties.
   * @param responseFormat    Expected format of the response to the HTTP request.
   * @param <T>               The type of the deserialized response to the HTTP request.
   * @return The deserialized response to the HTTP request, or null if no data is expected.
   */
  public <T> T httpRequest(String path,
                           String method,
                           byte[] requestBodyData,
                           Map<String, String> requestProperties,
                           TypeReference<T> responseFormat)
      throws IOException, RestClientException {
    if (isForward) {
      requestProperties.put(X_FORWARD_HEADER, "true");
    }
    for (int i = 0, n = baseUrls.size(); i < n; i++) {
      String baseUrl = baseUrls.current();
      String requestUrl = buildRequestUrl(baseUrl, path);
      try {
        return sendHttpRequest(requestUrl,
                               method,
                               requestBodyData,
                               requestProperties,
                               responseFormat);
      } catch (IOException e) {
        baseUrls.fail(baseUrl);
        if (i == n - 1) {
          throw e; // Raise the exception since we have no more urls to try
        }
      }
    }
    throw new IOException("Internal HTTP retry error"); // Can't get here
  }

  // Visible for testing
  static String buildRequestUrl(String baseUrl, String path) {
    // Join base URL and path, collapsing any duplicate forward slash delimiters
    return baseUrl.replaceFirst("/$", "") + "/" + path.replaceFirst("^/", "");
  }

  // Visible for testing
  public Schema lookUpSubjectVersion(String schemaString, String subject)
      throws IOException, RestClientException {
    return lookUpSubjectVersion(schemaString, subject, false);
  }

  // Visible for testing
  public Schema lookUpSubjectVersion(String schemaString,
                                     String subject,
                                     boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    return lookUpSubjectVersion(schemaString, subject, false, lookupDeletedSchema);
  }

  public Schema lookUpSubjectVersion(String schemaString,
                                     String subject,
                                     boolean normalize,
                                     boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    return lookUpSubjectVersion(
        DEFAULT_REQUEST_PROPERTIES, request, subject, normalize, lookupDeletedSchema);
  }

  public Schema lookUpSubjectVersion(String schemaString,
                                     String schemaType,
                                     List<SchemaReference> references,
                                     String subject,
                                     boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    return lookUpSubjectVersion(
        schemaString, schemaType, references, subject, false, lookupDeletedSchema);
  }

  public Schema lookUpSubjectVersion(String schemaString,
                                     String schemaType,
                                     List<SchemaReference> references,
                                     String subject,
                                     boolean normalize,
                                     boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    request.setSchemaType(schemaType);
    request.setReferences(references);
    return lookUpSubjectVersion(
        DEFAULT_REQUEST_PROPERTIES, request, subject, normalize, lookupDeletedSchema);
  }

  public Schema lookUpSubjectVersion(RegisterSchemaRequest registerSchemaRequest,
                                     String subject,
                                     boolean normalize,
                                     boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    return lookUpSubjectVersion(
        DEFAULT_REQUEST_PROPERTIES, registerSchemaRequest, subject, normalize, lookupDeletedSchema);
  }

  public Schema lookUpSubjectVersion(Map<String, String> requestProperties,
                                     RegisterSchemaRequest registerSchemaRequest,
                                     String subject,
                                     boolean normalize,
                                     boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}")
        .queryParam("normalize", normalize)
        .queryParam("deleted", lookupDeletedSchema);
    String path = builder.build(subject).toString();

    Schema schema = httpRequest(path, "POST",
                                registerSchemaRequest.toJson().getBytes(StandardCharsets.UTF_8),
                                requestProperties, SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE);
    return schema;
  }

  // Visible for testing
  public int registerSchema(String schemaString, String subject)
      throws IOException, RestClientException {
    return registerSchema(schemaString, subject, false).getId();
  }

  public RegisterSchemaResponse registerSchema(String schemaString, String subject,
                                               boolean normalize)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    return registerSchema(request, subject, normalize);
  }

  public RegisterSchemaResponse registerSchema(String schemaString, String schemaType,
                                               List<SchemaReference> references, String subject)
      throws IOException, RestClientException {
    return registerSchema(schemaString, schemaType, references, subject, false);
  }

  public RegisterSchemaResponse registerSchema(String schemaString, String schemaType,
                                               List<SchemaReference> references,
                                               String subject, boolean normalize)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    request.setSchemaType(schemaType);
    request.setReferences(references);
    return registerSchema(request, subject, normalize);
  }

  // Visible for testing
  public int registerSchema(String schemaString, String subject, int version, int id)
      throws IOException, RestClientException {
    return registerSchema(schemaString, subject, version, id, false).getId();
  }

  public RegisterSchemaResponse registerSchema(String schemaString, String subject,
                                               int version, int id, boolean normalize)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    request.setVersion(version);
    request.setId(id);
    return registerSchema(request, subject, normalize);
  }

  public RegisterSchemaResponse registerSchema(String schemaString, String schemaType,
                                               List<SchemaReference> references, String subject,
                                               int version, int id)
      throws IOException, RestClientException {
    return registerSchema(schemaString, schemaType, references, subject, version, id, false);
  }

  public RegisterSchemaResponse registerSchema(String schemaString, String schemaType,
                                               List<SchemaReference> references, String subject,
                                               int version, int id, boolean normalize)
                            throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    request.setSchemaType(schemaType);
    request.setReferences(references);
    request.setVersion(version);
    request.setId(id);
    return registerSchema(request, subject, normalize);
  }

  public RegisterSchemaResponse registerSchema(RegisterSchemaRequest registerSchemaRequest,
                                               String subject,
                                               boolean normalize)
      throws IOException, RestClientException {
    return registerSchema(DEFAULT_REQUEST_PROPERTIES, registerSchemaRequest, subject, normalize);
  }

  public RegisterSchemaResponse registerSchema(Map<String, String> requestProperties,
                                               RegisterSchemaRequest registerSchemaRequest,
                                               String subject,
                                               boolean normalize)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions")
        .queryParam("normalize", normalize);
    String path = builder.build(subject).toString();

    RegisterSchemaResponse response = httpRequest(
        path, "POST",
        registerSchemaRequest.toJson().getBytes(StandardCharsets.UTF_8),
        requestProperties,
        REGISTER_RESPONSE_TYPE);

    return response;
  }

  public RegisterSchemaResponse modifySchemaTags(Map<String, String> requestProperties,
                                                 TagSchemaRequest tagSchemaRequest,
                                                 String subject,
                                                 String version)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions/{version}/tags");
    String path = builder.build(subject, version).toString();

    RegisterSchemaResponse response = httpRequest(
        path, "POST",
        tagSchemaRequest.toJson().getBytes(StandardCharsets.UTF_8),
        requestProperties,
        REGISTER_RESPONSE_TYPE);

    return response;
  }

  public List<String> testCompatibility(String schemaString, String subject, boolean verbose)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    return testCompatibility(request, subject, null, false, verbose);
  }

  // Visible for testing
  public List<String> testCompatibility(String schemaString, String subject, String version)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    return testCompatibility(request, subject, version, false, false);
  }

  public List<String> testCompatibility(String schemaString,
                                        String schemaType,
                                        List<SchemaReference> references,
                                        String subject,
                                        String version,
                                        boolean verbose)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    request.setSchemaType(schemaType);
    request.setReferences(references);
    return testCompatibility(request, subject, version, false, verbose);
  }

  public List<String> testCompatibility(RegisterSchemaRequest registerSchemaRequest,
                                        String subject,
                                        String version,
                                        boolean normalize,
                                        boolean verbose)
      throws IOException, RestClientException {
    return testCompatibility(DEFAULT_REQUEST_PROPERTIES, registerSchemaRequest,
                             subject, version, normalize, verbose);
  }

  public List<String> testCompatibility(Map<String, String> requestProperties,
                                        RegisterSchemaRequest registerSchemaRequest,
                                        String subject,
                                        String version,
                                        boolean normalize,
                                        boolean verbose)
      throws IOException, RestClientException {
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

    CompatibilityCheckResponse response =
        httpRequest(path, "POST",
                    registerSchemaRequest.toJson().getBytes(StandardCharsets.UTF_8),
                    requestProperties, COMPATIBILITY_CHECK_RESPONSE_TYPE_REFERENCE);
    if (response.getIsCompatible()) {
      return Collections.emptyList();
    } else {
      return Optional.ofNullable(response.getMessages())
              .filter(it -> !it.isEmpty())
              .orElseGet(() -> Collections.singletonList("Schemas are incompatible"));
    }
  }

  public ConfigUpdateRequest updateCompatibility(String compatibility, String subject)
      throws IOException, RestClientException {
    ConfigUpdateRequest request = new ConfigUpdateRequest();
    request.setCompatibilityLevel(compatibility);
    return updateConfig(request, subject);
  }

  public ConfigUpdateRequest updateConfig(ConfigUpdateRequest configUpdateRequest,
                                          String subject)
      throws IOException, RestClientException {
    return updateConfig(DEFAULT_REQUEST_PROPERTIES, configUpdateRequest, subject);
  }

  /**
   * On success, this api simply echoes the request in the response.
   */
  public ConfigUpdateRequest updateConfig(Map<String, String> requestProperties,
                                          ConfigUpdateRequest configUpdateRequest,
                                          String subject)
      throws IOException, RestClientException {
    String path = subject != null
                  ? UriBuilder.fromPath("/config/{subject}").build(subject).toString()
                  : "/config";

    ConfigUpdateRequest response =
        httpRequest(path, "PUT", configUpdateRequest.toJson().getBytes(StandardCharsets.UTF_8),
                    requestProperties, UPDATE_CONFIG_RESPONSE_TYPE_REFERENCE);
    return response;
  }

  public Config getConfig(String subject)
      throws IOException, RestClientException {
    return getConfig(DEFAULT_REQUEST_PROPERTIES, subject, false);
  }

  public Config getConfig(Map<String, String> requestProperties,
                          String subject)
      throws IOException, RestClientException {
    return getConfig(requestProperties, subject, false);
  }

  public Config getConfig(Map<String, String> requestProperties,
                          String subject,
                          boolean defaultToGlobal)
      throws IOException, RestClientException {
    String path = subject != null
        ? UriBuilder.fromPath("/config/{subject}")
        .queryParam("defaultToGlobal", defaultToGlobal).build(subject).toString()
        : "/config";

    Config config =
        httpRequest(path, "GET", null, requestProperties, GET_CONFIG_RESPONSE_TYPE);
    return config;
  }

  public Config deleteConfig(String subject)
      throws IOException, RestClientException {
    return deleteConfig(DEFAULT_REQUEST_PROPERTIES, subject);
  }

  public Config deleteConfig(Map<String, String> requestProperties, String subject)
      throws IOException, RestClientException {
    String path = subject != null
        ? UriBuilder.fromPath("/config/{subject}").build(subject).toString() : "/config";

    Config response = httpRequest(path, "DELETE", null, requestProperties,
        DELETE_SUBJECT_CONFIG_RESPONSE_TYPE);
    return response;
  }

  public ModeUpdateRequest setMode(String mode)
      throws IOException, RestClientException {
    return setMode(mode, null);
  }

  public ModeUpdateRequest setMode(String mode, String subject)
      throws IOException, RestClientException {
    return setMode(mode, subject, false);
  }

  public ModeUpdateRequest setMode(String mode, String subject, boolean force)
      throws IOException, RestClientException {
    ModeUpdateRequest request = new ModeUpdateRequest();
    request.setMode(mode);
    return setMode(DEFAULT_REQUEST_PROPERTIES, request, subject, force);
  }

  /**
   * On success, this api simply echoes the request in the response.
   */
  public ModeUpdateRequest setMode(Map<String, String> requestProperties,
                                   ModeUpdateRequest modeUpdateRequest,
                                   String subject,
                                   boolean force)
      throws IOException, RestClientException {
    String path = subject != null
        ? UriBuilder.fromPath("/mode/{subject}")
        .queryParam("force", force).build(subject).toString()
        : UriBuilder.fromPath("/mode")
            .queryParam("force", force).build().toString();

    ModeUpdateRequest response =
        httpRequest(path, "PUT", modeUpdateRequest.toJson().getBytes(StandardCharsets.UTF_8),
            requestProperties, UPDATE_MODE_RESPONSE_TYPE_REFERENCE);
    return response;
  }

  public Mode getMode()
      throws IOException, RestClientException {
    return getMode(null, false);
  }

  public Mode getMode(String subject)
      throws IOException, RestClientException {
    return getMode(subject, false);
  }

  public Mode getMode(String subject, boolean defaultToGlobal)
      throws IOException, RestClientException {
    String path = subject != null
        ? UriBuilder.fromPath("/mode/{subject}")
        .queryParam("defaultToGlobal", defaultToGlobal).build(subject).toString()
        : "/mode";

    Mode mode =
        httpRequest(path, "GET", null, DEFAULT_REQUEST_PROPERTIES, GET_MODE_RESPONSE_TYPE);
    return mode;
  }

  public Mode deleteSubjectMode(String subject)
      throws IOException, RestClientException {
    return deleteSubjectMode(DEFAULT_REQUEST_PROPERTIES, subject);
  }

  public Mode deleteSubjectMode(Map<String, String> requestProperties, String subject)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/mode/{subject}");
    String path = builder.build(subject).toString();

    Mode response = httpRequest(path, "DELETE", null, requestProperties,
        DELETE_SUBJECT_MODE_RESPONSE_TYPE);
    return response;
  }

  public List<Schema> getSchemas(
      String subjectPrefix,
      boolean lookupDeletedSchema,
      boolean latestOnly)
      throws IOException, RestClientException {
    return getSchemas(DEFAULT_REQUEST_PROPERTIES,
        subjectPrefix, lookupDeletedSchema, latestOnly, null, null, null);
  }

  public List<Schema> getSchemas(Map<String, String> requestProperties,
      String subjectPrefix,
      boolean lookupDeletedSchema,
      boolean latestOnly,
      String ruleType,
      Integer offset,
      Integer limit)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/schemas");
    if (subjectPrefix != null) {
      builder.queryParam("subjectPrefix", subjectPrefix);
    }
    builder.queryParam("deleted", lookupDeletedSchema);
    builder.queryParam("latestOnly", latestOnly);
    if (ruleType != null) {
      builder.queryParam("ruleType", ruleType);
    }
    if (offset != null) {
      builder.queryParam("offset", offset);
    }
    if (limit != null) {
      builder.queryParam("limit", limit);
    }
    String path = builder.build().toString();

    List<Schema> response = httpRequest(path, "GET", null, requestProperties,
        GET_SCHEMAS_RESPONSE_TYPE);
    return response;
  }

  public List<ExtendedSchema> getSchemas(Map<String, String> requestProperties,
      String subjectPrefix,
      boolean includeAliases,
      boolean lookupDeletedSchema,
      boolean latestOnly,
      String ruleType,
      Integer offset,
      Integer limit)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/schemas");
    if (subjectPrefix != null) {
      builder.queryParam("subjectPrefix", subjectPrefix);
    }
    builder.queryParam("aliases", includeAliases);
    builder.queryParam("deleted", lookupDeletedSchema);
    builder.queryParam("latestOnly", latestOnly);
    if (ruleType != null) {
      builder.queryParam("ruleType", ruleType);
    }
    if (offset != null) {
      builder.queryParam("offset", offset);
    }
    if (limit != null) {
      builder.queryParam("limit", limit);
    }
    String path = builder.build().toString();

    List<ExtendedSchema> response = httpRequest(path, "GET", null, requestProperties,
        GET_EXTENDED_SCHEMAS_RESPONSE_TYPE);
    return response;
  }

  public SchemaString getId(int id) throws IOException, RestClientException {
    return getId(DEFAULT_REQUEST_PROPERTIES, id, null, false);
  }

  public SchemaString getId(int id, boolean fetchMaxId)
      throws IOException, RestClientException {
    return getId(DEFAULT_REQUEST_PROPERTIES, id, null, fetchMaxId);
  }

  public SchemaString getId(int id, String subject) throws IOException, RestClientException {
    return getId(DEFAULT_REQUEST_PROPERTIES, id, subject, false);
  }

  public SchemaString getId(int id, String subject, boolean fetchMaxId)
      throws IOException, RestClientException {
    return getId(DEFAULT_REQUEST_PROPERTIES, id, subject, fetchMaxId);
  }

  public SchemaString getId(Map<String, String> requestProperties,
                            int id) throws IOException, RestClientException {
    return getId(requestProperties, id, null, false);
  }

  public SchemaString getId(Map<String, String> requestProperties,
                            int id, String subject) throws IOException, RestClientException {
    return getId(requestProperties, id, subject, false);
  }

  public SchemaString getId(Map<String, String> requestProperties,
      int id, String subject, boolean fetchMaxId) throws IOException, RestClientException {
    return getId(requestProperties, id, subject, null, fetchMaxId);
  }

  public SchemaString getId(Map<String, String> requestProperties,
      int id, String subject, Set<String> findTags, boolean fetchMaxId)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/schemas/ids/{id}")
        .queryParam("fetchMaxId", fetchMaxId);
    if (subject != null) {
      builder.queryParam("subject", subject);
    }
    if (findTags != null && !findTags.isEmpty()) {
      builder.queryParam("findTags", String.join(",", findTags));
    }
    String path = builder.build(id).toString();

    SchemaString response = httpRequest(path, "GET", null, requestProperties,
                                        GET_SCHEMA_BY_ID_RESPONSE_TYPE);
    return response;
  }

  public String getOnlySchemaById(int id) throws RestClientException, IOException {
    return getOnlySchemaById(DEFAULT_REQUEST_PROPERTIES, id, null);
  }

  public String getOnlySchemaById(Map<String, String> requestProperties,
                                  int id, String subject)
          throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/schemas/ids/{id}/schema");
    if (subject != null) {
      builder.queryParam("subject", subject);
    }
    String path = builder.build(id).toString();
    JsonNode response = httpRequest(path, "GET", null,
            requestProperties, GET_SCHEMA_ONLY_BY_VERSION_RESPONSE_TYPE);
    return response.toString();
  }

  public List<String> getSchemaTypes() throws IOException, RestClientException {
    return getSchemaTypes(DEFAULT_REQUEST_PROPERTIES);
  }

  public List<String> getSchemaTypes(Map<String, String> requestProperties)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/schemas/types");
    String path = builder.toString();

    List<String> response = httpRequest(path, "GET", null, requestProperties,
        GET_SCHEMA_TYPES_TYPE);
    return response;
  }

  public Schema getVersion(String subject, int version) throws IOException, RestClientException {
    return getVersion(DEFAULT_REQUEST_PROPERTIES, subject, version, false);
  }

  public Schema getVersion(String subject, int version, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    return getVersion(DEFAULT_REQUEST_PROPERTIES, subject, version, lookupDeletedSchema);
  }

  public Schema getVersion(Map<String, String> requestProperties,
                           String subject, int version)
      throws IOException, RestClientException {
    return getVersion(requestProperties, subject, version, false);
  }

  public Schema getVersion(Map<String, String> requestProperties,
                           String subject, int version, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    return getVersion(requestProperties, subject, version, lookupDeletedSchema, null);
  }

  public Schema getVersion(Map<String, String> requestProperties,
      String subject, int version, boolean lookupDeletedSchema, Set<String> findTags)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions/{version}")
        .queryParam("deleted", lookupDeletedSchema);
    if (findTags != null && !findTags.isEmpty()) {
      builder.queryParam("findTags", String.join(",", findTags));
    }
    String path = builder.build(subject, version).toString();

    Schema response = httpRequest(path, "GET", null, requestProperties,
                                  GET_SCHEMA_RESPONSE_TYPE);
    return response;
  }

  public Schema getLatestVersion(String subject)
      throws IOException, RestClientException {
    return getLatestVersion(DEFAULT_REQUEST_PROPERTIES, subject);
  }

  public Schema getLatestVersion(Map<String, String> requestProperties,
                                 String subject)
      throws IOException, RestClientException {
    return getLatestVersion(requestProperties, subject, null);
  }

  public Schema getLatestVersion(Map<String, String> requestProperties,
                                 String subject, Set<String> findTags)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions/latest");
    if (findTags != null && !findTags.isEmpty()) {
      builder.queryParam("findTags", String.join(",", findTags));
    }
    String path = builder.build(subject).toString();

    Schema response = httpRequest(path, "GET", null, requestProperties,
                                  GET_SCHEMA_RESPONSE_TYPE);
    return response;
  }

  public String getVersionSchemaOnly(String subject, int version)
            throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions/{version}/schema");
    String path = builder.build(subject, version).toString();

    JsonNode response = httpRequest(path, "GET", null, DEFAULT_REQUEST_PROPERTIES,
            GET_SCHEMA_ONLY_BY_VERSION_RESPONSE_TYPE);
    return response.toString();
  }

  public String getLatestVersionSchemaOnly(String subject)
            throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions/latest/schema");
    String path = builder.build(subject).toString();

    JsonNode response = httpRequest(path, "GET", null, DEFAULT_REQUEST_PROPERTIES,
            GET_SCHEMA_ONLY_BY_VERSION_RESPONSE_TYPE);
    return response.toString();
  }

  public Schema getLatestWithMetadata(
      String subject, Map<String, String> metadata, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    return getLatestWithMetadata(
        DEFAULT_REQUEST_PROPERTIES, subject, metadata, lookupDeletedSchema);
  }

  public Schema getLatestWithMetadata(Map<String, String> requestProperties,
      String subject, Map<String, String> metadata, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/metadata");
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      builder.queryParam("key", entry.getKey());
      builder.queryParam("value", entry.getValue());
    }
    builder.queryParam("deleted", lookupDeletedSchema);
    String path = builder.build(subject).toString();

    Schema response = httpRequest(path, "GET", null, requestProperties, GET_SCHEMA_RESPONSE_TYPE);
    return response;
  }

  public List<Integer> getReferencedBy(String subject, int version) throws IOException,
      RestClientException {
    return getReferencedBy(DEFAULT_REQUEST_PROPERTIES, subject, version);
  }

  public List<Integer> getReferencedBy(Map<String, String> requestProperties,
                                       String subject, int version)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions/{version}/referencedby");
    String path = builder.build(subject, version).toString();

    List<Integer> response = httpRequest(path, "GET", null, requestProperties,
        GET_REFERENCED_BY_RESPONSE_TYPE);
    return response;
  }

  public List<Integer> getAllVersions(String subject)
      throws IOException, RestClientException {
    return getAllVersions(DEFAULT_REQUEST_PROPERTIES, subject);
  }

  public List<Integer> getAllVersions(Map<String, String> requestProperties,
                                      String subject)
      throws IOException, RestClientException {
    return getAllVersions(requestProperties, subject, false, false);
  }

  public List<Integer> getAllVersions(Map<String, String> requestProperties,
                                      String subject,
                                      boolean lookupDeletedSchema)
          throws IOException, RestClientException {
    return getAllVersions(requestProperties, subject, lookupDeletedSchema, false);
  }

  public List<Integer> getAllVersions(Map<String, String> requestProperties,
                                      String subject,
                                      boolean lookupDeletedSchema,
                                      boolean lookupDeletedOnlySchema)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions");
    builder.queryParam("deleted", lookupDeletedSchema);
    builder.queryParam("deletedOnly", lookupDeletedOnlySchema);
    String path = builder.build(subject).toString();

    List<Integer> response = httpRequest(path, "GET", null, requestProperties,
        ALL_VERSIONS_RESPONSE_TYPE);
    return response;
  }

  public List<Integer> getDeletedOnlyVersions(String subject)
      throws IOException, RestClientException {
    return getAllVersions(DEFAULT_REQUEST_PROPERTIES, subject, false, true);
  }

  public List<String> getAllContexts()
      throws IOException, RestClientException {
    return getAllContexts(DEFAULT_REQUEST_PROPERTIES);
  }

  public List<String> getAllContexts(Map<String, String> requestProperties)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/contexts");
    String path = builder.build().toString();
    List<String> response = httpRequest(path, "GET", null, requestProperties,
        ALL_CONTEXTS_RESPONSE_TYPE);
    return response;
  }

  public List<String> getAllSubjects()
      throws IOException, RestClientException {
    return getAllSubjects(DEFAULT_REQUEST_PROPERTIES);
  }

  public List<String> getAllSubjects(boolean deletedSubjects)
      throws IOException, RestClientException {
    return getAllSubjects(DEFAULT_REQUEST_PROPERTIES, null, deletedSubjects);
  }

  public List<String> getAllSubjects(String subjectPrefix, boolean deletedSubjects)
      throws IOException, RestClientException {
    return getAllSubjects(DEFAULT_REQUEST_PROPERTIES, subjectPrefix, deletedSubjects);
  }

  public List<String> getAllSubjects(Map<String, String> requestProperties)
      throws IOException, RestClientException {
    List<String> response = httpRequest("/subjects", "GET", null, requestProperties,
                                        ALL_TOPICS_RESPONSE_TYPE);
    return response;
  }

  public List<String> getAllSubjects(Map<String, String> requestProperties,
                                     String subjectPrefix,
                                     boolean deletedSubjects)
      throws IOException, RestClientException {
    return getAllSubjects(requestProperties, subjectPrefix, deletedSubjects, false);
  }

  public List<String> getAllSubjects(Map<String, String> requestProperties,
                                     String subjectPrefix,
                                     boolean deletedSubjects,
                                     boolean deletedOnlySubjects)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects");
    builder.queryParam("deleted", deletedSubjects);
    builder.queryParam("deletedOnly", deletedOnlySubjects);
    if (subjectPrefix != null) {
      builder.queryParam("subjectPrefix", subjectPrefix);
    }
    String path = builder.build().toString();
    List<String> response = httpRequest(path, "GET", null, requestProperties,
        ALL_TOPICS_RESPONSE_TYPE);
    return response;
  }

  public List<String> getDeletedOnlySubjects(String subjectPrefix)
      throws IOException, RestClientException {
    return getAllSubjects(DEFAULT_REQUEST_PROPERTIES, subjectPrefix, false, true);
  }

  public List<String> getAllSubjectsById(int id)
      throws IOException, RestClientException {
    return getAllSubjectsById(DEFAULT_REQUEST_PROPERTIES, id, null);
  }

  public List<String> getAllSubjectsById(int id, String subject)
      throws IOException, RestClientException {
    return getAllSubjectsById(DEFAULT_REQUEST_PROPERTIES, id, subject);
  }

  public List<String> getAllSubjectsById(int id, String subject, boolean deleted)
      throws IOException, RestClientException {
    return getAllSubjectsById(DEFAULT_REQUEST_PROPERTIES, id, subject, deleted);
  }

  public List<String> getAllSubjectsById(Map<String, String> requestProperties,
                                         int id)
      throws IOException, RestClientException {
    return getAllSubjectsById(requestProperties, id, null, false);
  }

  public List<String> getAllSubjectsById(Map<String, String> requestProperties,
                                         int id,
                                         String subject)
      throws IOException, RestClientException {
    return getAllSubjectsById(requestProperties, id, subject, false);
  }

  public List<String> getAllSubjectsById(Map<String, String> requestProperties,
                                         int id,
                                         String subject,
                                         boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/schemas/ids/{id}/subjects");
    builder.queryParam("deleted", lookupDeleted);
    if (subject != null) {
      builder.queryParam("subject", subject);
    }
    String path = builder.build(id).toString();

    List<String> response = httpRequest(path, "GET", null, requestProperties,
                                        ALL_TOPICS_RESPONSE_TYPE);

    return response;
  }

  public List<SubjectVersion> getAllVersionsById(int id)
      throws IOException, RestClientException {
    return getAllVersionsById(DEFAULT_REQUEST_PROPERTIES, id, null);
  }

  public List<SubjectVersion> getAllVersionsById(int id, String subject)
      throws IOException, RestClientException {
    return getAllVersionsById(DEFAULT_REQUEST_PROPERTIES, id, subject);
  }

  public List<SubjectVersion> getAllVersionsById(int id, String subject, boolean deleted)
      throws IOException, RestClientException {
    return getAllVersionsById(DEFAULT_REQUEST_PROPERTIES, id, subject, deleted);
  }

  public List<SubjectVersion> getAllVersionsById(Map<String, String> requestProperties,
                                                 int id)
      throws IOException, RestClientException {
    return getAllVersionsById(requestProperties, id, null, false);
  }

  public List<SubjectVersion> getAllVersionsById(Map<String, String> requestProperties,
                                                 int id,
                                                 String subject)
      throws IOException, RestClientException {
    return getAllVersionsById(requestProperties, id, subject, false);
  }

  public List<SubjectVersion> getAllVersionsById(Map<String, String> requestProperties,
                                                 int id,
                                                 String subject,
                                                 boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/schemas/ids/{id}/versions");
    builder.queryParam("deleted", lookupDeleted);
    if (subject != null) {
      builder.queryParam("subject", subject);
    }
    String path = builder.build(id).toString();

    List<SubjectVersion> response = httpRequest(path, "GET", null, requestProperties,
        GET_VERSIONS_RESPONSE_TYPE);

    return response;
  }

  public Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version
  ) throws IOException,
                                                                            RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions/{version}");
    String path = builder.build(subject, version).toString();

    Integer response = httpRequest(path, "DELETE", null, requestProperties,
                                   DELETE_SUBJECT_VERSION_RESPONSE_TYPE);
    return response;
  }

  public Integer deleteSchemaVersion(
          Map<String, String> requestProperties,
          String subject,
          String version,
          boolean permanentDelete
  ) throws IOException,
          RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions/{version}");
    builder.queryParam("permanent", permanentDelete);
    String path = builder.build(subject, version).toString();

    Integer response = httpRequest(path, "DELETE", null, requestProperties,
            DELETE_SUBJECT_VERSION_RESPONSE_TYPE);
    return response;
  }

  public List<Integer> deleteSubject(
      Map<String, String> requestProperties,
      String subject
  ) throws IOException,
                                                            RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}");
    String path = builder.build(subject).toString();

    List<Integer> response = httpRequest(path, "DELETE", null, requestProperties,
                                         DELETE_SUBJECT_RESPONSE_TYPE);
    return response;
  }

  public List<Integer> deleteSubject(
          Map<String, String> requestProperties,
          String subject,
          boolean permanentDelete
  ) throws IOException,
          RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}");
    builder.queryParam("permanent", permanentDelete);
    String path = builder.build(subject).toString();

    List<Integer> response = httpRequest(path, "DELETE", null, requestProperties,
            DELETE_SUBJECT_RESPONSE_TYPE);
    return response;
  }

  public ServerClusterId getClusterId() throws IOException, RestClientException {
    return getClusterId(DEFAULT_REQUEST_PROPERTIES);
  }

  public ServerClusterId getClusterId(Map<String, String> requestProperties)
      throws IOException, RestClientException {
    return httpRequest("/v1/metadata/id", "GET", null,
                        requestProperties, GET_CLUSTER_ID_RESPONSE_TYPE);
  }

  public SchemaRegistryServerVersion getSchemaRegistryServerVersion()
          throws IOException, RestClientException {
    return httpRequest("/v1/metadata/version", "GET", null,
            DEFAULT_REQUEST_PROPERTIES, GET_SR_VERSION_RESPONSE_TYPE);
  }

  private static List<String> parseBaseUrl(String baseUrl) {
    List<String> baseUrls = Arrays.asList(baseUrl.split("\\s*,\\s*"));
    if (baseUrls.isEmpty()) {
      throw new IllegalArgumentException("Missing required schema registry url list");
    }
    return baseUrls;
  }

  public UrlList getBaseUrls() {
    return baseUrls;
  }

  private void setAuthRequestHeaders(HttpURLConnection connection) {
    if (basicAuthCredentialProvider != null) {
      String userInfo = basicAuthCredentialProvider.getUserInfo(connection.getURL());
      if (userInfo != null) {
        String authHeader = Base64.getEncoder().encodeToString(
            userInfo.getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty(AUTHORIZATION_HEADER, "Basic " + authHeader);
      }
    }

    if (bearerAuthCredentialProvider != null) {
      String bearerToken = bearerAuthCredentialProvider.getBearerToken(connection.getURL());
      if (bearerToken != null) {
        connection.setRequestProperty(AUTHORIZATION_HEADER, "Bearer " + bearerToken);
      }

      String targetIdentityPoolId = bearerAuthCredentialProvider.getTargetIdentityPoolId();
      if (targetIdentityPoolId != null) {
        connection.setRequestProperty(TARGET_IDENTITY_POOL_ID, targetIdentityPoolId);
      }

      String targetSchemaRegistry = bearerAuthCredentialProvider.getTargetSchemaRegistry();
      if (targetSchemaRegistry != null) {
        connection.setRequestProperty(TARGET_SR_CLUSTER, targetSchemaRegistry);
      }
    }
  }

  private void setCustomHeaders(HttpURLConnection connection) {
    if (httpHeaders != null) {
      httpHeaders.forEach((k, v) -> connection.setRequestProperty(k, v));
    }
  }

  public void setBasicAuthCredentialProvider(
      BasicAuthCredentialProvider basicAuthCredentialProvider) {
    this.basicAuthCredentialProvider = basicAuthCredentialProvider;
  }

  public void setBearerAuthCredentialProvider(
      BearerAuthCredentialProvider bearerAuthCredentialProvider) {
    this.bearerAuthCredentialProvider = bearerAuthCredentialProvider;
  }

  public void setHttpHeaders(Map<String, String> httpHeaders) {
    this.httpHeaders = httpHeaders;
  }

  public void setProxy(String proxyHost, int proxyPort) {
    this.proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
  }

  /**
   * Convert url string to URL. This method is package-private so that it can be mocked for unit
   * tests.
   *
   * @param requestUrl url string
   * @return {@link URL}
   * @throws MalformedURLException if the input string is a malformed URL
   */
  URL url(String requestUrl) throws MalformedURLException {
    return new URL(requestUrl);
  }

  @Override
  public void close() throws IOException {
    if (bearerAuthCredentialProvider != null) {
      bearerAuthCredentialProvider.close();
    }
  }
}
