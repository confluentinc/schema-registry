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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProviderFactory;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeGetResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProviderFactory;

/**
 * Rest access layer for sending requests to the schema registry.
 */
public class RestService implements Configurable {

  private static final Logger log = LoggerFactory.getLogger(RestService.class);
  private static final TypeReference<RegisterSchemaResponse> REGISTER_RESPONSE_TYPE =
      new TypeReference<RegisterSchemaResponse>() {
      };
  private static final TypeReference<Config> GET_CONFIG_RESPONSE_TYPE =
      new TypeReference<Config>() {
      };
  private static final TypeReference<List<ModeGetResponse>> GET_MODES_RESPONSE_TYPE =
      new TypeReference<List<ModeGetResponse>>() {
      };
  private static final TypeReference<ModeGetResponse> GET_MODE_RESPONSE_TYPE =
      new TypeReference<ModeGetResponse>() {
      };
  private static final TypeReference<SchemaString> GET_SCHEMA_BY_ID_RESPONSE_TYPE =
      new TypeReference<SchemaString>() {
      };
  private static final TypeReference<JsonNode> GET_SCHEMA_ONLY_BY_VERSION_RESPONSE_TYPE =
      new TypeReference<JsonNode>() {
      };
  private static final TypeReference<Schema> GET_SCHEMA_BY_VERSION_RESPONSE_TYPE =
      new TypeReference<Schema>() {
      };
  private static final TypeReference<List<Integer>> ALL_VERSIONS_RESPONSE_TYPE =
      new TypeReference<List<Integer>>() {
      };
  private static final TypeReference<List<String>> ALL_TOPICS_RESPONSE_TYPE =
      new TypeReference<List<String>>() {
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
  private static final TypeReference<String> DELETE_MODE_RESPONSE_TYPE =
      new TypeReference<String>() {
      };

  private static final int HTTP_CONNECT_TIMEOUT_MS = 60000;
  private static final int HTTP_READ_TIMEOUT_MS = 60000;

  private static final int JSON_PARSE_ERROR_CODE = 50005;
  private static ObjectMapper jsonDeserializer = new ObjectMapper();

  private static final String AUTHORIZATION_HEADER = "Authorization";

  public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;

  static {
    DEFAULT_REQUEST_PROPERTIES =
        Collections.singletonMap("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
  }

  private UrlList baseUrls;
  private SSLSocketFactory sslSocketFactory;
  private BasicAuthCredentialProvider basicAuthCredentialProvider;
  private BearerAuthCredentialProvider bearerAuthCredentialProvider;
  private Map<String, String> httpHeaders;
  private Proxy proxy;

  public RestService(UrlList baseUrls) {
    this.baseUrls = baseUrls;
  }

  public RestService(List<String> baseUrls) {
    this(new UrlList(baseUrls));
  }

  public RestService(String baseUrlConfig) {
    this(parseBaseUrl(baseUrlConfig));
  }

  @Override
  public void configure(Map<String, ?> configs) {
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
    Integer proxyPort = (Integer) configs.get(SchemaRegistryClientConfig.PROXY_PORT);

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
      URL url = new URL(requestUrl);
      
      connection = buildConnection(url, method, requestProperties);

      if (requestBodyData != null) {
        connection.setDoOutput(true);
        try (OutputStream os = connection.getOutputStream()) {
          os.write(requestBodyData);
          os.flush();
        } catch (IOException e) {
          log.error("Failed to send HTTP request to endpoint: " + url, e);
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
          errorMessage = jsonDeserializer.readValue(es, ErrorMessage.class);
        } catch (JsonProcessingException e) {
          errorMessage = new ErrorMessage(JSON_PARSE_ERROR_CODE, e.getMessage());
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

    connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
    connection.setReadTimeout(HTTP_READ_TIMEOUT_MS);

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
      ((HttpsURLConnection)connection).setSSLSocketFactory(sslSocketFactory);
    }
  }

  private <T> T httpRequest(String path,
                            String method,
                            byte[] requestBodyData,
                            Map<String, String> requestProperties,
                            TypeReference<T> responseFormat)
      throws IOException, RestClientException {
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

  public Schema lookUpSubjectVersion(String schemaString, String subject)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    return lookUpSubjectVersion(request, subject);
  }

  public Schema lookUpSubjectVersion(RegisterSchemaRequest registerSchemaRequest,
                                     String subject)
      throws IOException, RestClientException {
    return lookUpSubjectVersion(DEFAULT_REQUEST_PROPERTIES, registerSchemaRequest, subject, false);
  }

  public Schema lookUpSubjectVersion(Map<String, String> requestProperties,
                                     RegisterSchemaRequest registerSchemaRequest,
                                     String subject)
      throws IOException, RestClientException {
    String path = String.format("/subjects/%s", subject);
    if (requestProperties.isEmpty()) {
      requestProperties = DEFAULT_REQUEST_PROPERTIES;
    }
    Schema schema = httpRequest(path, "POST",
                                registerSchemaRequest.toJson().getBytes(StandardCharsets.UTF_8),
                                requestProperties, SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE);

    return schema;
  }


  public Schema lookUpSubjectVersion(String schemaString, String subject,
                                     boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    return lookUpSubjectVersion(DEFAULT_REQUEST_PROPERTIES, request, subject, lookupDeletedSchema);
  }


  public Schema lookUpSubjectVersion(Map<String, String> requestProperties,
                                     RegisterSchemaRequest registerSchemaRequest,
                                     String subject,
                                     boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    String path = String.format("/subjects/%s?deleted=%s", subject,
                                lookupDeletedSchema);

    Schema schema = httpRequest(path, "POST",
                                registerSchemaRequest.toJson().getBytes(StandardCharsets.UTF_8),
                                requestProperties, SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE);

    return schema;
  }

  public int registerSchema(String schemaString, String subject)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    return registerSchema(request, subject);
  }

  public int registerSchema(String schemaString, String subject, int version, int id)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    request.setVersion(version);
    request.setId(id);
    return registerSchema(request, subject);
  }

  public int registerSchema(RegisterSchemaRequest registerSchemaRequest, String subject)
      throws IOException, RestClientException {
    return registerSchema(DEFAULT_REQUEST_PROPERTIES, registerSchemaRequest, subject);
  }

  public int registerSchema(Map<String, String> requestProperties,
                            RegisterSchemaRequest registerSchemaRequest, String subject)
      throws IOException, RestClientException {
    String path = String.format("/subjects/%s/versions", subject);

    RegisterSchemaResponse response = httpRequest(
        path, "POST",
        registerSchemaRequest.toJson().getBytes(StandardCharsets.UTF_8),
        requestProperties,
        REGISTER_RESPONSE_TYPE);

    return response.getId();
  }

  public boolean testCompatibility(String schemaString, String subject, String version)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
    return testCompatibility(request, subject, version);
  }

  public boolean testCompatibility(RegisterSchemaRequest registerSchemaRequest,
                                   String subject,
                                   String version)
      throws IOException, RestClientException {
    return testCompatibility(DEFAULT_REQUEST_PROPERTIES, registerSchemaRequest,
                             subject, version);
  }

  public boolean testCompatibility(Map<String, String> requestProperties,
                                   RegisterSchemaRequest registerSchemaRequest,
                                   String subject,
                                   String version)
      throws IOException, RestClientException {
    String path = String.format("/compatibility/subjects/%s/versions/%s", subject, version);

    CompatibilityCheckResponse response =
        httpRequest(path, "POST",
                    registerSchemaRequest.toJson().getBytes(StandardCharsets.UTF_8),
                    requestProperties, COMPATIBILITY_CHECK_RESPONSE_TYPE_REFERENCE);
    return response.getIsCompatible();
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
    String path = subject != null ? String.format("/config/%s", subject) : "/config";

    ConfigUpdateRequest response =
        httpRequest(path, "PUT", configUpdateRequest.toJson().getBytes(StandardCharsets.UTF_8),
                    requestProperties, UPDATE_CONFIG_RESPONSE_TYPE_REFERENCE);
    return response;
  }

  public Config getConfig(String subject)
      throws IOException, RestClientException {
    return getConfig(DEFAULT_REQUEST_PROPERTIES, subject);
  }

  public Config getConfig(Map<String, String> requestProperties,
                          String subject)
      throws IOException, RestClientException {
    String path = subject != null ? String.format("/config/%s", subject) : "/config";

    Config config =
        httpRequest(path, "GET", null, requestProperties, GET_CONFIG_RESPONSE_TYPE);
    return config;
  }

  public ModeUpdateRequest setMode(String mode)
      throws IOException, RestClientException {
    return setMode(mode, null);
  }

  public ModeUpdateRequest setMode(String mode, String subject)
      throws IOException, RestClientException {
    ModeUpdateRequest request = new ModeUpdateRequest();
    request.setMode(mode);
    return setMode(DEFAULT_REQUEST_PROPERTIES, request, subject);
  }

  /**
   * On success, this api simply echoes the request in the response.
   */
  public ModeUpdateRequest setMode(Map<String, String> requestProperties,
                                   ModeUpdateRequest modeUpdateRequest,
                                   String subject)
      throws IOException, RestClientException {
    String path = subject != null ? String.format("/mode/%s", subject) : "/mode";

    ModeUpdateRequest response =
        httpRequest(path, "PUT", modeUpdateRequest.toJson().getBytes(StandardCharsets.UTF_8),
            requestProperties, UPDATE_MODE_RESPONSE_TYPE_REFERENCE);
    return response;
  }

  public ModeGetResponse getMode()
      throws IOException, RestClientException {
    return getMode(null);
  }

  public ModeGetResponse getMode(String subject)
      throws IOException, RestClientException {
    String path = subject != null ? String.format("/mode/%s", subject) : "/mode";

    ModeGetResponse mode =
        httpRequest(path, "GET", null, DEFAULT_REQUEST_PROPERTIES, GET_MODE_RESPONSE_TYPE);
    return mode;
  }

  public SchemaString getId(int id) throws IOException, RestClientException {
    return getId(DEFAULT_REQUEST_PROPERTIES, id, false);
  }

  public SchemaString getId(Map<String, String> requestProperties,
                            int id) throws IOException, RestClientException {
    return getId(requestProperties, id, false);
  }

  public SchemaString getId(int id, boolean highWaterMark) throws IOException, RestClientException {
    return getId(DEFAULT_REQUEST_PROPERTIES, id, highWaterMark);
  }

  public SchemaString getId(Map<String, String> requestProperties,
                            int id, boolean highWaterMark) throws IOException, RestClientException {
    String path = String.format("/schemas/ids/%d?hwm=%b", id, highWaterMark);

    SchemaString response = httpRequest(path, "GET", null, requestProperties,
                                        GET_SCHEMA_BY_ID_RESPONSE_TYPE);
    return response;
  }

  public Schema getVersion(String subject, int version) throws IOException, RestClientException {
    return getVersion(DEFAULT_REQUEST_PROPERTIES, subject, version);
  }

  public Schema getVersion(Map<String, String> requestProperties,
                           String subject, int version)
      throws IOException, RestClientException {
    String path = String.format("/subjects/%s/versions/%d", subject, version);

    Schema response = httpRequest(path, "GET", null, requestProperties,
                                  GET_SCHEMA_BY_VERSION_RESPONSE_TYPE);
    return response;
  }

  public Schema getLatestVersion(String subject)
      throws IOException, RestClientException {
    return getLatestVersion(DEFAULT_REQUEST_PROPERTIES, subject);
  }

  public Schema getLatestVersion(Map<String, String> requestProperties,
                                 String subject)
      throws IOException, RestClientException {
    String path = String.format("/subjects/%s/versions/latest", subject);

    Schema response = httpRequest(path, "GET", null, requestProperties,
                                  GET_SCHEMA_BY_VERSION_RESPONSE_TYPE);
    return response;
  }

  public String getVersionSchemaOnly(String subject, int version)
            throws IOException, RestClientException {
    String path = String.format("/subjects/%s/versions/%d/schema", subject, version);

    JsonNode response = httpRequest(path, "GET", null, DEFAULT_REQUEST_PROPERTIES,
            GET_SCHEMA_ONLY_BY_VERSION_RESPONSE_TYPE);
    return response.toString();
  }

  public String getLatestVersionSchemaOnly(String subject)
            throws IOException, RestClientException {
    String path = String.format("/subjects/%s/versions/latest/schema", subject);

    JsonNode response = httpRequest(path, "GET", null, DEFAULT_REQUEST_PROPERTIES,
            GET_SCHEMA_ONLY_BY_VERSION_RESPONSE_TYPE);
    return response.toString();
  }

  public List<Integer> getAllVersions(String subject)
      throws IOException, RestClientException {
    return getAllVersions(DEFAULT_REQUEST_PROPERTIES, subject);
  }

  public List<Integer> getAllVersions(Map<String, String> requestProperties,
                                      String subject)
      throws IOException, RestClientException {
    String path = String.format("/subjects/%s/versions", subject);

    List<Integer> response = httpRequest(path, "GET", null, requestProperties,
                                         ALL_VERSIONS_RESPONSE_TYPE);
    return response;
  }

  public List<String> getAllSubjects()
      throws IOException, RestClientException {
    return getAllSubjects(DEFAULT_REQUEST_PROPERTIES);
  }

  public List<String> getAllSubjects(Map<String, String> requestProperties)
      throws IOException, RestClientException {
    List<String> response = httpRequest("/subjects", "GET", null, requestProperties,
                                        ALL_TOPICS_RESPONSE_TYPE);
    return response;
  }

  public List<String> getAllSubjectsById(int id)
      throws IOException, RestClientException {
    return getAllSubjectsById(DEFAULT_REQUEST_PROPERTIES, id);
  }

  public List<String> getAllSubjectsById(Map<String, String> requestProperties,
                                         int id)
      throws IOException, RestClientException {
    String path = String.format("/schemas/ids/%d/subjects", id);

    List<String> response = httpRequest(path, "GET", null, requestProperties,
                                        ALL_TOPICS_RESPONSE_TYPE);

    return response;
  }

  public Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version
  ) throws IOException,
                                                                            RestClientException {
    String path = String.format("/subjects/%s/versions/%s", subject, version);

    Integer response = httpRequest(path, "DELETE", null, requestProperties,
                                   DELETE_SUBJECT_VERSION_RESPONSE_TYPE);
    return response;
  }

  public List<Integer> deleteSubject(
      Map<String, String> requestProperties,
      String subject
  ) throws IOException,
                                                            RestClientException {
    String path = String.format("/subjects/%s", subject);

    List<Integer> response = httpRequest(path, "DELETE", null, requestProperties,
                                         DELETE_SUBJECT_RESPONSE_TYPE);
    return response;
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
}
