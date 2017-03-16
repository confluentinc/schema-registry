/**
 * Copyright 2015 Confluent Inc.
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rest access layer for sending requests to the schema registry.
 */
public class RestService {

  private static final Logger log = LoggerFactory.getLogger(RestService.class);
  private static final TypeReference<RegisterSchemaResponse> REGISTER_RESPONSE_TYPE =
      new TypeReference<RegisterSchemaResponse>() {
      };
  private static final TypeReference<Config> GET_CONFIG_RESPONSE_TYPE =
      new TypeReference<Config>() {
      };
  private static final TypeReference<SchemaString> GET_SCHEMA_BY_ID_RESPONSE_TYPE =
      new TypeReference<SchemaString>() {
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
  private static final int JSON_PARSE_ERROR_CODE = 50005;
  private static ObjectMapper jsonDeserializer = new ObjectMapper();

  public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;

  static {
    DEFAULT_REQUEST_PROPERTIES = new HashMap<String, String>();
    DEFAULT_REQUEST_PROPERTIES.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
  }

  private UrlList baseUrls;

  public RestService(UrlList baseUrls) {
    this.baseUrls = baseUrls;
  }

  public RestService(List<String> baseUrls) {
    this(new UrlList(baseUrls));
  }

  public RestService(String baseUrlConfig) {
    this(parseBaseUrl(baseUrlConfig));
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
    log.debug(String.format("Sending %s with input %s to %s",
                            method, requestBodyData == null ? "null" : new String(requestBodyData),
                            requestUrl));

    HttpURLConnection connection = null;
    try {
      URL url = new URL(requestUrl);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod(method);

      // connection.getResponseCode() implicitly calls getInputStream, so always set to true.
      // On the other hand, leaving this out breaks nothing.
      connection.setDoInput(true);

      for (Map.Entry<String, String> entry : requestProperties.entrySet()) {
        connection.setRequestProperty(entry.getKey(), entry.getValue());
      }

      connection.setUseCaches(false);

      if (requestBodyData != null) {
        connection.setDoOutput(true);
        OutputStream os = null;
        try {
          os = connection.getOutputStream();
          os.write(requestBodyData);
          os.flush();
        } catch (IOException e) {
          log.error("Failed to send HTTP request to endpoint: " + url, e);
          throw e;
        } finally {
          if (os != null) {
            os.close();
          }
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
        InputStream es = connection.getErrorStream();
        ErrorMessage errorMessage;
        try {
          errorMessage = jsonDeserializer.readValue(es, ErrorMessage.class);
        } catch (JsonProcessingException e) {
          errorMessage = new ErrorMessage(JSON_PARSE_ERROR_CODE, e.getMessage());
        }
        es.close();
        throw new RestClientException(errorMessage.getMessage(), responseCode,
                                      errorMessage.getErrorCode());
      }

    } finally {
      if (connection != null) {
        connection.disconnect();
      }
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
    return lookUpSubjectVersion(DEFAULT_REQUEST_PROPERTIES, registerSchemaRequest, subject);
  }

  public Schema lookUpSubjectVersion(Map<String, String> requestProperties,
                                     RegisterSchemaRequest registerSchemaRequest,
                                     String subject)
      throws IOException, RestClientException {
    String path = String.format("/subjects/%s", subject);

    Schema schema = httpRequest(path, "POST", registerSchemaRequest.toJson().getBytes(),
                                requestProperties, SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE);

    return schema;
  }

  public int registerSchema(String schemaString, String subject)
      throws IOException, RestClientException {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemaString);
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

    RegisterSchemaResponse response = httpRequest(path, "POST",
                                                  registerSchemaRequest.toJson().getBytes(),
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
        httpRequest(path, "POST", registerSchemaRequest.toJson().getBytes(),
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
        httpRequest(path, "PUT", configUpdateRequest.toJson().getBytes(),
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

  public SchemaString getId(int id) throws IOException, RestClientException {
    return getId(DEFAULT_REQUEST_PROPERTIES, id);
  }

  public SchemaString getId(Map<String, String> requestProperties,
                            int id) throws IOException, RestClientException {
    String path = String.format("/schemas/ids/%d", id);

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

}
