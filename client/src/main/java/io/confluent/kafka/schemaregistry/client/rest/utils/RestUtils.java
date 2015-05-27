/**
 * Copyright 2014 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.client.rest.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Helper methods for making http client requests to the schema registry servlet.
 */
public class RestUtils {

  /**
   * Minimum header data necessary for using the Schema Registry REST api.
   */
  public static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;

  static {
    DEFAULT_REQUEST_PROPERTIES = new HashMap<String, String>();
    DEFAULT_REQUEST_PROPERTIES.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
  }

  private static final Logger log = LoggerFactory.getLogger(RestUtils.class);
  private final static TypeReference<RegisterSchemaResponse> REGISTER_RESPONSE_TYPE =
      new TypeReference<RegisterSchemaResponse>() {
      };
  private final static TypeReference<Config> GET_CONFIG_RESPONSE_TYPE =
      new TypeReference<Config>() {
      };
  private final static TypeReference<SchemaString> GET_SCHEMA_BY_ID_RESPONSE_TYPE =
      new TypeReference<SchemaString>() {
      };
  private final static TypeReference<Schema> GET_SCHEMA_BY_VERSION_RESPONSE_TYPE =
      new TypeReference<Schema>() {
      };
  private final static TypeReference<List<Integer>> ALL_VERSIONS_RESPONSE_TYPE =
      new TypeReference<List<Integer>>() {
      };
  private final static TypeReference<List<String>> ALL_TOPICS_RESPONSE_TYPE =
      new TypeReference<List<String>>() {
      };
  private final static TypeReference<CompatibilityCheckResponse>
      COMPATIBILITY_CHECK_RESPONSE_TYPE_REFERENCE =
      new TypeReference<CompatibilityCheckResponse>() {
      };
  private final static TypeReference<Schema>
      SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE =
      new TypeReference<Schema>() {
      };
  private final static TypeReference<ConfigUpdateRequest>
      UPDATE_CONFIG_RESPONSE_TYPE_REFERENCE =
      new TypeReference<ConfigUpdateRequest>() {
      };
  private static ObjectMapper jsonDeserializer = new ObjectMapper();

  /**
   * @param baseUrl           HTTP connection will be established with this url.
   * @param method            HTTP method ("GET", "POST", "PUT", etc.)
   * @param requestBodyData   Bytes to be sent in the request body.
   * @param requestProperties HTTP header properties.
   * @param responseFormat    Expected format of the response to the HTTP request.
   * @param <T>               The type of the deserialized response to the HTTP request.
   * @return The deserialized response to the HTTP request, or null if no data is expected.
   */
  public static <T> T httpRequest(String baseUrl, String method, byte[] requestBodyData,
                                  Map<String, String> requestProperties,
                                  TypeReference<T> responseFormat)
      throws IOException, RestClientException {
    log.debug(String.format("Sending %s with input %s to %s",
                            method, requestBodyData == null ? "null" : new String(requestBodyData),
                            baseUrl));

    HttpURLConnection connection = null;
    try {
      URL url = new URL(baseUrl);
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

        OutputStream os = connection.getOutputStream();
        os.write(requestBodyData);
        os.flush();
        os.close();
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
        ErrorMessage errorMessage = jsonDeserializer.readValue(es, ErrorMessage.class);
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

  public static <T> T httpRequest(UrlRetryList baseUrls, String path, String method,
                                  byte[] requestBodyData, Map<String, String> requestProperties,
                                  TypeReference<T> responseFormat) throws IOException, RestClientException {
    for (int i = 0, n = baseUrls.size(); i < n; i++) {
      String baseUrl = baseUrls.current();
      try {
        return httpRequest(baseUrl + path, method, requestBodyData, requestProperties, responseFormat);
      } catch (IOException e) {
        baseUrls.fail(baseUrl);
        if (i == n-1) throw e; // Raise the exception since we have no more urls to try
      }
    }
    throw new IOException("Internal HTTP retry error"); // Can't get here
  }

  public static Schema lookUpSubjectVersion(UrlRetryList baseUrl, Map<String, String> requestProperties,
                                            RegisterSchemaRequest registerSchemaRequest,
                                            String subject)
          throws IOException, RestClientException {
    String path = String.format("/subjects/%s", subject);

    Schema schema = RestUtils.httpRequest(baseUrl, path, "POST", registerSchemaRequest.toJson().getBytes(),
                    requestProperties, SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE);

    return schema;
  }

  public static Schema lookUpSubjectVersion(String baseUrl, Map<String, String> requestProperties,
                                            RegisterSchemaRequest registerSchemaRequest,
                                            String subject)
      throws IOException, RestClientException {
    String url = String.format("%s/subjects/%s", baseUrl, subject);

    io.confluent.kafka.schemaregistry.client.rest.entities.Schema response =
        RestUtils.httpRequest(url, "POST", registerSchemaRequest.toJson().getBytes(),
                              requestProperties, SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE);
    return response;
  }

  public static int registerSchema(UrlRetryList baseUrls, Map<String, String> requestProperties,
                                   RegisterSchemaRequest registerSchemaRequest, String subject)
          throws IOException, RestClientException {
    String path = String.format("/subjects/%s/versions", subject);

    RegisterSchemaResponse response = RestUtils.httpRequest(baseUrls, path, "POST",
            registerSchemaRequest.toJson().getBytes(), requestProperties, REGISTER_RESPONSE_TYPE);

    return response.getId();
  }

  public static int registerSchema(String baseUrl, Map<String, String> requestProperties,
                                   RegisterSchemaRequest registerSchemaRequest, String subject)
      throws IOException, RestClientException {
    String url = String.format("%s/subjects/%s/versions", baseUrl, subject);

    RegisterSchemaResponse response =
        RestUtils.httpRequest(url, "POST", registerSchemaRequest.toJson().getBytes(),
                              requestProperties, REGISTER_RESPONSE_TYPE);
    return response.getId();
  }

  public static boolean testCompatibility(UrlRetryList baseUrl, Map<String, String> requestProperties,
                                          RegisterSchemaRequest registerSchemaRequest,
                                          String subject,
                                          String version)
          throws IOException, RestClientException {
    String path = String.format("/compatibility/subjects/%s/versions/%s", subject, version);

    CompatibilityCheckResponse response =
            RestUtils.httpRequest(baseUrl, path, "POST", registerSchemaRequest.toJson().getBytes(),
                    requestProperties, COMPATIBILITY_CHECK_RESPONSE_TYPE_REFERENCE);
    return response.getIsCompatible();
  }

  public static boolean testCompatibility(String baseUrl, Map<String, String> requestProperties,
                                          RegisterSchemaRequest registerSchemaRequest,
                                          String subject,
                                          String version)
      throws IOException, RestClientException {
    String
        url =
        String.format("%s/compatibility/subjects/%s/versions/%s", baseUrl, subject, version);

    CompatibilityCheckResponse response =
        RestUtils.httpRequest(url, "POST", registerSchemaRequest.toJson().getBytes(),
                              requestProperties, COMPATIBILITY_CHECK_RESPONSE_TYPE_REFERENCE);
    return response.getIsCompatible();
  }

  public static ConfigUpdateRequest updateConfig(UrlRetryList baseUrl,
                                                 Map<String, String> requestProperties,
                                                 ConfigUpdateRequest configUpdateRequest,
                                                 String subject)
          throws IOException, RestClientException {
    String path = subject != null ? String.format("/config/%s", subject) : "/config";

    ConfigUpdateRequest response =
            RestUtils.httpRequest(baseUrl, path, "PUT", configUpdateRequest.toJson().getBytes(),
                    requestProperties, UPDATE_CONFIG_RESPONSE_TYPE_REFERENCE);
    return response;
  }

  /**
   *  On success, this api simply echoes the request in the response.
   */
  public static ConfigUpdateRequest updateConfig(String baseUrl,
                                                 Map<String, String> requestProperties,
                                                 ConfigUpdateRequest configUpdateRequest,
                                                 String subject)
      throws IOException, RestClientException {
    String url = subject != null ? String.format("%s/config/%s", baseUrl, subject) :
                 String.format("%s/config", baseUrl);

    ConfigUpdateRequest response =
        RestUtils.httpRequest(url, "PUT", configUpdateRequest.toJson().getBytes(),
                              requestProperties, UPDATE_CONFIG_RESPONSE_TYPE_REFERENCE);
    return response;
  }

  public static Config getConfig(UrlRetryList baseUrl,
                                 Map<String, String> requestProperties,
                                 String subject)
          throws IOException, RestClientException {
    String path = subject != null ? String.format("/config/%s", subject) : "/config";

    Config config =
            RestUtils.httpRequest(baseUrl, path, "GET", null, requestProperties, GET_CONFIG_RESPONSE_TYPE);
    return config;
  }

  public static Config getConfig(String baseUrl,
                                 Map<String, String> requestProperties,
                                 String subject)
      throws IOException, RestClientException {
    String url = subject != null ? String.format("%s/config/%s", baseUrl, subject) :
                 String.format("%s/config", baseUrl);

    Config config =
        RestUtils.httpRequest(url, "GET", null, requestProperties, GET_CONFIG_RESPONSE_TYPE);
    return config;
  }

  public static SchemaString getId(UrlRetryList baseUrl, Map<String, String> requestProperties,
                                   int id) throws IOException, RestClientException {
    String path = String.format("/schemas/ids/%d", id);

    SchemaString response = RestUtils.httpRequest(baseUrl, path, "GET", null, requestProperties,
            GET_SCHEMA_BY_ID_RESPONSE_TYPE);
    return response;
  }

  public static SchemaString getId(String baseUrl, Map<String, String> requestProperties,
                                   int id) throws IOException, RestClientException {
    String url = String.format("%s/schemas/ids/%d", baseUrl, id);

    SchemaString response = RestUtils.httpRequest(url, "GET", null, requestProperties,
                                                  GET_SCHEMA_BY_ID_RESPONSE_TYPE);
    return response;
  }

  public static Schema getVersion(String baseUrl, Map<String, String> requestProperties,
                                  String subject, int version)
      throws IOException, RestClientException {
    String url = String.format("%s/subjects/%s/versions/%d", baseUrl, subject, version);

    Schema response = RestUtils.httpRequest(url, "GET", null, requestProperties,
                                            GET_SCHEMA_BY_VERSION_RESPONSE_TYPE);
    return response;
  }

  public static Schema getLatestVersion(UrlRetryList baseUrl, Map<String, String> requestProperties,
                                        String subject)
          throws IOException, RestClientException {
    String path = String.format("/subjects/%s/versions/latest", subject);

    Schema response = RestUtils.httpRequest(baseUrl, path, "GET", null, requestProperties,
            GET_SCHEMA_BY_VERSION_RESPONSE_TYPE);
    return response;
  }

  public static Schema getLatestVersion(String baseUrl, Map<String, String> requestProperties,
                                        String subject)
      throws IOException, RestClientException {
    String url = String.format("%s/subjects/%s/versions/latest", baseUrl, subject);

    Schema response = RestUtils.httpRequest(url, "GET", null, requestProperties,
                                            GET_SCHEMA_BY_VERSION_RESPONSE_TYPE);
    return response;
  }

  public static List<Integer> getAllVersions(String baseUrl, Map<String, String> requestProperties,
                                             String subject)
      throws IOException, RestClientException {
    String url = String.format("%s/subjects/%s/versions", baseUrl, subject);

    List<Integer> response = RestUtils.httpRequest(url, "GET", null, requestProperties,
                                                   ALL_VERSIONS_RESPONSE_TYPE);
    return response;
  }

  public static List<String> getAllSubjects(String baseUrl, Map<String, String> requestProperties)
      throws IOException, RestClientException {
    String url = String.format("%s/subjects", baseUrl);

    List<String> response = RestUtils.httpRequest(url, "GET", null, requestProperties,
                                                  ALL_TOPICS_RESPONSE_TYPE);
    return response;
  }
}
