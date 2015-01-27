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
package io.confluent.kafka.schemaregistry.utils;

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

import javax.ws.rs.WebApplicationException;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.SubjectSchemaVersionResponse;
import io.confluent.kafka.schemaregistry.rest.entities.Config;
import io.confluent.kafka.schemaregistry.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.rest.entities.ErrorMessage;

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
  private final static TypeReference<Schema> GET_SCHEMA_RESPONSE_TYPE =
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
  private final static TypeReference<SubjectSchemaVersionResponse>
      SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE =
      new TypeReference<SubjectSchemaVersionResponse>() {
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
                                  TypeReference<T> responseFormat) throws IOException {
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
        throw new WebApplicationException(errorMessage.getMessage(), errorMessage.getErrorCode());
      }

    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  public static int registerSchemaDryRun(String baseUrl, Map<String, String> requestProperties,
                                         RegisterSchemaRequest registerSchemaRequest, 
                                         String subject)
      throws IOException {
    String url = String.format("%s/subjects/%s", baseUrl, subject);

    SubjectSchemaVersionResponse response =
        RestUtils.httpRequest(url, "POST", registerSchemaRequest.toJson().getBytes(),
                              requestProperties, SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE);
    return response.getVersion();
  }

  public static int registerSchema(String baseUrl, Map<String, String> requestProperties,
                                    RegisterSchemaRequest registerSchemaRequest, String subject)
      throws IOException {
    String url = String.format("%s/subjects/%s/versions", baseUrl, subject);

    RegisterSchemaResponse response =
        RestUtils.httpRequest(url, "POST", registerSchemaRequest.toJson().getBytes(),
                              requestProperties, REGISTER_RESPONSE_TYPE);
    return response.getId();
  }

  public static boolean testCompatibility(String baseUrl, Map<String, String> requestProperties,
                                      RegisterSchemaRequest registerSchemaRequest, String subject,
                                      String version)
      throws IOException {
    String
        url =
        String.format("%s/compatibility/subjects/%s/versions/%s", baseUrl, subject, version);

    CompatibilityCheckResponse response =
        RestUtils.httpRequest(url, "POST", registerSchemaRequest.toJson().getBytes(),
                              requestProperties, COMPATIBILITY_CHECK_RESPONSE_TYPE_REFERENCE);
    return response.getIsCompatible();
  }

  public static void updateConfig(String baseUrl, Map<String, String> requestProperties,
                                  ConfigUpdateRequest configUpdateRequest, String subject)
      throws IOException {
    String url = subject != null ? String.format("%s/config/%s", baseUrl, subject) :
                 String.format("%s/config", baseUrl);

    RestUtils.httpRequest(url, "PUT", configUpdateRequest.toJson().getBytes(),
                          requestProperties, null);
  }

  public static Config getConfig(String baseUrl,
                                 Map<String, String> requestProperties,
                                 String subject)
      throws IOException {
    String url = subject != null ? String.format("%s/config/%s", baseUrl, subject) :
                 String.format("%s/config", baseUrl);

    Config config =
        RestUtils.httpRequest(url, "GET", null, requestProperties, GET_CONFIG_RESPONSE_TYPE);
    return config;
  }

  public static Schema getId(String baseUrl, Map<String, String> requestProperties,
                             int id) throws IOException {
    String url = String.format("%s/schemas/%d", baseUrl, id);

    Schema response = RestUtils.httpRequest(url, "GET", null, requestProperties,
                                            GET_SCHEMA_RESPONSE_TYPE);
    return response;
  }

  public static Schema getVersion(String baseUrl, Map<String, String> requestProperties,
                                  String subject, int version) throws IOException {
    String url = String.format("%s/subjects/%s/versions/%d", baseUrl, subject, version);

    Schema response = RestUtils.httpRequest(url, "GET", null, requestProperties,
                                            GET_SCHEMA_RESPONSE_TYPE);
    return response;
  }

  public static List<Integer> getAllVersions(String baseUrl, Map<String, String> requestProperties,
                                             String subject) throws IOException {
    String url = String.format("%s/subjects/%s/versions", baseUrl, subject);

    List<Integer> response = RestUtils.httpRequest(url, "GET", null, requestProperties,
                                                   ALL_VERSIONS_RESPONSE_TYPE);
    return response;
  }

  public static List<String> getAllSubjects(String baseUrl, Map<String, String> requestProperties)
      throws IOException {
    String url = String.format("%s/subjects", baseUrl);

    List<String> response = RestUtils.httpRequest(url, "GET", null, requestProperties,
                                                  ALL_TOPICS_RESPONSE_TYPE);
    return response;
  }
}
