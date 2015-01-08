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
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;

import io.confluent.kafka.schemaregistry.rest.entities.Config;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.rest.entities.ErrorMessage;

/**
 * Helper methods for making http client requests to the schema registry servlet.
 */
public class RestUtils {

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
  private static ObjectMapper jsonDeserializer = new ObjectMapper();

  public static <T> T httpRequest(String target, String method, byte[] entity,
                                  Map<String, String> requestProperties,
                                  TypeReference<T> responseFormat) throws IOException {
    log.debug(String.format("Sending %s with input %s to %s",
                            method, entity == null ? "null" : new String(entity), target));
    HttpURLConnection connection = null;
    try {
      URL url = new URL(target);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod(method);
      if (entity != null) {
        connection.setDoInput(true);
      }

      for (Map.Entry<String, String> entry : requestProperties.entrySet()) {
        connection.setRequestProperty(entry.getKey(), entry.getValue());
      }

      connection.setUseCaches(false);
      if (method != "DELETE") {
        connection.setDoOutput(true);
      }

      if (entity != null) {
        OutputStream os = connection.getOutputStream();
        os.write(entity);
        os.flush();
        os.close();
      }

      if (method != "DELETE") {
        if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
          InputStream is = connection.getInputStream();
          T result = jsonDeserializer.readValue(is, responseFormat);
          is.close();
          return result;
        } else if (connection.getResponseCode() == HttpURLConnection.HTTP_NO_CONTENT) {
          return null;
        } else {
          InputStream es = connection.getErrorStream();
          ErrorMessage errorMessage = jsonDeserializer.readValue(es, ErrorMessage.class);
          es.close();
          throw new WebApplicationException(errorMessage.getMessage(), errorMessage.getErrorCode());
        }
      }
      return null;
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  public static int registerSchema(String baseUrl, Map<String, String> requestProperties,
                                   RegisterSchemaRequest registerSchemaRequest, String subject)
      throws IOException {
    String url = String.format("%s/subjects/%s/versions", baseUrl, subject);

    RegisterSchemaResponse response =
        RestUtils.httpRequest(url, "POST", registerSchemaRequest.toJson().getBytes(),
                              requestProperties, REGISTER_RESPONSE_TYPE);
    return response.getVersion();
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
