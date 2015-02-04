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
package io.confluent.kafka.schemaregistry.client;

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
import java.util.Map;

import javax.ws.rs.WebApplicationException;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;

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

  private final static TypeReference<Schema> GET_SCHEMA_RESPONSE_TYPE =
      new TypeReference<Schema>() {
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
        // TODO: This is a hack until we fix it correctly as part of the refactoring planned in
        // issue #66
//        throw new WebApplicationException(errorMessage.getMessage(), errorMessage.getErrorCode());
        throw new WebApplicationException(errorMessage.getMessage(), responseCode);
      }

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
    return response.getId();
  }

  public static Schema getId(String baseUrl, Map<String, String> requestProperties,
                             int id) throws IOException {
    String url = String.format("%s/schemas/ids/%d", baseUrl, id);

    Schema response = RestUtils.httpRequest(url, "GET", null, requestProperties,
                                            GET_SCHEMA_RESPONSE_TYPE);
    return response;
  }
}
