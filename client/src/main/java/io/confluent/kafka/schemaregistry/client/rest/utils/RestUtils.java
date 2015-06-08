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

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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


  private static final RestService REST_SERVICE = new RestService();

  public static Schema lookUpSubjectVersion(String baseUrl, Map<String, String> requestProperties,
                                            RegisterSchemaRequest registerSchemaRequest,
                                            String subject)
      throws IOException, RestClientException {
    return REST_SERVICE.lookUpSubjectVersion(baseUrl, requestProperties, registerSchemaRequest, subject);
  }

  public static int registerSchema(String baseUrl, Map<String, String> requestProperties,
                                   RegisterSchemaRequest registerSchemaRequest, String subject)
      throws IOException, RestClientException {
    return REST_SERVICE.registerSchema(baseUrl, requestProperties, registerSchemaRequest, subject);
  }

  public static boolean testCompatibility(String baseUrl, Map<String, String> requestProperties,
                                          RegisterSchemaRequest registerSchemaRequest,
                                          String subject,
                                          String version)
      throws IOException, RestClientException {
    return REST_SERVICE.testCompatibility(baseUrl, requestProperties, registerSchemaRequest, subject, version);
  }

  /**
   *  On success, this api simply echoes the request in the response.
   */
  public static ConfigUpdateRequest updateConfig(String baseUrl,
                                                 Map<String, String> requestProperties,
                                                 ConfigUpdateRequest configUpdateRequest,
                                                 String subject)
      throws IOException, RestClientException {
    return REST_SERVICE.updateConfig(baseUrl, requestProperties, configUpdateRequest, subject);
  }

  public static Config getConfig(String baseUrl,
                                 Map<String, String> requestProperties,
                                 String subject)
      throws IOException, RestClientException {
    return REST_SERVICE.getConfig(baseUrl, requestProperties, subject);
  }

  public static SchemaString getId(String baseUrl, Map<String, String> requestProperties,
                                   int id) throws IOException, RestClientException {
    return REST_SERVICE.getId(baseUrl, requestProperties, id);
  }

  public static Schema getVersion(String baseUrl, Map<String, String> requestProperties,
                                  String subject, int version)
      throws IOException, RestClientException {
    return REST_SERVICE.getVersion(baseUrl, requestProperties, subject, version);
  }

  public static Schema getLatestVersion(String baseUrl, Map<String, String> requestProperties,
                                        String subject)
      throws IOException, RestClientException {
    return REST_SERVICE.getLatestVersion(baseUrl, requestProperties, subject);
  }

  public static List<Integer> getAllVersions(String baseUrl, Map<String, String> requestProperties,
                                             String subject)
      throws IOException, RestClientException {
    return REST_SERVICE.getAllVersions(baseUrl, requestProperties, subject);
  }

  public static List<String> getAllSubjects(String baseUrl, Map<String, String> requestProperties)
      throws IOException, RestClientException {
    return REST_SERVICE.getAllSubjects(baseUrl, requestProperties);
  }
}
