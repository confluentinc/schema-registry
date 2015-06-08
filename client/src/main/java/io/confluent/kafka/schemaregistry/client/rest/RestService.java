package io.confluent.kafka.schemaregistry.client.rest;

import com.fasterxml.jackson.core.type.TypeReference;
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
import io.confluent.kafka.schemaregistry.client.rest.utils.RestUtils;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Rest access layer for sending requests to the schema registry.
 */
public class RestService {

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
  private <T> T httpRequest(String baseUrl, String method, byte[] requestBodyData,
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

  private <T> T httpRequest(UrlList baseUrls, String path, String method,
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

  public Schema lookUpSubjectVersion(UrlList baseUrl,
                                     RegisterSchemaRequest registerSchemaRequest,
                                     String subject)
          throws IOException, RestClientException {
    return lookUpSubjectVersion(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, registerSchemaRequest, subject);
  }

  public Schema lookUpSubjectVersion(UrlList baseUrl, Map<String, String> requestProperties,
                                     RegisterSchemaRequest registerSchemaRequest,
                                     String subject)
          throws IOException, RestClientException {
    String path = String.format("/subjects/%s", subject);

    Schema schema = httpRequest(baseUrl, path, "POST", registerSchemaRequest.toJson().getBytes(),
            requestProperties, SUBJECT_SCHEMA_VERSION_RESPONSE_TYPE_REFERENCE);

    return schema;
  }

  public Schema lookUpSubjectVersion(String baseUrl, Map<String, String> requestProperties,
                                     RegisterSchemaRequest registerSchemaRequest,
                                     String subject)
          throws IOException, RestClientException {
    return lookUpSubjectVersion(new UrlList(baseUrl), requestProperties,
            registerSchemaRequest, subject);
  }

  public int registerSchema(UrlList baseUrls, RegisterSchemaRequest registerSchemaRequest, String subject)
          throws IOException, RestClientException {
    return registerSchema(baseUrls, RestUtils.DEFAULT_REQUEST_PROPERTIES, registerSchemaRequest, subject);
  }

  public int registerSchema(UrlList baseUrls, Map<String, String> requestProperties,
                            RegisterSchemaRequest registerSchemaRequest, String subject)
          throws IOException, RestClientException {
    String path = String.format("/subjects/%s/versions", subject);

    RegisterSchemaResponse response = httpRequest(baseUrls, path, "POST",
            registerSchemaRequest.toJson().getBytes(), requestProperties, REGISTER_RESPONSE_TYPE);

    return response.getId();
  }

  public int registerSchema(String baseUrl, Map<String, String> requestProperties,
                            RegisterSchemaRequest registerSchemaRequest, String subject)
          throws IOException, RestClientException {
    return registerSchema(new UrlList(baseUrl), requestProperties,
            registerSchemaRequest, subject);
  }

  public boolean testCompatibility(UrlList baseUrl,
                                   RegisterSchemaRequest registerSchemaRequest,
                                   String subject,
                                   String version)
          throws IOException, RestClientException {
    return testCompatibility(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, registerSchemaRequest,
            subject, version);
  }

  public boolean testCompatibility(UrlList baseUrl, Map<String, String> requestProperties,
                                   RegisterSchemaRequest registerSchemaRequest,
                                   String subject,
                                   String version)
          throws IOException, RestClientException {
    String path = String.format("/compatibility/subjects/%s/versions/%s", subject, version);

    CompatibilityCheckResponse response =
            httpRequest(baseUrl, path, "POST", registerSchemaRequest.toJson().getBytes(),
                    requestProperties, COMPATIBILITY_CHECK_RESPONSE_TYPE_REFERENCE);
    return response.getIsCompatible();
  }

  public boolean testCompatibility(String baseUrl, Map<String, String> requestProperties,
                                   RegisterSchemaRequest registerSchemaRequest,
                                   String subject,
                                   String version)
          throws IOException, RestClientException {
    return testCompatibility(new UrlList(baseUrl), requestProperties, registerSchemaRequest,
            subject, version);
  }

  public ConfigUpdateRequest updateConfig(UrlList baseUrl,
                                          ConfigUpdateRequest configUpdateRequest,
                                          String subject)
          throws IOException, RestClientException {
    return updateConfig(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, configUpdateRequest, subject);
  }

  public ConfigUpdateRequest updateConfig(UrlList baseUrl,
                                          Map<String, String> requestProperties,
                                          ConfigUpdateRequest configUpdateRequest,
                                          String subject)
          throws IOException, RestClientException {
    String path = subject != null ? String.format("/config/%s", subject) : "/config";

    ConfigUpdateRequest response =
            httpRequest(baseUrl, path, "PUT", configUpdateRequest.toJson().getBytes(),
                    requestProperties, UPDATE_CONFIG_RESPONSE_TYPE_REFERENCE);
    return response;
  }

  /**
   *  On success, this api simply echoes the request in the response.
   */
  public ConfigUpdateRequest updateConfig(String baseUrl,
                                          Map<String, String> requestProperties,
                                          ConfigUpdateRequest configUpdateRequest,
                                          String subject)
          throws IOException, RestClientException {
    return updateConfig(new UrlList(baseUrl), requestProperties, configUpdateRequest, subject);
  }

  public Config getConfig(UrlList baseUrl,
                          String subject)
          throws IOException, RestClientException {
    return getConfig(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, subject);
  }

  public Config getConfig(UrlList baseUrl,
                          Map<String, String> requestProperties,
                          String subject)
          throws IOException, RestClientException {
    String path = subject != null ? String.format("/config/%s", subject) : "/config";

    Config config =
            httpRequest(baseUrl, path, "GET", null, requestProperties, GET_CONFIG_RESPONSE_TYPE);
    return config;
  }

  public Config getConfig(String baseUrl,
                          Map<String, String> requestProperties,
                          String subject)
          throws IOException, RestClientException {
    return getConfig(new UrlList(baseUrl), requestProperties, subject);
  }

  public SchemaString getId(UrlList baseUrl, int id) throws IOException, RestClientException {
    return getId(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, id);
  }

  public SchemaString getId(UrlList baseUrl, Map<String, String> requestProperties,
                            int id) throws IOException, RestClientException {
    String path = String.format("/schemas/ids/%d", id);

    SchemaString response = httpRequest(baseUrl, path, "GET", null, requestProperties,
            GET_SCHEMA_BY_ID_RESPONSE_TYPE);
    return response;
  }

  public SchemaString getId(String baseUrl, Map<String, String> requestProperties,
                            int id) throws IOException, RestClientException {
    return getId(new UrlList(baseUrl), requestProperties, id);
  }

  public Schema getVersion(String baseUrl, Map<String, String> requestProperties,
                           String subject, int version)
          throws IOException, RestClientException {
    String url = String.format("%s/subjects/%s/versions/%d", baseUrl, subject, version);

    Schema response = httpRequest(url, "GET", null, requestProperties,
            GET_SCHEMA_BY_VERSION_RESPONSE_TYPE);
    return response;
  }

  public Schema getLatestVersion(UrlList baseUrl, String subject)
          throws IOException, RestClientException {
    return getLatestVersion(baseUrl, RestUtils.DEFAULT_REQUEST_PROPERTIES, subject);
  }

  public Schema getLatestVersion(UrlList baseUrl, Map<String, String> requestProperties,
                                 String subject)
          throws IOException, RestClientException {
    String path = String.format("/subjects/%s/versions/latest", subject);

    Schema response = httpRequest(baseUrl, path, "GET", null, requestProperties,
            GET_SCHEMA_BY_VERSION_RESPONSE_TYPE);
    return response;
  }

  public Schema getLatestVersion(String baseUrl, Map<String, String> requestProperties,
                                 String subject)
          throws IOException, RestClientException {
    return getLatestVersion(new UrlList(baseUrl), requestProperties, subject);
  }

  public List<Integer> getAllVersions(String baseUrl, Map<String, String> requestProperties,
                                      String subject)
          throws IOException, RestClientException {
    String url = String.format("%s/subjects/%s/versions", baseUrl, subject);

    List<Integer> response = httpRequest(url, "GET", null, requestProperties,
            ALL_VERSIONS_RESPONSE_TYPE);
    return response;
  }

  public List<String> getAllSubjects(String baseUrl, Map<String, String> requestProperties)
          throws IOException, RestClientException {
    String url = String.format("%s/subjects", baseUrl);

    List<String> response = httpRequest(url, "GET", null, requestProperties,
            ALL_TOPICS_RESPONSE_TYPE);
    return response;
  }


}
