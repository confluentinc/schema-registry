/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.dekregistry.client.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.dekregistry.client.rest.entities.CreateDekRequest;
import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.client.rest.entities.UpdateKekRequest;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.UriBuilder;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DekRegistryRestService extends RestService implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(DekRegistryRestService.class);

  private static final TypeReference<List<String>> STRINGS_TYPE =
      new TypeReference<List<String>>() {
      };
  private static final TypeReference<List<Integer>> INTEGERS_TYPE =
      new TypeReference<List<Integer>>() {
      };
  private static final TypeReference<Dek> DEK_TYPE =
      new TypeReference<Dek>() {
      };
  private static final TypeReference<Kek> KEK_TYPE =
      new TypeReference<Kek>() {
      };
  private static final TypeReference<Void> VOID_TYPE =
      new TypeReference<Void>() {
      };

  public DekRegistryRestService(UrlList baseUrls) {
    super(baseUrls);
  }

  public DekRegistryRestService(List<String> baseUrls) {
    super(baseUrls);
  }

  public DekRegistryRestService(String baseUrlConfig) {
    super(baseUrlConfig);
  }

  public List<String> listKeks(boolean lookupDeleted)
      throws IOException, RestClientException {
    return listKeks(null, lookupDeleted);
  }

  public List<String> listKeks(List<String> subjectPrefix, boolean lookupDeleted)
      throws IOException, RestClientException {
    return listKeks(DEFAULT_REQUEST_PROPERTIES, subjectPrefix, lookupDeleted);
  }

  public List<String> listKeks(Map<String, String> requestProperties,
      List<String> subjectPrefix, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks")
        .queryParam("deleted", lookupDeleted);
    if (subjectPrefix != null && !subjectPrefix.isEmpty()) {
      for (String prefix : subjectPrefix) {
        builder = builder.queryParam("subjectPrefix", prefix);
      }
    }
    String path = builder.build().toString();

    return httpRequest(path, "GET", null, requestProperties, STRINGS_TYPE);
  }

  public Kek getKek(String name, boolean lookupDeleted)
      throws IOException, RestClientException {
    return getKek(DEFAULT_REQUEST_PROPERTIES, name, lookupDeleted);
  }

  public Kek getKek(Map<String, String> requestProperties, String name, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}")
        .queryParam("deleted", lookupDeleted);
    String path = builder.build(name).toString();

    return httpRequest(path, "GET", null, requestProperties, KEK_TYPE);
  }

  public List<String> listDeks(String kekName, boolean lookupDeleted)
      throws IOException, RestClientException {
    return listDeks(DEFAULT_REQUEST_PROPERTIES, kekName, lookupDeleted);
  }

  public List<String> listDeks(Map<String, String> requestProperties,
      String kekName, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks")
        .queryParam("deleted", lookupDeleted);
    String path = builder.build(kekName).toString();

    return httpRequest(path, "GET", null, requestProperties, STRINGS_TYPE);
  }

  public List<Integer> listDekVersions(String kekName, String subject,
      DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    return listDekVersions(DEFAULT_REQUEST_PROPERTIES, kekName, subject, algorithm, lookupDeleted);
  }

  public List<Integer> listDekVersions(Map<String, String> requestProperties,
      String kekName, String subject, DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks/{subject}/versions")
        .queryParam("deleted", lookupDeleted);
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(kekName, subject).toString();

    return httpRequest(path, "GET", null, requestProperties, INTEGERS_TYPE);
  }

  public Dek getDek(String name, String subject, boolean lookupDeleted)
      throws IOException, RestClientException {
    return getDek(name, subject, null, lookupDeleted);
  }

  public Dek getDek(String name, String subject, DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    return getDek(DEFAULT_REQUEST_PROPERTIES, name, subject, algorithm, lookupDeleted);
  }

  public Dek getDek(Map<String, String> requestProperties,
      String kekName, String subject, DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks/{subject}")
        .queryParam("deleted", lookupDeleted);
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(kekName, subject).toString();

    return httpRequest(path, "GET", null, requestProperties, DEK_TYPE);
  }

  public Dek getDekVersion(String name, String subject, int version, boolean lookupDeleted)
      throws IOException, RestClientException {
    return getDekVersion(name, subject, version, null, lookupDeleted);
  }

  public Dek getDekVersion(String name, String subject, int version,
      DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    return getDekVersion(
        DEFAULT_REQUEST_PROPERTIES, name, subject, version, algorithm, lookupDeleted);
  }

  public Dek getDekVersion(Map<String, String> requestProperties,
      String kekName, String subject, int version,
      DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
        "/dek-registry/v1/keks/{name}/deks/{subject}/versions/{version}")
        .queryParam("deleted", lookupDeleted);
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(kekName, subject, version).toString();

    return httpRequest(path, "GET", null, requestProperties, DEK_TYPE);
  }

  public Kek createKek(CreateKekRequest request)
      throws IOException, RestClientException {
    return createKek(DEFAULT_REQUEST_PROPERTIES, request);
  }

  public Kek createKek(
      Map<String, String> requestProperties,
      CreateKekRequest request)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks");
    String path = builder.build().toString();

    return httpRequest(
        path, "POST",
        request.toJson().getBytes(StandardCharsets.UTF_8),
        requestProperties,
        KEK_TYPE);
  }

  public Dek createDek(String kekName, CreateDekRequest request)
      throws IOException, RestClientException {
    return createDek(DEFAULT_REQUEST_PROPERTIES, kekName, request);
  }

  public Dek createDek(
      Map<String, String> requestProperties,
      String kekName,
      CreateDekRequest request)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks");
    String path = builder.build(kekName).toString();

    return httpRequest(
        path, "POST",
        request.toJson().getBytes(StandardCharsets.UTF_8),
        requestProperties,
        DEK_TYPE);
  }

  public Kek updateKek(String name, UpdateKekRequest request)
      throws IOException, RestClientException {
    return updateKek(DEFAULT_REQUEST_PROPERTIES, name, request);
  }

  public Kek updateKek(
      Map<String, String> requestProperties,
      String name,
      UpdateKekRequest request)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}");
    String path = builder.build(name).toString();

    return httpRequest(
        path, "PUT",
        request.toJson().getBytes(StandardCharsets.UTF_8),
        requestProperties,
        KEK_TYPE);
  }

  public void deleteKek(String name, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteKek(DEFAULT_REQUEST_PROPERTIES, name, permanentDelete);
  }

  public void deleteKek(Map<String, String> requestProperties, String name, boolean permanentDelete)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}")
        .queryParam("permanent", permanentDelete);
    String path = builder.build(name).toString();

    httpRequest(path, "DELETE", null, requestProperties, VOID_TYPE);
  }

  public void deleteDek(String name, String subject, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDek(name, subject, null, permanentDelete);
  }

  public void deleteDek(String name, String subject, DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDek(DEFAULT_REQUEST_PROPERTIES, name, subject, algorithm, permanentDelete);
  }

  public void deleteDek(Map<String, String> requestProperties, String name, String subject,
      DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks/{subject}")
        .queryParam("permanent", permanentDelete);
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(name, subject).toString();

    httpRequest(path, "DELETE", null, requestProperties, VOID_TYPE);
  }

  public void deleteDekVersion(String name, String subject, int version, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDekVersion(name, subject, version, null, permanentDelete);
  }

  public void deleteDekVersion(String name, String subject, int version,
      DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDekVersion(
        DEFAULT_REQUEST_PROPERTIES, name, subject, version, algorithm, permanentDelete);
  }

  public void deleteDekVersion(Map<String, String> requestProperties, String name, String subject,
      int version, DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
        "/dek-registry/v1/keks/{name}/deks/{subject}/versions/{version}")
        .queryParam("permanent", permanentDelete);
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(name, subject, version).toString();

    httpRequest(path, "DELETE", null, requestProperties, VOID_TYPE);
  }

  public void undeleteKek(String name)
      throws IOException, RestClientException {
    undeleteKek(DEFAULT_REQUEST_PROPERTIES, name);
  }

  public void undeleteKek(Map<String, String> requestProperties, String name)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/undelete");
    String path = builder.build(name).toString();

    httpRequest(path, "POST", null, requestProperties, VOID_TYPE);
  }

  public void undeleteDek(String name, String subject, DekFormat algorithm)
      throws IOException, RestClientException {
    undeleteDek(DEFAULT_REQUEST_PROPERTIES, name, subject, algorithm);
  }

  public void undeleteDek(Map<String, String> requestProperties, String name, String subject,
      DekFormat algorithm)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
        "/dek-registry/v1/keks/{name}/deks/{subject}/undelete");
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(name, subject).toString();

    httpRequest(path, "POST", null, requestProperties, VOID_TYPE);
  }

  public void undeleteDekVersion(String name, String subject, int version,
      DekFormat algorithm)
      throws IOException, RestClientException {
    undeleteDekVersion(
        DEFAULT_REQUEST_PROPERTIES, name, subject, version, algorithm);
  }

  public void undeleteDekVersion(Map<String, String> requestProperties, String name, String subject,
      int version, DekFormat algorithm)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
            "/dek-registry/v1/keks/{name}/deks/{subject}/versions/{version}/undelete");
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(name, subject, version).toString();

    httpRequest(path, "POST", null, requestProperties, VOID_TYPE);
  }
}
