/*
 * Copyright 2021 Confluent Inc.
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
    return listKeks(DEFAULT_REQUEST_PROPERTIES, lookupDeleted);
  }

  public List<String> listKeks(Map<String, String> requestProperties, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks")
        .queryParam("deleted", lookupDeleted);
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

  public Dek getDek(String name, String scope, boolean lookupDeleted)
      throws IOException, RestClientException {
    return getDek(name, scope, null, lookupDeleted);
  }

  public Dek getDek(String name, String scope, DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    return getDek(DEFAULT_REQUEST_PROPERTIES, name, scope, algorithm, lookupDeleted);
  }

  public Dek getDek(Map<String, String> requestProperties,
      String kekName, String scope, DekFormat algorithm, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks/{scope}")
        .queryParam("deleted", lookupDeleted);
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(kekName, scope).toString();

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

  public void deleteDek(String name, String scope, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDek(name, scope, null, permanentDelete);
  }

  public void deleteDek(String name, String scope, DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDek(DEFAULT_REQUEST_PROPERTIES, name, scope, algorithm, permanentDelete);
  }

  public void deleteDek(Map<String, String> requestProperties, String name, String scope,
      DekFormat algorithm, boolean permanentDelete)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks/{scope}")
        .queryParam("permanent", permanentDelete);
    if (algorithm != null) {
      builder = builder.queryParam("algorithm", algorithm.name());
    }
    String path = builder.build(name, scope).toString();

    httpRequest(path, "DELETE", null, requestProperties, VOID_TYPE);
  }
}
