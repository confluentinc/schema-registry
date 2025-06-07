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

package io.confluent.dpregistry.client.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.dpregistry.client.rest.entities.DataProduct;
import io.confluent.dpregistry.client.rest.entities.RegisteredDataProduct;
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

public class DataProductRegistryRestService extends RestService implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(DataProductRegistryRestService.class);

  private static final TypeReference<List<String>> STRINGS_TYPE =
      new TypeReference<List<String>>() {
      };
  private static final TypeReference<List<Integer>> INTEGERS_TYPE =
      new TypeReference<List<Integer>>() {
      };
  private static final TypeReference<RegisteredDataProduct> REGISTERED_DATA_PRODUCT_TYPE =
      new TypeReference<RegisteredDataProduct>() {
      };
  private static final TypeReference<Void> VOID_TYPE =
      new TypeReference<Void>() {
      };

  public DataProductRegistryRestService(UrlList baseUrls) {
    super(baseUrls);
  }

  public DataProductRegistryRestService(List<String> baseUrls) {
    super(baseUrls);
  }

  public DataProductRegistryRestService(String baseUrlConfig) {
    super(baseUrlConfig);
  }

  public List<String> listDataProductNames(String env, String cluster, boolean lookupDeleted)
      throws IOException, RestClientException {
    return listDataProductNames(DEFAULT_REQUEST_PROPERTIES, env, cluster, lookupDeleted);
  }

  public List<String> listDataProductNames(Map<String, String> requestProperties,
      String env, String cluster, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
        "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}/dataproducts")
        .queryParam("deleted", lookupDeleted);
    String path = builder.build(env, cluster).toString();

    return httpRequest(path, "GET", null, requestProperties, STRINGS_TYPE);
  }

  public List<String> listDataProductNamesWithPagination(String env, String cluster,
      boolean lookupDeleted, int offset, int limit) throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
        "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}/dataproducts")
        .queryParam("deleted", lookupDeleted)
        .queryParam("offset", offset)
        .queryParam("limit", limit);
    String path = builder.build(env, cluster).toString();

    return httpRequest(path, "GET", null, DEFAULT_REQUEST_PROPERTIES, STRINGS_TYPE);
  }

  public List<Integer> listDataProductVersions(
      String env, String cluster, String name, boolean lookupDeleted)
      throws IOException, RestClientException {
    return listDataProductVersions(DEFAULT_REQUEST_PROPERTIES, env, cluster, name, lookupDeleted);
  }

  public List<Integer> listDataProductVersions(Map<String, String> requestProperties,
      String env, String cluster, String name, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
            "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}/dataproducts/{name}")
        .queryParam("deleted", lookupDeleted);
    String path = builder.build(env, cluster, name).toString();

    return httpRequest(path, "GET", null, requestProperties, INTEGERS_TYPE);
  }

  public List<String> listDataProductVersionsWithPagination(
      String env, String cluster, String name,
      boolean lookupDeleted, int offset, int limit) throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
            "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}/dataproducts/{name}")
        .queryParam("deleted", lookupDeleted)
        .queryParam("offset", offset)
        .queryParam("limit", limit);
    String path = builder.build(env, cluster, name).toString();

    return httpRequest(path, "GET", null, DEFAULT_REQUEST_PROPERTIES, STRINGS_TYPE);
  }

  public RegisteredDataProduct getDataProduct(String env, String cluster, String name, int version,
      boolean lookupDeleted)
      throws IOException, RestClientException {
    return getDataProduct(DEFAULT_REQUEST_PROPERTIES, env, cluster, name, lookupDeleted);
  }

  public RegisteredDataProduct getDataProduct(Map<String, String> requestProperties,
      String env, String cluster, String name, boolean lookupDeleted)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
        "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}"
            + "/dataproducts/{name}/versions/{version}")
        .queryParam("deleted", lookupDeleted);
    String path = builder.build(env, cluster, name).toString();

    return httpRequest(path, "GET", null, requestProperties, REGISTERED_DATA_PRODUCT_TYPE);
  }

  public RegisteredDataProduct createDataProduct(String env, String cluster, DataProduct request)
      throws IOException, RestClientException {
    return createDataProduct(DEFAULT_REQUEST_PROPERTIES, env, cluster, request);
  }

  public RegisteredDataProduct createDataProduct(
      Map<String, String> requestProperties, String env, String cluster, DataProduct request)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
        "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}"
            + "/dataproducts/{name}/versions");
    String path = builder.build(env, cluster, request.getInfo().getName()).toString();

    return httpRequest(
        path, "POST",
        request.toJson().getBytes(StandardCharsets.UTF_8),
        requestProperties,
        REGISTERED_DATA_PRODUCT_TYPE);
  }

  public void deleteDataProduct(String env, String cluster, String name, boolean permanentDelete)
      throws IOException, RestClientException {
    deleteDataProduct(DEFAULT_REQUEST_PROPERTIES, env, cluster, name, permanentDelete);
  }

  public void deleteDataProduct(Map<String, String> requestProperties, String env, String cluster,
      String name, boolean permanentDelete)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
        "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}/dataproducts/{name}")
        .queryParam("permanent", permanentDelete);
    String path = builder.build(env, cluster, name).toString();

    httpRequest(path, "DELETE", null, requestProperties, VOID_TYPE);
  }

  public void deleteDataProductVersion(String env, String cluster, String name,
      int version, boolean permanentDelete) throws IOException, RestClientException {
    deleteDataProductVersion(
        DEFAULT_REQUEST_PROPERTIES, env, cluster, name, version, permanentDelete);
  }

  public void deleteDataProductVersion(Map<String, String> requestProperties, String env,
      String cluster, String name, int version, boolean permanentDelete)
      throws IOException, RestClientException {
    UriBuilder builder = UriBuilder.fromPath(
        "/dataproduct-registry/v1/environments/{env}/clusters/{cluster}"
            + "/dataproducts/{name}/versions/{version}")
        .queryParam("permanent", permanentDelete);
    String path = builder.build(env, cluster, name, version).toString();

    httpRequest(path, "DELETE", null, requestProperties, VOID_TYPE);
  }
}
