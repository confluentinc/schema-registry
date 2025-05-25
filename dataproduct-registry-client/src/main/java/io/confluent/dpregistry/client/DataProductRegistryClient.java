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

package io.confluent.dpregistry.client;

import io.confluent.dpregistry.client.rest.entities.DataProduct;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface DataProductRegistryClient extends Closeable {

  public static final int LATEST_VERSION = -1;

  List<String> listDataProducts(boolean lookupDeleted)
      throws IOException, RestClientException;

  DataProduct getDataProduct(String env, String cluster, String name, boolean lookupDeleted)
      throws IOException, RestClientException;

  DataProduct createDataProduct(
      String env,
      String cluster,
      DataProduct dataProduct)
      throws IOException, RestClientException;

  void deleteDataProduct(String env, String cluster, String name, boolean permanentDelete)
      throws IOException, RestClientException;

  void reset();

  @Override
  default void close() throws IOException {
  }
}
