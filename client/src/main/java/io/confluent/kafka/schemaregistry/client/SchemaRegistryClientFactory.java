/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

public class SchemaRegistryClientFactory {

  /**
   * Transport that reaches the registry over Kafka RPCs rather than HTTP. Loaded
   * reflectively so that this module does not depend on the Kafka build carrying the
   * schema RPCs; it is only needed on the classpath when {@code kafka://} is used.
   */
  private static final String KAFKA_RPC_REST_SERVICE_CLASS =
      "io.confluent.kafka.schemaregistry.client.rest.KafkaRpcRestService";

  public static SchemaRegistryClient newClient(
      List<String> baseUrls,
      int cacheCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> configs,
      Map<String, String> httpHeaders) {
    List<String> mockScopes = MockSchemaRegistry.validateAndMaybeGetMockScopes(baseUrls);
    if (mockScopes != null) {
      return MockSchemaRegistry.getClientForScope(mockScopes, providers);
    }
    List<String> bootstrapServers = KafkaRpcUrls.validateAndMaybeGetBootstrapServers(baseUrls);
    if (bootstrapServers != null) {
      return newKafkaRpcClient(bootstrapServers, cacheCapacity, providers, configs, httpHeaders);
    }
    return new CachedSchemaRegistryClient(
        baseUrls,
        cacheCapacity,
        providers,
        configs,
        httpHeaders
    );
  }

  public static SchemaRegistryClient newClient(
          String baseUrls,
          int cacheCapacity,
          List<SchemaProvider> providers,
          Map<String, ?> configs,
          Map<String, String> httpHeaders) {
    List<String> mockScopes = MockSchemaRegistry.validateAndMaybeGetMockScopes(baseUrls);
    if (mockScopes != null) {
      return MockSchemaRegistry.getClientForScope(mockScopes, providers);
    }
    List<String> bootstrapServers = KafkaRpcUrls.validateAndMaybeGetBootstrapServers(baseUrls);
    if (bootstrapServers != null) {
      return newKafkaRpcClient(bootstrapServers, cacheCapacity, providers, configs, httpHeaders);
    }
    return new CachedSchemaRegistryClient(
            baseUrls,
            cacheCapacity,
            providers,
            configs,
            httpHeaders
    );
  }

  private static SchemaRegistryClient newKafkaRpcClient(
      List<String> bootstrapServers,
      int cacheCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> configs,
      Map<String, String> httpHeaders) {
    List<String> resolved = KafkaRpcUrls.resolveBootstrapServers(bootstrapServers, configs);
    RestService restService;
    try {
      restService = Class.forName(KAFKA_RPC_REST_SERVICE_CLASS)
          .asSubclass(RestService.class)
          .getConstructor(List.class, Map.class)
          .newInstance(resolved, configs);
    } catch (ClassNotFoundException e) {
      throw new ConfigException(
          "'schema.registry.url' uses the '" + KafkaRpcUrls.KAFKA_URL_PREFIX + "' scheme, but "
              + KAFKA_RPC_REST_SERVICE_CLASS + " is not on the classpath. Add the "
              + "kafka-schema-registry-client-kafka-rpc dependency, or use an http url."
      );
    } catch (ReflectiveOperationException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      throw new ConfigException(
          "Could not create " + KAFKA_RPC_REST_SERVICE_CLASS + " for 'schema.registry.url': "
              + cause.getMessage()
      );
    }
    return new CachedSchemaRegistryClient(
        restService,
        cacheCapacity,
        providers,
        configs,
        httpHeaders
    );
  }
}
