/*
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

package io.confluent.kafka.schemaregistry.testutil;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A repository for mocked Schema Registry clients, to aid in testing.
 *
 * <p>Logically independent "instances" of mocked Schema Registry are created or retrieved
 * via named scopes {@link MockSchemaRegistry#getClientForScope(String)}.
 * Each named scope is an independent registry.
 * Each named-scope registry is statically defined and visible to the entire JVM.
 * Scopes can be cleaned up when no longer needed via {@link MockSchemaRegistry#dropScope(String)}.
 * Reusing a scope name after cleanup results in a completely new mocked Schema Registry instance.
 *
 * <p>This registry can be used to manage scoped clients directly, but scopes can also be registered
 * and used as {@code schema.registry.url} with the special pseudo-protocol 'mock://'
 * in serde configurations, so that testing code doesn't have to run an actual instance of
 * Schema Registry listening on a local port. For example,
 * {@code schema.registry.url: 'mock://my-scope-name'} corresponds to
 * {@code MockSchemaRegistry.getClientForScope("my-scope-name")}.
 */
public final class MockSchemaRegistry {
  private static final String MOCK_URL_PREFIX = "mock://";
  private static final Map<String, SchemaRegistryClient> SCOPED_CLIENTS = new HashMap<>();

  // Not instantiable. All access is via static methods.
  private MockSchemaRegistry() {

  }

  /**
   * Get a client for a mocked Schema Registry. The {@code scope} represents a particular registry,
   * so operations on one scope will never affect another.
   *
   * @param scope Identifies a logically independent Schema Registry instance. It's similar to a
   *              schema registry URL, in that two different Schema Registry deployments have two
   *              different URLs, except that these registries are only mocked, so they have no
   *              actual URL.
   * @return A client for the specified scope.
   */
  public static SchemaRegistryClient getClientForScope(final String scope) {
    synchronized (SCOPED_CLIENTS) {
      if (!SCOPED_CLIENTS.containsKey(scope)) {
        SCOPED_CLIENTS.put(scope, new MockSchemaRegistryClient());
      }
    }
    return SCOPED_CLIENTS.get(scope);
  }

  /**
   * Get a client for a mocked Schema Registry. The {@code scope} represents a particular registry,
   * so operations on one scope will never affect another.
   *
   * @param scope Identifies a logically independent Schema Registry instance. It's similar to a
   *              schema registry URL, in that two different Schema Registry deployments have two
   *              different URLs, except that these registries are only mocked, so they have no
   *              actual URL.
   * @param providers A list of schema providers.
   * @return A client for the specified scope.
   */
  public static SchemaRegistryClient getClientForScope(final String scope,
                                                       List<SchemaProvider> providers) {
    synchronized (SCOPED_CLIENTS) {
      if (!SCOPED_CLIENTS.containsKey(scope)) {
        SCOPED_CLIENTS.put(scope, new MockSchemaRegistryClient(providers));
      }
    }
    return SCOPED_CLIENTS.get(scope);
  }

  /**
   * Get a client for a mocked Schema Registry. The {@code scope} represents a particular registry,
   * so operations on one scope will never affect another.
   *
   * @param scopes Identifies a logically independent Schema Registry instance. It's similar to a
   *              List of schema registry URLs, in that two different Schema Registry deployments
   *              have two different URLs, except that these registries are only mocked, so they
   *              have no actual URL.
   * @param providers A list of schema providers.
   * @return A client for the specified scope.
   */
  public static SchemaRegistryClient getClientForScope(final List<String> scopes,
                                                       List<SchemaProvider> providers) {
    synchronized (SCOPED_CLIENTS) {
      for (String scope : scopes) {
        if (!SCOPED_CLIENTS.containsKey(scope)) {
          SCOPED_CLIENTS.put(scope, new MockSchemaRegistryClient(providers));
        }
      }
    }
    return SCOPED_CLIENTS.get(scopes.get(0));
  }

  /**
   * Destroy the mocked registry corresponding to the scope. Subsequent clients for the same scope
   * will have a completely blank slate.
   * @param scope Identifies a logically independent Schema Registry instance. It's similar to a
   *             schema registry URL, in that two different Schema Registry deployments have two
   *             different URLs, except that these registries are only mocked, so they have no
   *             actual URL.
   */
  public static void dropScope(final String scope) {
    synchronized (SCOPED_CLIENTS) {
      SCOPED_CLIENTS.remove(scope);
    }
  }

  public static void clear() {
    synchronized (SCOPED_CLIENTS) {
      SCOPED_CLIENTS.clear();
    }
  }

  public static String validateAndMaybeGetMockScope(final List<String> urls) {
    final List<String> mockScopes = new LinkedList<>();
    for (final String url : urls) {
      if (url.startsWith(MOCK_URL_PREFIX)) {
        mockScopes.add(url.substring(MOCK_URL_PREFIX.length()));
      }
    }

    if (mockScopes.isEmpty()) {
      return null;
    } else if (mockScopes.size() > 1) {
      throw new ConfigException(
              "Only one mock scope is permitted for 'schema.registry.url'. Got: " + urls
      );
    } else if (urls.size() > mockScopes.size()) {
      throw new ConfigException(
              "Cannot mix mock and real urls for 'schema.registry.url'. Got: " + urls
      );
    } else {
      return mockScopes.get(0);
    }
  }

  public static List<String> validateAndMaybeGetMockScope(final String baseUrls) {
    final List<String> mockScopes = new LinkedList<>();
    List<String> urls = parseBaseUrl(baseUrls);
    for (final String url : urls) {
      if (url.startsWith(MOCK_URL_PREFIX)) {
        mockScopes.add(url.substring(MOCK_URL_PREFIX.length()));
      }
    }

    if (mockScopes.isEmpty()) {
      return null;
    } else if (urls.size() > mockScopes.size()) {
      throw new ConfigException(
              "Cannot mix mock and real urls for 'schema.registry.url'. Got: " + urls
      );
    } else {
      return mockScopes;
    }
  }

  private static List<String> parseBaseUrl(String baseUrls) {
    List<String> urls = Arrays.asList(baseUrls.split("\\s*,\\s*"));
    if (urls.isEmpty()) {
      throw new IllegalArgumentException("Missing required schema registry url list");
    }
    return urls;
  }
}
