/*
 * Copyright 2026 Confluent Inc.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;

/**
 * Recognises the {@code kafka://} scheme on {@code schema.registry.url}, which directs schema
 * operations over Kafka RPCs to the brokers rather than over HTTP to a separate registry endpoint.
 *
 * <p>Two forms are accepted:
 * <ul>
 *   <li>{@code kafka://} — use the brokers this client already talks to, taken from
 *       {@code bootstrap.servers}. Nothing is duplicated, and there is no second
 *       endpoint to keep in step with the first.</li>
 *   <li>{@code kafka://host:port} — an explicit override, for the uncommon case
 *       where schema traffic should reach a different set of brokers.</li>
 * </ul>
 *
 * <p>This mirrors the {@code mock://} scheme handled by
 * {@link io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry}: the scheme on the
 * one config that already means "where is the registry" selects the transport, rather than
 * a second config that could contradict it.
 */
public class KafkaRpcUrls {

  public static final String KAFKA_URL_PREFIX = "kafka://";

  private static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

  private KafkaRpcUrls() {
  }

  /**
   * Returns the bootstrap servers to use if every URL carries the {@code kafka://} scheme, and null
   * if none does. An empty list means the scheme was given with no authority, so the caller should
   * fall back to {@code bootstrap.servers}; see
   * {@link #resolveBootstrapServers(List, Map)}.
   *
   * @throws ConfigException if schemes are mixed, since one client cannot speak both
   */
  public static List<String> validateAndMaybeGetBootstrapServers(final List<String> urls) {
    if (urls == null) {
      return null;
    }
    final List<String> authorities = new LinkedList<>();
    int kafkaUrls = 0;
    for (final String url : urls) {
      if (url != null && url.startsWith(KAFKA_URL_PREFIX)) {
        kafkaUrls++;
        final String authority = url.substring(KAFKA_URL_PREFIX.length()).trim();
        if (!authority.isEmpty()) {
          authorities.addAll(split(authority));
        }
      }
    }

    if (kafkaUrls == 0) {
      return null;
    } else if (urls.size() > kafkaUrls) {
      throw new ConfigException(
          "Cannot mix kafka and http urls for 'schema.registry.url'. Got: " + urls
      );
    } else if (!authorities.isEmpty() && authorities.size() < kafkaUrls) {
      // Some entries named brokers and others did not, so it is unclear whether the
      // unqualified ones meant bootstrap.servers or were a mistake. Refuse rather than guess.
      throw new ConfigException(
          "Cannot mix 'kafka://' with 'kafka://host:port' for 'schema.registry.url'. Got: " + urls
      );
    } else {
      return authorities;
    }
  }

  public static List<String> validateAndMaybeGetBootstrapServers(final String urls) {
    return urls == null ? null : validateAndMaybeGetBootstrapServers(split(urls));
  }

  /**
   * Resolves the brokers to use, falling back to the client's own
   * {@code bootstrap.servers} when the scheme named none.
   *
   * @throws ConfigException if no brokers can be determined
   */
  public static List<String> resolveBootstrapServers(
      final List<String> authorities, final Map<String, ?> configs) {
    if (authorities != null && !authorities.isEmpty()) {
      return authorities;
    }
    final Object bootstrap = configs == null ? null : configs.get(BOOTSTRAP_SERVERS_CONFIG);
    final List<String> resolved = toList(bootstrap);
    if (resolved.isEmpty()) {
      throw new ConfigException(
          "'schema.registry.url' is '" + KAFKA_URL_PREFIX + "' but no '" + BOOTSTRAP_SERVERS_CONFIG
              + "' is configured, so there are no brokers to send schema requests to. Either set "
              + BOOTSTRAP_SERVERS_CONFIG + " or name the brokers explicitly as '"
              + KAFKA_URL_PREFIX + "host:port'."
      );
    }
    return resolved;
  }

  private static List<String> toList(final Object value) {
    if (value == null) {
      return List.of();
    } else if (value instanceof Collection) {
      return ((Collection<?>) value).stream()
          .map(String::valueOf)
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .collect(Collectors.toList());
    } else {
      return split(String.valueOf(value));
    }
  }

  private static List<String> split(final String value) {
    return Arrays.stream(value.split("\\s*,\\s*"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
}
