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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

public class KafkaRpcUrlsTest {

  @Test
  public void testHttpUrlsAreNotKafkaRpc() {
    assertNull(KafkaRpcUrls.validateAndMaybeGetBootstrapServers(
        List.of("http://localhost:8081")));
    assertNull(KafkaRpcUrls.validateAndMaybeGetBootstrapServers(
        List.of("https://a:8081", "https://b:8081")));
    assertNull(KafkaRpcUrls.validateAndMaybeGetBootstrapServers((List<String>) null));
  }

  @Test
  public void testBareSchemeDefersToBootstrapServers() {
    // Empty rather than null: the scheme matched, but named no brokers.
    assertEquals(List.of(), KafkaRpcUrls.validateAndMaybeGetBootstrapServers(
        List.of("kafka://")));
  }

  @Test
  public void testSchemeWithExplicitBrokers() {
    assertEquals(List.of("localhost:9092"),
        KafkaRpcUrls.validateAndMaybeGetBootstrapServers(List.of("kafka://localhost:9092")));
    assertEquals(List.of("a:9092", "b:9092"),
        KafkaRpcUrls.validateAndMaybeGetBootstrapServers(List.of("kafka://a:9092,b:9092")));
    assertEquals(List.of("a:9092", "b:9092"),
        KafkaRpcUrls.validateAndMaybeGetBootstrapServers(List.of("kafka://a:9092", "kafka://b:9092")));
  }

  @Test
  public void testStringOverloadSplitsOnCommas() {
    assertEquals(List.of("a:9092", "b:9092"),
        KafkaRpcUrls.validateAndMaybeGetBootstrapServers("kafka://a:9092, kafka://b:9092"));
    assertNull(KafkaRpcUrls.validateAndMaybeGetBootstrapServers("http://localhost:8081"));
  }

  @Test
  public void testMixingKafkaAndHttpIsRejected() {
    // A single client cannot speak both transports, so this is a configuration error rather than
    // a fallback.
    try {
      KafkaRpcUrls.validateAndMaybeGetBootstrapServers(
          List.of("kafka://", "http://localhost:8081"));
      fail("expected mixed schemes to be rejected");
    } catch (ConfigException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Cannot mix kafka and http urls"));
    }
  }

  @Test
  public void testMixingBareAndQualifiedSchemesIsRejected() {
    try {
      KafkaRpcUrls.validateAndMaybeGetBootstrapServers(List.of("kafka://", "kafka://a:9092"));
      fail("expected mixed forms to be rejected");
    } catch (ConfigException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Cannot mix"));
    }
  }

  @Test
  public void testResolvePrefersExplicitBrokers() {
    assertEquals(List.of("explicit:9092"), KafkaRpcUrls.resolveBootstrapServers(
        List.of("explicit:9092"), Map.of("bootstrap.servers", "ignored:9092")));
  }

  @Test
  public void testResolveFallsBackToBootstrapServers() {
    assertEquals(List.of("a:9092", "b:9092"), KafkaRpcUrls.resolveBootstrapServers(
        List.of(), Map.of("bootstrap.servers", "a:9092,b:9092")));
    // bootstrap.servers is a LIST config, so it may already be parsed.
    assertEquals(List.of("a:9092", "b:9092"), KafkaRpcUrls.resolveBootstrapServers(
        List.of(), Map.of("bootstrap.servers", List.of("a:9092", "b:9092"))));
  }

  @Test
  public void testResolveWithNoBrokersAnywhereExplainsBothRemedies() {
    try {
      KafkaRpcUrls.resolveBootstrapServers(List.of(), Map.of());
      fail("expected missing brokers to be rejected");
    } catch (ConfigException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("bootstrap.servers"));
      assertTrue(e.getMessage(), e.getMessage().contains("kafka://host:port"));
    }
  }

  @Test
  public void testFactoryReportsAMissingTransportModule() {
    // The transport lives in a separate module that is deliberately not on this module's classpath.
    // The failure must name the missing dependency rather than surface a NoClassDefFoundError.
    try {
      SchemaRegistryClientFactory.newClient(
          List.of("kafka://localhost:9092"), 10, null, Map.of(), null);
      fail("expected the missing transport module to be reported");
    } catch (ConfigException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("not on the classpath"));
      assertTrue(e.getMessage(),
          e.getMessage().contains("kafka-schema-registry-client-kafka-rpc"));
    }
  }

  @Test
  public void testFactoryStillReturnsAnHttpClientForHttpUrls() {
    SchemaRegistryClient client = SchemaRegistryClientFactory.newClient(
        List.of("http://localhost:8081"), 10, null, Map.of(), null);
    assertTrue(client instanceof CachedSchemaRegistryClient);
  }
}
