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

package io.confluent.kafka.serializers.provenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.provenance.strategy.ClientProvenanceStrategy;
import io.confluent.kafka.serializers.provenance.strategy.ProvenanceStrategy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

/**
 * A misspelt provenance algorithm fails configuration rather than quietly turning it off, and the
 * strategy asked for provenance is configured as the others are.
 */
public class ProvenanceAlgorithmConfigTest {

  @Test
  public void aKnownAlgorithmOrNoneIsAccepted() {
    assertEquals("v1", config("v1").getProvenanceAlgorithm());
    assertEquals("V1", config("V1").getProvenanceAlgorithm());
    assertEquals("latest", config("latest").getProvenanceAlgorithm());
    assertEquals("LATEST", config("LATEST").getProvenanceAlgorithm());
    assertEquals("dynamic", config("dynamic").getProvenanceAlgorithm());
    assertNull(config("none").getProvenanceAlgorithm());
    assertNull(config("None").getProvenanceAlgorithm());
    assertNull(config("").getProvenanceAlgorithm());
    assertNull(config(null).getProvenanceAlgorithm());
  }

  @Test
  public void anUnknownAlgorithmFailsConfiguration() {
    assertThrows(ConfigException.class, () -> config("vI"));
  }

  @Test
  public void theStrategyDefaultsToAskingTheRegistryAndCanBeReplaced() {
    assertTrue(config("v1").provenanceStrategy() instanceof ClientProvenanceStrategy);

    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put(AbstractKafkaSchemaSerDeConfig.PROVENANCE_STRATEGY,
        ConfiguredStrategy.class.getName());
    ProvenanceStrategy strategy = new AbstractKafkaSchemaSerDeConfig(
        AbstractKafkaSchemaSerDeConfig.baseConfigDef(), props).provenanceStrategy();
    assertTrue(strategy instanceof ConfiguredStrategy);
    assertEquals("bogus", ((ConfiguredStrategy) strategy).configs
        .get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
  }

  @Test
  public void theStrategyIsClosedWhenReconfiguredAndWhenClosed() throws Exception {
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put(AbstractKafkaSchemaSerDeConfig.PROVENANCE_ALGORITHM, "v1");
    props.put(AbstractKafkaSchemaSerDeConfig.PROVENANCE_STRATEGY,
        ClosingStrategy.class.getName());
    ClosingStrategy.closed.set(0);
    Serde serde = new Serde(true);
    serde.configure(props);
    serde.configure(props);
    assertEquals(1, ClosingStrategy.closed.get());
    serde.close();
    assertEquals(2, ClosingStrategy.closed.get());
  }

  @Test
  public void aSerdeNotReadingByProvenanceBuildsNoStrategy() {
    // A serializer sharing a deserializer's config: no strategy, and the algorithm is off.
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put(AbstractKafkaSchemaSerDeConfig.PROVENANCE_ALGORITHM, "v1");
    props.put(AbstractKafkaSchemaSerDeConfig.PROVENANCE_STRATEGY,
        ClosingStrategy.class.getName());
    ClosingStrategy.created.set(0);
    Serde serde = new Serde(false);
    serde.configure(props);
    assertEquals(0, ClosingStrategy.created.get());
    assertNull(serde.algorithm());
  }

  @Test
  public void cacheBoundsAreCheckedAtConfiguration() {
    // A negative size would fail only on the first record; a TTL below -1 would mean no TTL.
    assertThrows(ConfigException.class,
        () -> config(AbstractKafkaSchemaSerDeConfig.PROVENANCE_CACHE_SIZE, -1));
    assertThrows(ConfigException.class,
        () -> config(AbstractKafkaSchemaSerDeConfig.PROVENANCE_CACHE_TTL, -2));
    assertEquals(0, config(AbstractKafkaSchemaSerDeConfig.PROVENANCE_CACHE_SIZE, 0)
        .getProvenanceCacheSize());
    assertEquals(-1, config(AbstractKafkaSchemaSerDeConfig.PROVENANCE_CACHE_TTL, -1)
        .getProvenanceCacheTtl());
  }

  private static AbstractKafkaSchemaSerDeConfig config(String name, int value) {
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put(name, value);
    return new AbstractKafkaSchemaSerDeConfig(AbstractKafkaSchemaSerDeConfig.baseConfigDef(),
        props);
  }

  private static AbstractKafkaSchemaSerDeConfig config(String algorithm) {
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    if (algorithm != null) {
      props.put(AbstractKafkaSchemaSerDeConfig.PROVENANCE_ALGORITHM, algorithm);
    }
    return new AbstractKafkaSchemaSerDeConfig(AbstractKafkaSchemaSerDeConfig.baseConfigDef(),
        props);
  }

  /** Counts how often any instance is created and closed. */
  public static class ClosingStrategy extends ClientProvenanceStrategy {

    static final AtomicInteger created = new AtomicInteger();
    static final AtomicInteger closed = new AtomicInteger();

    public ClosingStrategy() {
      created.incrementAndGet();
    }

    @Override
    public void close() {
      closed.incrementAndGet();
    }
  }

  /** A serde configured as a deserializer, or a serializer, is, against a mock registry. */
  private static final class Serde extends AbstractKafkaSchemaSerDe {

    private final boolean reads;

    Serde(boolean reads) {
      this.reads = reads;
      schemaRegistry = new MockSchemaRegistryClient();
    }

    @Override
    protected boolean readsByProvenance() {
      return reads;
    }

    String algorithm() {
      return provenanceAlgorithm;
    }

    void configure(Map<String, Object> props) {
      configureClientProperties(new AbstractKafkaSchemaSerDeConfig(
          AbstractKafkaSchemaSerDeConfig.baseConfigDef(), props), new AvroSchemaProvider());
    }
  }

  /** Remembers the configuration it was given. */
  public static class ConfiguredStrategy extends ClientProvenanceStrategy {

    Map<String, ?> configs;

    @Override
    public void configure(Map<String, ?> configs) {
      this.configs = configs;
    }
  }
}
