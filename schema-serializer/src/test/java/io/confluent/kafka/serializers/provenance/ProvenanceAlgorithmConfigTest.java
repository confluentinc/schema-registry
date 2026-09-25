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

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

/** A misspelt provenance algorithm fails configuration rather than quietly turning it off. */
public class ProvenanceAlgorithmConfigTest {

  @Test
  public void aKnownAlgorithmOrNoneIsAccepted() {
    assertEquals("v1", config("v1").getProvenanceAlgorithm());
    assertEquals("V1", config("V1").getProvenanceAlgorithm());
    assertNull(config("none").getProvenanceAlgorithm());
    assertNull(config("None").getProvenanceAlgorithm());
    assertNull(config("").getProvenanceAlgorithm());
    assertNull(config(null).getProvenanceAlgorithm());
  }

  @Test
  public void anUnknownAlgorithmFailsConfiguration() {
    assertThrows(ConfigException.class, () -> config("vI"));
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
}
