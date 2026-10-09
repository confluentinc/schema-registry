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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Which algorithm a name, or under dynamic a registration time, selects. */
public class ProvenanceAlgorithmTest {

  @Test
  public void v1IsEffectiveFromTheEpoch() {
    assertEquals(0L, ProvenanceAlgorithm.V1.getEffectiveTimestamp());
    assertEquals(ProvenanceAlgorithm.V1, ProvenanceAlgorithm.effectiveAt(0L));
    assertEquals(ProvenanceAlgorithm.V1, ProvenanceAlgorithm.effectiveAt(System.currentTimeMillis()));
    // A version with no recorded registration time counts as the epoch.
    assertEquals(ProvenanceAlgorithm.V1, ProvenanceAlgorithm.effectiveAt(null));
  }

  @Test
  public void dynamicIsANameNotAVersion() {
    assertTrue(ProvenanceAlgorithm.isDynamic("dynamic"));
    assertTrue(ProvenanceAlgorithm.isDynamic("DYNAMIC"));
    assertFalse(ProvenanceAlgorithm.isDynamic("v1"));
    assertFalse(ProvenanceAlgorithm.isDynamic(null));
    assertThrows(IllegalArgumentException.class, () -> ProvenanceAlgorithm.of("dynamic"));
  }
}
