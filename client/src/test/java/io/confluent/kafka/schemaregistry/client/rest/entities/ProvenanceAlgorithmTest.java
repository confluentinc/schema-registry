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

import org.junit.Test;

/** Which algorithm a name selects. */
public class ProvenanceAlgorithmTest {

  @Test
  public void aNameSelectsItsVersionInAnyCase() {
    assertEquals(ProvenanceAlgorithm.V1, ProvenanceAlgorithm.of("v1"));
    assertEquals(ProvenanceAlgorithm.V1, ProvenanceAlgorithm.of("V1"));
    assertEquals(ProvenanceAlgorithm.DEFAULT, ProvenanceAlgorithm.of(null));
    assertEquals(ProvenanceAlgorithm.DEFAULT, ProvenanceAlgorithm.of(""));
  }
}
