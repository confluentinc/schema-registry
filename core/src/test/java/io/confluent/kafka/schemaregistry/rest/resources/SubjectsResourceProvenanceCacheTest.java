/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest.resources;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** What the provenance cache keeps, and what an entry weighs. */
class SubjectsResourceProvenanceCacheTest {

  @Test
  void aRangesOwnAnswerIsKeptAndAFailureThatMayPassIsNot() {
    // The key pins every version of the range, so a 422 cannot change for it.
    assertTrue(SubjectsResource.cachesFailure(Errors.provenanceTooLargeException("x")));
    assertTrue(SubjectsResource.cachesFailure(Errors.ambiguousProvenanceException("x")));
    assertTrue(SubjectsResource.cachesFailure(Errors.recursiveSchemaException("x")));
    assertTrue(SubjectsResource.cachesFailure(
        Errors.invalidSchemaException(new IllegalArgumentException("x"))));
    // A version's references are read from the store, and the store answers again.
    assertFalse(SubjectsResource.cachesFailure(Errors.unresolvableReferenceException("x")));
    assertFalse(SubjectsResource.cachesFailure(
        Errors.schemaRegistryException("x", new IllegalStateException("x"))));
  }

  @Test
  void anEntryWeighsAtLeastTheVersionsItsKeyPins() {
    // A 200-version ends-only answer: a 604-element key and a handful of locations.
    assertTrue(SubjectsResource.cacheWeight(Collections.nCopies(604, 0), 5) >= 604);
    // A failure counts one location, but its key still pins the range.
    assertTrue(SubjectsResource.cacheWeight(Collections.nCopies(604, 0), 1) >= 604);
  }
}
