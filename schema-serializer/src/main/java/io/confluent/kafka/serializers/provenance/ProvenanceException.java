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

import io.confluent.kafka.serializers.provenance.strategy.ProvenanceStrategy;

/**
 * Why provenance for a pairing could not be had, as a {@link ProvenanceStrategy} throws it: each
 * subclass says what the record gets.
 */
public abstract class ProvenanceException extends RuntimeException {

  protected ProvenanceException(String message) {
    super(message);
  }

  protected ProvenanceException(String message, Throwable cause) {
    super(message, cause);
  }
}
