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

/**
 * There is no provenance to project a writer by, or none a single schema can express: the record
 * is read as it would be without provenance.
 */
public class ProvenanceUnavailableException extends RuntimeException {

  public ProvenanceUnavailableException(String message) {
    super(message);
  }

  public ProvenanceUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
