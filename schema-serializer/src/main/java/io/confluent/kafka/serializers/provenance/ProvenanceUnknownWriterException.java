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
 * The writer's schema id is no version of the subject: the writer is matched to one by structure,
 * and provenance asked for again.
 */
public class ProvenanceUnknownWriterException extends ProvenanceException {

  public ProvenanceUnknownWriterException(String message) {
    super(message);
  }

  public ProvenanceUnknownWriterException(String message, Throwable cause) {
    super(message, cause);
  }
}
