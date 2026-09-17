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

package io.confluent.kafka.schemaregistry.exceptions;

/**
 * Thrown when an Associations batchMutate request exceeds one of the configured batch size
 * limits (association count, per-entry payload size, or cumulative payload size).
 */
public class AssociationBatchLimitExceededException extends SchemaRegistryException {

  public AssociationBatchLimitExceededException(String message, Throwable cause) {
    super(message, cause);
  }

  public AssociationBatchLimitExceededException(String message) {
    super(message);
  }

  public AssociationBatchLimitExceededException(Throwable cause) {
    super(cause);
  }

  public AssociationBatchLimitExceededException() {
    super();
  }
}
