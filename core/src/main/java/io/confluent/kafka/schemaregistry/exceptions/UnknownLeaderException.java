/*
 * Copyright 2014-2025 Confluent Inc.
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
 * Indicates that the node that is asked to serve the request is not the current leader and
 * is not aware of the leader node to forward the request to
 */
public class UnknownLeaderException extends SchemaRegistryException {

  public UnknownLeaderException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnknownLeaderException(String message) {
    super(message);
  }

  public UnknownLeaderException(Throwable cause) {
    super(cause);
  }

  public UnknownLeaderException() {
    super();
  }
}
