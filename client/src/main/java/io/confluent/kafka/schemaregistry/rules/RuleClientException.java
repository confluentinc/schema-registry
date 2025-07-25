/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules;

/**
 * A rule exception that occurs when the client interacts with a remote service.
 */
public class RuleClientException extends RuleException {

  public RuleClientException() {
  }

  public RuleClientException(Throwable cause) {
    super(cause);
  }

  public RuleClientException(String message) {
    super(message);
  }

  public RuleClientException(String message, Throwable cause) {
    super(message, cause);
  }
}
