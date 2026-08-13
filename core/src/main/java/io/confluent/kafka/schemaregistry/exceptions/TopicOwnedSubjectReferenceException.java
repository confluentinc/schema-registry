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
 * Indicates a schema reference violates a topic-owned subject rule -- e.g. referencing a
 * topic-owned subject, whose lifecycle follows its topic and would be blocked from deletion
 * when the topic is deleted.
 */
public class TopicOwnedSubjectReferenceException extends SchemaRegistryException {

  public TopicOwnedSubjectReferenceException(String message, Throwable cause) {
    super(message, cause);
  }

  public TopicOwnedSubjectReferenceException(String message) {
    super(message);
  }

  public TopicOwnedSubjectReferenceException(Throwable cause) {
    super(cause);
  }

  public TopicOwnedSubjectReferenceException() {
    super();
  }
}
