/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.serializers.subject.strategy;

import org.apache.kafka.common.Configurable;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;

/**
 * A {@link SubjectNameStrategy} is used by the serializer to determine
 * the subject name under which the event record schemas should be registered
 * in the schema registry. The default is {@link TopicNameStrategy}.
 */
public interface SubjectNameStrategy extends Configurable {

  /**
   * Whether the strategy depends on the schema.
   *
   * @return Whether the strategy depends on the schema.
   */
  default boolean usesSchema() {
    return false;
  }

  /**
   * For a given topic and message, returns the subject name under which the
   * schema should be registered in the schema registry.
   *
   * @param topic The Kafka topic name to which the message is being published.
   * @param isKey True when encoding a message key, false for a message value.
   * @param schema the schema of the record being serialized/deserialized
   * @return The subject name under which the schema should be registered.
   */
  String subjectName(String topic, boolean isKey, ParsedSchema schema);

}
