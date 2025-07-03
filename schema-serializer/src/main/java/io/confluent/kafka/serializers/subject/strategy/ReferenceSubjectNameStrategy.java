/*
 * Copyright 2018-2025 Confluent Inc.
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

/**
 * A {@link io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy}
 * is used by the serializer to determine the subject name under which the referenced schema
 * should be registered in the schema registry.
 * The default is to simply use the reference name as the subject name.
 */
public interface ReferenceSubjectNameStrategy extends Configurable {

  /**
   * For a given reference name, topic, and message, returns the subject name under which the
   * referenced schema should be registered in the schema registry.
   *
   * @param refName The name of the reference.
   * @param topic The Kafka topic name to which the message is being published.
   * @param isKey True when encoding a message key, false for a message value.
   * @param schema The referenced schema.
   * @return The subject name under which the referenced schema should be registered.
   */
  String subjectName(String refName, String topic, boolean isKey, ParsedSchema schema);

}
