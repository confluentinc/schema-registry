/*
 * Copyright 2014-2025 Confluent Inc.
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

package io.confluent.kafka.serializers.subject;

import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy;

/**
 * Default {@link io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy}:
 * simply uses the reference name as the subject name.
 */
public class DefaultReferenceSubjectNameStrategy implements ReferenceSubjectNameStrategy {

  @Override
  public void configure(Map<String, ?> config) {
  }

  @Override
  public String subjectName(String refName, String topic, boolean isKey, ParsedSchema schema) {
    return refName;
  }
}
