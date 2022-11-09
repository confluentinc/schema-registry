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

package io.confluent.kafka.serializers.subject;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import java.util.Map;

/**
 * Default {@link SubjectNameStrategy}: for any messages published to
 * &lt;topic&gt;, the schema of the message key is registered under
 * the subject name &lt;topic&gt;-key, and the message value is registered
 * under the subject name &lt;topic&gt;-value.
 */
public class TopicNameStrategy implements SubjectNameStrategy,
    io.confluent.kafka.serializers.subject.SubjectNameStrategy {

  @Override
  public void configure(Map<String, ?> config) {
  }

  @Override
  public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
    if (topic == null) {
      return null;
    }
    return isKey ? topic + "-key" : topic + "-value";
  }

  @Override
  @Deprecated
  public String getSubjectName(String topic, boolean isKey, Object value) {
    return subjectName(topic, isKey, null);
  }
}
