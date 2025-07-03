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

package io.confluent.connect.avro;

import io.confluent.kafka.serializers.subject.SubjectNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;

import java.util.Map;

public class DeprecatedTestTopicNameStrategy implements SubjectNameStrategy {
  private final TopicNameStrategy topicNameStrategy = new TopicNameStrategy();

  @Override
  public String getSubjectName(String topic, boolean isKey, Object value) {
    return topicNameStrategy.subjectName(topic, isKey, null);
  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
