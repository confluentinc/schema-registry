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

/**
 * For any record type that is published to Kafka topic &lt;topic&gt;,
 * registers the schema in the registry under the subject name
 * &lt;topic&gt;-&lt;recordName&gt;, where &lt;recordName&gt; is the
 * fully-qualified record name. This strategy allows a topic to contain
 * a mixture of different record types, since no intra-topic compatibility
 * checking is performed. Moreover, different topics may contain mutually
 * incompatible versions of the same record name, since the compatibility
 * check is scoped to a particular record name within a particular topic.
 */
public class TopicRecordNameStrategy extends RecordNameStrategy {

  @Override
  public boolean usesSchema() {
    return true;
  }

  @Override
  public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
    if (topic == null || schema == null) {
      return null;
    }
    return topic + "-" + getRecordName(schema, isKey);
  }
}
