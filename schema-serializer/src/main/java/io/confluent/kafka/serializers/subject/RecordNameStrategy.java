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

import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;

/**
 * For any record type that is published to Kafka, registers the schema
 * in the registry under the fully-qualified record name (regardless of the
 * topic). This strategy allows a topic to contain a mixture of different
 * record types, since no intra-topic compatibility checking is performed.
 * Instead, checks compatibility of any occurrences of the same record name
 * across <em>all</em> topics.
 */
public class RecordNameStrategy implements SubjectNameStrategy,
    io.confluent.kafka.serializers.subject.SubjectNameStrategy {

  @Override
  public void configure(Map<String, ?> config) {
  }

  @Override
  public boolean usesSchema() {
    return true;
  }

  @Override
  public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
    if (schema == null) {
      return null;
    }
    return getRecordName(schema, isKey);
  }

  /**
   * If the schema is a record type, returns its fully-qualified name.
   * Otherwise throws an error.
   */
  protected String getRecordName(ParsedSchema schema, boolean isKey) {
    String name = schema.name();
    if (name != null) {
      return name;
    }

    // isKey is only used to produce more helpful error messages
    if (isKey) {
      throw new SerializationException("In configuration "
          + AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY + " = "
          + getClass().getName() + ", the message key must only be a record schema");
    } else {
      throw new SerializationException("In configuration "
          + AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY + " = "
          + getClass().getName() + ", the message value must only be a record schema");
    }
  }

  @Override
  @Deprecated
  public String getSubjectName(String topic, boolean isKey, Object value) {
    return subjectName(topic, isKey, new AvroSchema(AvroSchemaUtils.getSchema(value)));
  }
}
