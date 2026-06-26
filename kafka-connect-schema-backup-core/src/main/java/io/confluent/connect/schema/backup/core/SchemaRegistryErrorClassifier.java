/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.schema.backup.core;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import org.apache.kafka.common.KafkaException;

/**
 * Classifies Schema Registry errors using the same logic as the converters.
 *
 * <p>Reuses {@link AbstractKafkaSchemaSerDe#toKafkaException} to classify
 * {@link RestClientException} by HTTP status code into retriable vs fatal
 * exceptions. Same classification used by AvroConverter, ProtobufConverter,
 * and JsonSchemaConverter.
 */
class SchemaRegistryErrorClassifier extends AbstractKafkaSchemaSerDe {

  static KafkaException classify(RestClientException e, String msg) {
    return toKafkaException(e, msg);
  }
}
