/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.formatter;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import io.confluent.kafka.schemaregistry.ParsedSchema;

public interface SchemaMessageSerializer<T> {

  Serializer getKeySerializer();

  byte[] serializeKey(String topic, Headers headers, Object payload);

  byte[] serialize(
      String subject,
      String topic,
      boolean isKey,
      Headers headers,
      T object,
      ParsedSchema schema
  );
}
