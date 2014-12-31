/*
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import java.util.Map;

import io.confluent.kafka.schemaregistry.rest.entities.Schema;

public class KafkaStoreMessageHandler implements StoreUpdateHandler<String, Schema> {

  public KafkaStoreMessageHandler() {
  }

  /**
   * Invoked on every new schema written to the Kafka store
   *
   * @param key    Key associated with the schema. Key is in the form SubjectSEPARATORVersion
   * @param schema Schema written to the Kafka store
   */
  @Override
  public void handleUpdate(String key, Schema schema) {
  }
}
