/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage.encoder;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class KeysetWrapperSerde implements Serde<KeysetWrapper> {

  private final Serde<KeysetWrapper> inner;

  public KeysetWrapperSerde(SchemaRegistryConfig config) {
    inner = Serdes
        .serdeFrom(new KeysetWrapperSerializer(config), new KeysetWrapperDeserializer(config));
  }

  @Override
  public Serializer<KeysetWrapper> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<KeysetWrapper> deserializer() {
    return inner.deserializer();
  }

  @Override
  public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
    inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
    inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }
}