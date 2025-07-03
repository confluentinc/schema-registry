/*
 * Copyright 2017-2018 Confluent Inc.
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

package io.confluent.kafka.streams.serdes.avro;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConfigurationUtilsTest {

  @Test
  public void shouldEnableSpecificAvroWhenSettingIsMissing() {
    // Given
    Map<String, Object> config = new HashMap<>();

    // When
    Map<String, Object> updatedConfig =
        ConfigurationUtils.withSpecificAvroEnabled(config);

    // Then
    assertThat(
        (boolean) updatedConfig.get(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG),
        is(true));
  }

  @Test
  public void shouldEnableSpecificAvroWhenSettingIsFalse() {
    // Given
    Map<String, Object> config = new HashMap<>();
    config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

    // When
    Map<String, Object> updatedConfig =
        ConfigurationUtils.withSpecificAvroEnabled(config);

    // Then
    assertThat(
        (boolean) updatedConfig.get(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG),
        is(true));
  }

  @Test
  public void shouldEnableSpecificAvroWhenConfigIsNull() {
    // Given/when
    Map<String, Object> updatedConfig =
        ConfigurationUtils.withSpecificAvroEnabled(null);

    // Then
    assertThat(
        (boolean) updatedConfig.get(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG),
        is(true));
  }

}