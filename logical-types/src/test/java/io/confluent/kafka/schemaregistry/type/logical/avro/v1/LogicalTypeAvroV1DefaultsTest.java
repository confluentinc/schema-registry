/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.type.logical.avro.v1;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroDefaultValueConverter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Avro default-value extraction round-trip, ported from Flink's
 * {@code AvroDefaultsToFlinkDataConverterTest}. Verifies that LT's
 * {@link AvroDefaultValueConverter#toJavaData} produces the same Java
 * representations Flink's reader did, for every type Flink's tests cover.
 */
class LogicalTypeAvroV1DefaultsTest {

  @Test
  void testDecimalTypes() {
    assertThat(AvroDefaultValueConverter.toJavaData(
            Schema.createDecimal(4, 2), new byte[]{0x01, 0x02}))
        .isEqualTo(BigDecimal.valueOf(258, 2));
    assertThat(AvroDefaultValueConverter.toJavaData(
            Schema.createDecimal(10, 4), new byte[]{0x01, 0x02}))
        .isEqualTo(BigDecimal.valueOf(258, 4));
  }

  @Test
  void testLocalZonedTimestampTypes() {
    final Instant tsInstant = Instant.ofEpochMilli(1741263480000L);

    // timestamp_ltz in millis
    assertThat(AvroDefaultValueConverter.toJavaData(
            Schema.createTimestampLtz(3), 1741263480000L))
        .isEqualTo(tsInstant);
    // timestamp_ltz in micros
    assertThat(AvroDefaultValueConverter.toJavaData(
            Schema.createTimestampLtz(6), 1741263480000000L))
        .isEqualTo(tsInstant);
  }

  @Test
  void testTimestampTypes() {
    final LocalDateTime ts = LocalDateTime.ofInstant(
        Instant.ofEpochMilli(1741263480000L), ZoneOffset.UTC);

    // timestamp in millis
    assertThat(AvroDefaultValueConverter.toJavaData(
            Schema.createTimestamp(3), 1741263480000L))
        .isEqualTo(ts);
    // timestamp in micros
    assertThat(AvroDefaultValueConverter.toJavaData(
            Schema.createTimestamp(6), 1741263480000000L))
        .isEqualTo(ts);
  }

  @Test
  void testDateTypes() {
    final Timestamp ts = Timestamp.valueOf("2025-03-06 06:18:00.000000");
    final LocalDate date = ts.toLocalDateTime().toLocalDate();

    assertThat(AvroDefaultValueConverter.toJavaData(
            Schema.create(Schema.Type.DATE), date.toEpochDay()))
        .isEqualTo(date);
  }

  @Test
  void testTimeTypes() {
    // Flink's test passes raw int 1000 → 1 ms (1_000_000 nanos). The
    // conversion treats the value as micros, which corresponds to LT's
    // micros path (precision 4..9, Long input).
    assertThat(AvroDefaultValueConverter.toJavaData(
            Schema.createTime(6), 1000L))
        .isEqualTo(LocalTime.ofNanoOfDay(1_000_000L));
  }

  @Test
  void testArrayTypes() {
    Schema arrayOfInt = Schema.createArray(
        Schema.create(Schema.Type.INT).setNullable(false));
    assertThat(AvroDefaultValueConverter.toJavaData(
            arrayOfInt, new int[]{1, 2, 3}))
        .isEqualTo(List.of(1, 2, 3));
    assertThat(AvroDefaultValueConverter.toJavaData(
            arrayOfInt, List.of(1, 2, 3)))
        .isEqualTo(List.of(1, 2, 3));

    Schema arrayOfDecimal = Schema.createArray(Schema.createDecimal(4, 2));
    assertThat(AvroDefaultValueConverter.toJavaData(
            arrayOfDecimal, new byte[][]{new byte[]{0x01, 0x02}}))
        .isEqualTo(List.of(BigDecimal.valueOf(258, 2)));
  }

  @Test
  void testMapTypes() {
    Schema mapStringInt = Schema.createMap(
        Schema.createString(), Schema.create(Schema.Type.INT));
    assertThat(AvroDefaultValueConverter.toJavaData(
            mapStringInt, Map.of("a", 1)))
        .isEqualTo(Map.of("a", 1));

    Schema mapStringDecimal = Schema.createMap(
        Schema.createString(), Schema.createDecimal(5, 3));
    assertThat(AvroDefaultValueConverter.toJavaData(
            mapStringDecimal, Map.of("a", new byte[]{0x01, 0x02})))
        .isEqualTo(Map.of("a", BigDecimal.valueOf(258, 3)));
  }
}
