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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that LT's Avro reader extracts per-field default values into the
 * path-keyed {@link LogicalType#getDefaultValues()} map. Ported from Flink's
 * {@code AvroToFlinkSchemaConverterTest#testRecordWithDefaultValues} and
 * {@code testNullDefaultValue}.
 */
class AvroReaderDefaultsTest {

  /**
   * {@code default: null} on a nullable union field is intentionally NOT
   * registered in the defaults map — it can't be distinguished from "no
   * default" once round-tripped through serialization.
   */
  @Test
  void testNullDefaultValue() {
    String avroSchemaString =
        "{\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"default\": null,\n"
            + "      \"name\": \"i\",\n"
            + "      \"type\": [\n"
            + "        \"null\",\n"
            + "        \"int\"\n"
            + "      ]\n"
            + "    }\n"
            + "  ],\n"
            + "  \"name\": \"a1_value\",\n"
            + "  \"namespace\": \"org.apache.flink.avro.generated.record\",\n"
            + "  \"type\": \"record\"\n"
            + "}";
    LogicalType lt = AvroToLogicalTypeConverter.toLogicalType(
        new AvroSchema(avroSchemaString));
    assertThat(lt.getDefaultValues()).isEmpty();
  }

  /**
   * Mega-schema with nested records, arrays, maps, and per-field defaults
   * across every primitive + composite type. Asserts the path-keyed default
   * values map matches Flink's expected.
   */
  @Test
  void testRecordWithDefaultValues() {
    final String avroString =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"topLevelRecord\",\n"
            + "  \"fields\":\n"
            + "  [\n"
            + "    {\n"
            + "      \"name\": \"value\",\n"
            + "      \"type\":\n"
            + "      [\n"
            + "        {\n"
            + "          \"type\": \"record\",\n"
            + "          \"name\": \"primitives\",\n"
            + "          \"namespace\": \"io.test\",\n"
            + "          \"fields\":\n"
            + "          [\n"
            + "            { \"name\": \"f_bool\", \"type\": \"boolean\", \"default\": true },\n"
            + "            { \"name\": \"f_int\", \"type\": \"int\", \"default\": 1024 },\n"
            + "            { \"name\": \"f_long\", \"type\": \"long\", \"default\": 4096 },\n"
            + "            { \"name\": \"f_float\", \"type\": \"float\", \"default\": 1.024 },\n"
            + "            { \"name\": \"f_double\", \"type\": \"double\", \"default\": 4.096 },\n"
            + "            { \"name\": \"f_string\", \"type\": \"string\", \"default\": \"earth\" },\n"
            + "            { \"name\": \"f_bytes\", \"type\": \"bytes\", \"default\": \"\u00aa\" },\n"
            + "            {\n"
            + "              \"name\": \"f_fixed\",\n"
            + "              \"type\": {\"type\": \"fixed\", \"name\": \"UUID\", \"size\": 3},\n"
            + "              \"default\": \"\uaabbCC\"\n"
            + "            },\n"
            + "            {\n"
            + "              \"name\": \"f_decimal\",\n"
            + "              \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2},\n"
            + "              \"default\": \"\u00ea\"\n"
            + "            },\n"
            + "            {\n"
            + "              \"name\": \"f_date\",\n"
            + "              \"type\": { \"logicalType\": \"date\", \"type\": \"int\" },\n"
            + "              \"default\": 20151\n"
            + "            },\n"
            + "            {\n"
            + "              \"name\": \"f_time\",\n"
            + "              \"type\": {\n"
            + "                \"flink.precision\": 0,\n"
            + "                \"flink.version\": \"1\",\n"
            + "                \"logicalType\": \"time-millis\",\n"
            + "                \"type\": \"int\"\n"
            + "              },\n"
            + "              \"default\": 43320000\n"
            + "            },\n"
            + "            {\n"
            + "              \"name\": \"f_timestamp_millis\",\n"
            + "              \"type\": { \"logicalType\": \"local-timestamp-millis\", \"type\": \"long\" },\n"
            + "              \"default\": 1741263480000\n"
            + "            },\n"
            + "            {\n"
            + "              \"name\": \"f_timestamp_micros\",\n"
            + "              \"type\": { \"logicalType\": \"local-timestamp-micros\", \"type\": \"long\" },\n"
            + "              \"default\": 1741263480000000\n"
            + "            },\n"
            + "            {\n"
            + "              \"name\": \"f_tlz_millis\",\n"
            + "              \"type\": { \"logicalType\": \"timestamp-millis\", \"type\": \"long\" },\n"
            + "              \"default\": 1741263480000\n"
            + "            },\n"
            + "            {\n"
            + "              \"name\": \"f_tlz_micros\",\n"
            + "              \"type\": { \"logicalType\": \"timestamp-micros\", \"type\": \"long\" },\n"
            + "              \"default\": 1741263480000000\n"
            + "            }\n"
            + "          ]\n"
            + "        }, {\n"
            + "          \"type\": \"record\",\n"
            + "          \"name\": \"collections\",\n"
            + "          \"namespace\": \"io.test\",\n"
            + "          \"fields\":\n"
            + "          [\n"
            + "            {\n"
            + "              \"name\": \"f_array_decimals\",\n"
            + "              \"type\": {\n"
            + "                \"type\": \"array\",\n"
            + "                \"items\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2}\n"
            + "              },\n"
            + "              \"default\": [\"C\", \"?\"]\n"
            + "            },\n"
            + "            {\n"
            + "              \"name\": \"f_map_float\",\n"
            + "              \"type\": { \"type\": \"map\", \"values\": \"float\" },\n"
            + "              \"default\": {\"a\": 1.0, \"b\": 2.0}\n"
            + "            },\n"
            + "            {\n"
            + "              \"name\": \"f_row\",\n"
            + "              \"default\": {\"f1\": 1},\n"
            + "              \"type\": {\n"
            + "                \"name\": \"F_row\",\n"
            + "                \"type\": \"record\",\n"
            + "                \"fields\": [{\"name\": \"f1\", \"type\": \"int\", \"default\": 100}]\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"default\": null,\n"
            + "              \"name\": \"f_array_row\",\n"
            + "              \"type\": [\n"
            + "                \"null\",\n"
            + "                {\n"
            + "                  \"items\": [\n"
            + "                    \"null\",\n"
            + "                    {\n"
            + "                      \"fields\": [\n"
            + "                        {\"default\": \"jinjer\", \"name\": \"name\", \"type\": \"string\"}\n"
            + "                      ],\n"
            + "                      \"name\": \"t1_value_b\",\n"
            + "                      \"type\": \"record\"\n"
            + "                    }\n"
            + "                  ],\n"
            + "                  \"type\": \"array\"\n"
            + "                }\n"
            + "              ]\n"
            + "            },\n"
            + "            {\n"
            + "              \"default\": null,\n"
            + "              \"name\": \"f_map_array\",\n"
            + "              \"type\": [\n"
            + "                \"null\",\n"
            + "                {\n"
            + "                  \"type\": \"map\",\n"
            + "                  \"values\": [\n"
            + "                    \"null\",\n"
            + "                    {\n"
            + "                      \"fields\": [\n"
            + "                        {\"default\": 100, \"name\": \"id\", \"type\": \"int\"}\n"
            + "                      ],\n"
            + "                      \"name\": \"t1_value_c\",\n"
            + "                      \"type\": \"record\"\n"
            + "                    }\n"
            + "                  ]\n"
            + "                }\n"
            + "              ]\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    LogicalType lt = AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(avroString));
    Map<List<Integer>, Object> defaults = lt.getDefaultValues();

    // primitives: path is [value-field, primitives-union-member, field-pos]
    assertThat(defaults).containsEntry(List.of(0, 0, 0), true);
    assertThat(defaults).containsEntry(List.of(0, 0, 1), 1024);
    assertThat(defaults).containsEntry(List.of(0, 0, 2), 4096L);
    assertThat(defaults).containsEntry(List.of(0, 0, 3), 1.024f);
    assertThat(defaults).containsEntry(List.of(0, 0, 4), 4.096d);
    assertThat(defaults).containsEntry(List.of(0, 0, 5), "earth");
    assertThat((byte[]) defaults.get(List.of(0, 0, 6))).containsExactly(-86);
    assertThat((byte[]) defaults.get(List.of(0, 0, 7))).containsExactly(63, 67, 67);

    // decimal
    assertThat(defaults).containsEntry(List.of(0, 0, 8), BigDecimal.valueOf(-22, 2));

    // dates, times & timestamps
    assertThat(defaults).containsEntry(List.of(0, 0, 9), LocalDate.ofEpochDay(20151));
    // Flink's test expected LocalTime.ofNanoOfDay(320_000_000) = 00:00:00.320 which
    // doesn't match any reasonable conversion of 43320000 millis-since-midnight.
    // LT's correct interpretation: 43320000 ms = 12:02:00.000.
    assertThat(defaults).containsEntry(List.of(0, 0, 10),
        LocalTime.ofNanoOfDay(43_320_000L * 1_000_000L));
    assertThat(defaults).containsEntry(List.of(0, 0, 11),
        Timestamp.valueOf("2025-03-06 12:18:00.000000").toLocalDateTime());
    assertThat(defaults).containsEntry(List.of(0, 0, 12),
        Timestamp.valueOf("2025-03-06 12:18:00.000000").toLocalDateTime());

    // ltz timestamps
    final Instant tsInstant = Instant.ofEpochMilli(1741263480000L);
    assertThat(defaults).containsEntry(List.of(0, 0, 13), tsInstant);
    assertThat(defaults).containsEntry(List.of(0, 0, 14), tsInstant);

    // array of decimals
    assertThat((List<BigDecimal>) defaults.get(List.of(0, 1, 0)))
        .containsExactlyInAnyOrderElementsOf(
            List.of(BigDecimal.valueOf(67, 2), BigDecimal.valueOf(63, 2)));

    // map of floats
    assertThat((Map<String, Float>) defaults.get(List.of(0, 1, 1)))
        .containsExactlyInAnyOrderEntriesOf(Map.of("a", 1.0f, "b", 2.0f));

    // nested row + its inner field default
    assertThat((Map<String, Integer>) defaults.get(List.of(0, 1, 2)))
        .containsExactlyInAnyOrderEntriesOf(Map.of("f1", 1));
    assertThat(defaults).containsEntry(List.of(0, 1, 2, 0), 100);

    // nullable array of nullable record + its leaf field default
    assertThat(defaults).containsEntry(List.of(0, 1, 3, 0, 0), "jinjer");

    // nullable map of nullable record + its leaf field default
    assertThat(defaults).containsEntry(List.of(0, 1, 4, 1, 0), 100);
  }
}
