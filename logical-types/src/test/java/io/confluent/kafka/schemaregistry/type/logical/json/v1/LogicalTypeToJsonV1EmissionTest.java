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

package io.confluent.kafka.schemaregistry.type.logical.json.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that LT V1 JSON Schema emission produces the expected JSON for each
 * mapping ported from {@code FlinkToJsonSchemaConverterTest}.
 *
 * <p>Comparison: parse both expected and actual into {@code JsonNode} trees
 * and compare structurally — order of object keys is not load-bearing in
 * JSON Schema.
 */
class LogicalTypeToJsonV1EmissionTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static Stream<Arguments> typesToCheck() {
    return JsonMappingsV1.get().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("typesToCheck")
  void testV1Emission(JsonMappingsV1.TypeMapping mapping) throws Exception {
    JsonSchema actual = LogicalTypeToJsonConverter.fromLogicalType(
        mapping.toLogicalType(), "io.confluent.row", LogicalTypeVersion.V1);
    assertThat(MAPPER.readTree(actual.toString()))
        .isEqualTo(MAPPER.readTree(mapping.getExpectedJson()));
  }
}
