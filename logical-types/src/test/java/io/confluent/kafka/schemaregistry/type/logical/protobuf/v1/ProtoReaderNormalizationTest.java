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

package io.confluent.kafka.schemaregistry.type.logical.protobuf.v1;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that LT's Proto reader produces the same LT Schema before and
 * after {@link ProtobufSchema#normalize}. Ported from Flink's
 * {@code ProtoToFlinkSchemaConverterTest#testTypeMappingAfterNormalization}.
 *
 * <p>Catches normalize-induced reader drift: if normalize() reorders fields,
 * fully-qualifies type names, or drops information the reader uses, this
 * fires.
 */
class ProtoReaderNormalizationTest {

  static Stream<Arguments> typesToCheck() {
    return ProtoMappingsV1.get().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("typesToCheck")
  void testTypeMappingAfterNormalization(ProtoMappingsV1.TypeMapping mapping) {
    ProtobufSchema original = new ProtobufSchema(mapping.getExpectedProto());
    String normalized = original.normalize().canonicalString();
    ProtobufSchema reparsed = new ProtobufSchema(normalized);

    Schema before = ProtoToLogicalTypeConverter.toRootSchema(original);
    Schema after = ProtoToLogicalTypeConverter.toRootSchema(reparsed);

    assertThat(after).isEqualTo(before);
  }
}
