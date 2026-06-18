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
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Combinatorial union-shape mapping test, ported from Flink's
 * {@code AvroToFlinkSchemaConverterTest#testUnionTypeMapping} with the
 * inlined {@code UnionUtil.SchemaProvider}.
 *
 * <p>Generates unions of every (A, B) pair from {@link AvroMappingsV1#getNotNull()}
 * (~24 × 23 ordered pairs) and adds NULL at positions 0/1/2 (~× 4). Each pair
 * is verified to read back to the equivalent LT shape.
 */
class AvroReaderUnionTest {

  static Stream<Arguments> unionMappings() {
    Stream<UnionMapping> unionsWithoutNull =
        createUnionTypeMappings(AvroMappingsV1.getNotNull());
    Stream<UnionMapping> unionsWithNull =
        IntStream.range(0, 3)
            .mapToObj(i ->
                createUnionTypeMappings(AvroMappingsV1.getNotNull())
                    .map(mapping -> addNullToUnion(mapping, i)))
            .flatMap(Function.identity());
    return Stream.concat(unionsWithoutNull, unionsWithNull).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("unionMappings")
  void testUnionTypeMapping(UnionMapping mapping) {
    assertThat(AvroToLogicalTypeConverter.toRootSchema(
            new AvroSchema(mapping.avroSchema)))
        .isEqualTo(mapping.logicalType);
  }

  // ---------- helpers ----------

  /**
   * Holds an Avro union schema and the LT struct it should be read back as.
   * Distinct from {@link AvroMappingsV1.TypeMapping} because that pairs
   * V1-emission outputs (Avro from LT), not reader inputs (LT from Avro).
   */
  static final class UnionMapping {
    final org.apache.avro.Schema avroSchema;
    final Schema logicalType;

    UnionMapping(org.apache.avro.Schema avroSchema, Schema logicalType) {
      this.avroSchema = avroSchema;
      this.logicalType = logicalType;
    }

    @Override
    public String toString() {
      return "union(" + avroSchema + ")";
    }
  }

  /**
   * Cross-product over a list of single-type mappings, producing every
   * 2-member union pair. Filters out:
   *   - same-type pairs (Avro forbids unions of identical types)
   *   - pairs that resolve to the same LT type (would collapse on read)
   *   - pairs where either side is a record/STRUCT (LT's reader recovers
   *     records as NAMED_TYPE_REF, not inline STRUCT — verified in
   *     {@link LogicalTypeV1RoundTripTest}; including them here would
   *     require named-type-aware expected construction, out of scope).
   */
  static Stream<UnionMapping> createUnionTypeMappings(
      List<AvroMappingsV1.TypeMapping> singles) {
    return singles.stream()
        .filter(t -> t.getLogicalType().getType() != Schema.Type.STRUCT)
        .flatMap(first -> singles.stream()
            .filter(t -> t.getLogicalType().getType() != Schema.Type.STRUCT)
            .filter(second -> !first.getAvroSchema().getFullName()
                .equals(second.getAvroSchema().getFullName()))
            .filter(second -> !first.getLogicalType().equals(second.getLogicalType()))
            .map(second -> createUnionMapping(first, second)));
  }

  private static UnionMapping createUnionMapping(
      AvroMappingsV1.TypeMapping first, AvroMappingsV1.TypeMapping second) {
    org.apache.avro.Schema unionSchema = org.apache.avro.Schema.createUnion(
        first.getAvroSchema(), second.getAvroSchema());
    // LT reader contract: branches keep their declared nullability (LT does
    // not auto-mark proper-union branches as nullable, unlike Flink). Outer
    // UNION is non-nullable since no NULL member is present.
    Schema ltUnion = Schema.createUnion(Arrays.asList(
        new UnionBranch(first.getAvroSchema().getName(), first.getLogicalType()),
        new UnionBranch(second.getAvroSchema().getName(), second.getLogicalType())))
        .setNullable(false);
    return new UnionMapping(unionSchema, ltUnion);
  }

  /**
   * Insert NULL at the given position in the union members. Per Avro+LT
   * semantics, adding NULL to a proper union doesn't change the union's
   * shape — it just makes the wrapping nullable. The non-null branches stay
   * in their original order; the outer LT UNION becomes nullable.
   */
  static UnionMapping addNullToUnion(UnionMapping mapping, int position) {
    List<org.apache.avro.Schema> unionTypes =
        new ArrayList<>(mapping.avroSchema.getTypes());
    unionTypes.add(position, org.apache.avro.Schema.create(
        org.apache.avro.Schema.Type.NULL));
    return new UnionMapping(
        org.apache.avro.Schema.createUnion(unionTypes),
        mapping.logicalType.setNullable(true));
  }
}
