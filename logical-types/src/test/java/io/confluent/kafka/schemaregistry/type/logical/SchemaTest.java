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

package io.confluent.kafka.schemaregistry.type.logical;

import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for Schema model-level validation invariants. */
class SchemaTest {

  // STRUCT field-name uniqueness — pre-existing invariant; pinned here for completeness.
  @Test
  void testDuplicateStructFieldNameThrows() {
    assertThrows(ValidationException.class, () ->
        Schema.createStruct(Arrays.asList(
            new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0),
            new Field("x", Schema.createString().setNullable(false), 1))));
  }

  // UNION branch-name uniqueness — new invariant.
  @Test
  void testDuplicateUnionBranchNameThrows() {
    assertThrows(ValidationException.class, () ->
        Schema.createUnion(Arrays.asList(
            new UnionBranch("v", Schema.create(Schema.Type.INT).setNullable(false)),
            new UnionBranch("v", Schema.createString().setNullable(false)))));
  }

  // ENUM symbol uniqueness — new invariant.
  @Test
  void testDuplicateEnumSymbolThrows() {
    assertThrows(ValidationException.class, () ->
        Schema.createEnum(Arrays.asList(
            new EnumValue("RED"),
            new EnumValue("GREEN"),
            new EnumValue("RED"))));
  }
}
