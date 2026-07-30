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

import io.confluent.kafka.schemaregistry.type.logical.CompatibilityChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.Incompatibility.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cases carried over one-for-one from the suite of the equivalent Iceberg-schema checker, which is
 * maintained elsewhere and still live.
 *
 * <p><b>Keep this file diffable against that suite.</b> Method names match it exactly and appear in
 * the same order. When a case is added there, add it here; when a case here starts failing, decide
 * deliberately which implementation is right.
 *
 * <p>Two mechanical translations apply throughout:
 *
 * <ul>
 *   <li>Their Iceberg {@code NestedField.optional/required} become SRLT fields whose schema is
 *       nullable or not. Iceberg field IDs have no SRLT counterpart and are dropped.
 *   <li>Their {@code assertThrows(XException.class)} plus {@code getFieldPath()} become an assertion
 *       on the equivalent {@link Rule} and path, since this checker accumulates findings instead of
 *       throwing. Where they also assert rendered type names ({@code "long"}, {@code "int"}), those
 *       are message details rather than verdicts and are not carried over.
 * </ul>
 *
 * <p>SRLT-specific behaviour with no counterpart there — the type erasure, named-type references,
 * unions, enums, multisets, report-all — is covered in {@link CompatibilityCheckerTest} and is
 * deliberately <em>not</em> synced with theirs.
 */
class CompatibilityCheckerIcebergSchemaTest {

  // ---------------------------------------------------------------------------------------------
  // Translation helpers
  // ---------------------------------------------------------------------------------------------

  private static Schema longType() {
    return Schema.create(Schema.Type.BIGINT);
  }

  private static Schema intType() {
    return Schema.create(Schema.Type.INT);
  }

  private static Schema stringType() {
    return Schema.createString();
  }

  private static Schema binaryType() {
    return Schema.createBytes();
  }

  private static Schema doubleType() {
    return Schema.create(Schema.Type.DOUBLE);
  }

  /** Their {@code NestedField.optional(id, name, type)}. */
  private static Field optional(String name, Schema type) {
    return new Field(name, type.setNullable(true), 0);
  }

  /** Their {@code NestedField.required(id, name, type)}. */
  private static Field required(String name, Schema type) {
    return new Field(name, type.setNullable(false), 0);
  }

  /** Their {@code Types.StructType.of(...)}, as a nullable struct. */
  private static Schema structOf(Field... fields) {
    return Schema.createStruct(Arrays.asList(fields)).setNullable(true);
  }

  /** Their {@code Types.MapType.ofOptional(keyId, valueId, key, value)}. */
  private static Schema mapOfOptional(Schema key, Schema value) {
    return Schema.createMap(key.setNullable(false), value.setNullable(true)).setNullable(true);
  }

  /** Their {@code Types.ListType.ofOptional(elementId, element)}. */
  private static Schema listOfOptional(Schema element) {
    return Schema.createArray(element.setNullable(true)).setNullable(true);
  }

  /** Their {@code new Schema(...)} — the root struct is never nullable. */
  private static LogicalType schema(Field... fields) {
    return new LogicalType(Schema.createStruct(Arrays.asList(fields)).setNullable(false));
  }

  private static CompatibilityResult compare(LogicalType original, LogicalType update) {
    return CompatibilityChecker.compare(Mode.ICEBERG, original, update);
  }

  /** Their {@code assertDoesNotThrow(...)}. */
  private static void assertCompatible(LogicalType original, LogicalType update) {
    CompatibilityResult result = compare(original, update);
    assertTrue(result.isCompatible(), "expected compatible but got: " + result.describe());
  }

  /** Their {@code assertThrows(XException.class)} plus {@code getFieldPath()}. */
  private static void assertIncompatible(
      LogicalType original, LogicalType update, Rule expectedRule, String expectedPath) {
    CompatibilityResult result = compare(original, update);
    List<Incompatibility> matching = result.getIncompatibilities().stream()
        .filter(finding -> finding.getRule() == expectedRule)
        .collect(Collectors.toList());
    assertEquals(1, matching.size(),
        "expected exactly one " + expectedRule + " but got: " + result.describe());
    assertEquals(expectedPath, matching.get(0).getPath(), result.describe());
  }

  /**
   * The internal metadata columns that trail every materialized table, used by the two
   * "Real" cases below.
   */
  private static List<Field> internalMetadataColumns() {
    return Arrays.asList(
        optional("$$topic", stringType()),
        optional("$$partition", intType()),
        optional("$$headers", mapOfOptional(binaryType(), stringType())),
        optional("$$leader-epoch", intType()),
        optional("$$offset", longType()),
        optional("$$timestamp", stringType()),
        optional("$$timestamp-type", stringType()),
        optional("$$raw-key", binaryType()),
        optional("$$raw-value", binaryType()));
  }

  private static LogicalType schemaWithInternalMetadata(Field... userFields) {
    List<Field> fields = new ArrayList<>(Arrays.asList(userFields));
    fields.addAll(internalMetadataColumns());
    return new LogicalType(Schema.createStruct(fields).setNullable(false));
  }

  // ---------------------------------------------------------------------------------------------
  // Ported cases, in their source order
  // ---------------------------------------------------------------------------------------------

  @Test
  void testCompatibleColumnAddition() {
    assertCompatible(
        schema(
            optional("id", longType()),
            optional("name", stringType())),
        schema(
            optional("id", longType()),
            optional("name", stringType()),
            optional("salary", doubleType())));
  }

  @Test
  void testCompatibleColumnAdditionMiddle() {
    // New fields inserted between existing ones must not trip the reordering rule.
    assertCompatible(
        schema(
            optional("id", longType()),
            optional("name", stringType())),
        schema(
            optional("id", longType()),
            optional("salary1", doubleType()),
            optional("salary2", doubleType()),
            optional("name", stringType())));
  }

  @Test
  void testCompatibleColumnAdditionReal() {
    assertCompatible(
        schemaWithInternalMetadata(
            optional("id", longType()),
            optional("name", stringType()),
            optional("age", intType()),
            optional("city", stringType()),
            optional("email", stringType())),
        schemaWithInternalMetadata(
            optional("id", longType()),
            optional("name", stringType()),
            optional("age", intType()),
            optional("city", stringType()),
            optional("email", stringType()),
            optional("hobby", stringType())));
  }

  @Test
  void testCompatibleColumnAdditionRealSecondRound() {
    assertCompatible(
        schemaWithInternalMetadata(
            optional("id", longType()),
            optional("name", stringType()),
            optional("age", intType()),
            optional("city", stringType()),
            optional("email", stringType()),
            optional("hobby0", stringType())),
        schemaWithInternalMetadata(
            optional("id", longType()),
            optional("name", stringType()),
            optional("age", intType()),
            optional("city", stringType()),
            optional("email", stringType()),
            optional("hobby0", stringType()),
            optional("hobby1", stringType())));
  }

  @Test
  void testCompatibleColumnWiden() {
    assertCompatible(
        schema(
            optional("id", intType()),
            optional("name", stringType())),
        schema(
            optional("id", longType()),
            optional("name", stringType())));
  }

  @Test
  void testCompatibleNullability() {
    assertCompatible(
        schema(
            optional("id", intType()),
            required("name", stringType())),
        schema(
            optional("id", longType()),
            optional("name", stringType())));
  }

  @Test
  void testCompatibleNestedStructTypeWidening() {
    assertCompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("name", stringType()),
                optional("age", intType())))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("name", stringType()),
                optional("age", longType())))));
  }

  @Test
  void testCompatibleNestedMapValueTypeWidening() {
    assertCompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("attributes", mapOfOptional(stringType(), intType()))))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("attributes", mapOfOptional(stringType(), longType()))))));
  }

  @Test
  void testIncompatibleWideningMap() {
    assertIncompatible(
        schema(optional("$$headers", mapOfOptional(binaryType(), binaryType()))),
        schema(optional("$$headers", mapOfOptional(stringType(), stringType()))),
        Rule.MAP_KEY_TYPE_MISMATCH, "$$headers");
  }

  @Test
  void testIncompatibleRemoval() {
    assertIncompatible(
        schema(
            optional("id", intType()),
            required("name", stringType())),
        schema(optional("id", longType())),
        Rule.FIELD_DELETED, "name");
  }

  @Test
  void testIncompatibleRemovalNested() {
    assertIncompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("name", stringType()),
                optional("email", stringType())))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("email", stringType())))),
        Rule.FIELD_DELETED, "person.name");
  }

  @Test
  void testIncompatibleRemovalNestedMap() {
    assertIncompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("age", stringType()),
                optional("inner_map", mapOfOptional(intType(), structOf(
                    optional("name", stringType()),
                    optional("email", stringType()))))))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("age", stringType()),
                optional("inner_map", mapOfOptional(intType(), structOf(
                    optional("email", stringType()))))))),
        Rule.FIELD_DELETED, "person.inner_map{}.name");
  }

  @Test
  void testIncompatibleReordering() {
    assertIncompatible(
        schema(
            optional("id", intType()),
            required("name", stringType())),
        schema(
            optional("name", stringType()),
            optional("id", longType())),
        Rule.FIELD_REORDERED, "id");
  }

  @Test
  void testIncompatibleReorderingSameType() {
    // Both fields share a type, so only name-keyed matching can detect the swap.
    assertIncompatible(
        schema(
            optional("email", stringType()),
            required("name", stringType())),
        schema(
            optional("name", stringType()),
            optional("email", stringType())),
        Rule.FIELD_REORDERED, "email");
  }

  @Test
  void testIncompatibleReorderingNested() {
    assertIncompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("name", stringType()),
                optional("email", stringType())))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("email", stringType()),
                optional("name", stringType())))),
        Rule.FIELD_REORDERED, "person.name");
  }

  @Test
  void testIncompatibleReorderingNestedArray() {
    assertIncompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("age", stringType()),
                optional("inner_array", listOfOptional(structOf(
                    optional("name", stringType()),
                    optional("email", stringType()))))))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("age", stringType()),
                optional("inner_array", listOfOptional(structOf(
                    optional("email", stringType()),
                    optional("name", stringType()))))))),
        Rule.FIELD_REORDERED, "person.inner_array[].name");
  }

  @Test
  void testIncompatibleNullability() {
    assertIncompatible(
        schema(
            optional("id", intType()),
            optional("name", stringType())),
        schema(
            required("id", longType()),
            optional("name", stringType())),
        Rule.NULLABLE_TO_NON_NULLABLE, "id");
  }

  @Test
  void testIncompatibleColumnRename() {
    // A rename is indistinguishable from a delete plus an add when there are no field IDs.
    assertIncompatible(
        schema(
            required("id", intType()),
            optional("name", stringType())),
        schema(
            required("id", longType()),
            optional("banana", stringType())),
        Rule.FIELD_DELETED, "name");
  }

  @Test
  void testIncompatibleTypeNarrowing() {
    assertIncompatible(
        schema(
            optional("id", longType()),
            optional("name", stringType())),
        schema(
            optional("id", intType()),
            optional("name", stringType())),
        Rule.UNSUPPORTED_TYPE_CHANGE, "id");
  }

  @Test
  void testIncompatibleTypeChange() {
    assertIncompatible(
        schema(
            optional("id", stringType()),
            optional("name", stringType())),
        schema(
            optional("id", intType()),
            optional("name", stringType())),
        Rule.UNSUPPORTED_TYPE_CHANGE, "id");
  }

  @Test
  void testIncompatibleNestedMapValueTypeChange() {
    assertIncompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("attributes", mapOfOptional(stringType(), stringType()))))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("attributes", mapOfOptional(stringType(), intType()))))),
        Rule.UNSUPPORTED_TYPE_CHANGE, "person.attributes{}");
  }

  @Test
  void testIncompatibleNestedMapKeyTypeChange() {
    assertIncompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("attributes", mapOfOptional(stringType(), stringType()))))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("attributes", mapOfOptional(intType(), stringType()))))),
        Rule.MAP_KEY_TYPE_MISMATCH, "person.attributes");
  }

  @Test
  void testIncompatibleAdditionOfRequiredField() {
    assertIncompatible(
        schema(
            optional("id", longType()),
            optional("name", stringType())),
        schema(
            optional("id", longType()),
            optional("name", stringType()),
            required("age", intType())),
        Rule.REQUIRED_FIELD_ADDED, "age");
  }

  @Test
  void testIncompatibleNestedMapKeyTypeChangeValidWiden() {
    // int -> long is a valid promotion anywhere else, but map keys are frozen.
    assertIncompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("attributes", mapOfOptional(intType(), stringType()))))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("attributes", mapOfOptional(longType(), stringType()))))),
        Rule.MAP_KEY_TYPE_MISMATCH, "person.attributes");
  }

  @Test
  void testCompatibleAddToNestedStruct() {
    // The Iceberg spec explicitly permits adding to a nested struct, so this is allowed.
    assertCompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("name", stringType()),
                optional("age", intType())))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("name", stringType()),
                optional("email", stringType()),
                optional("age", intType())))));
  }

  // ---------------------------------------------------------------------------------------------
  // Container-nullability relaxation -- folded into this checker rather than a separate pass
  // ---------------------------------------------------------------------------------------------

  /** A non-nullable field carrying a default, as the converters emit for containers. */
  private static Field requiredWithDefault(String name, Schema type, Object dflt) {
    return new Field(name, type.setNullable(false), 0, dflt, true, null, null, null);
  }

  private static Schema requiredMap() {
    return Schema.createMap(stringType().setNullable(false), stringType().setNullable(true));
  }

  private static Schema requiredList() {
    return Schema.createArray(stringType().setNullable(false));
  }

  @Test
  void relaxesNewlyAddedRequiredMapInBothIcebergAndFlinkSchemas() {
    assertCompatible(
        schema(optional("id", intType())),
        schema(
            optional("id", intType()),
            requiredWithDefault("labels", requiredMap(), Collections.emptyMap())));
  }

  @Test
  void relaxesNewlyAddedRequiredListInBothSchemas() {
    assertCompatible(
        schema(optional("id", intType())),
        schema(
            optional("id", intType()),
            requiredWithDefault("tags", requiredList(), Collections.emptyList())));
  }

  @Test
  void doesNotRelaxRequiredFieldWithoutMatchingDefault() {
    assertIncompatible(
        schema(optional("id", intType())),
        schema(
            optional("id", intType()),
            required("labels", requiredMap())),
        Rule.REQUIRED_FIELD_ADDED, "labels");
  }

  @Test
  void doesNotRelaxPrimitiveRequiredEvenWithDefault() {
    assertIncompatible(
        schema(optional("id", intType())),
        schema(
            optional("id", intType()),
            requiredWithDefault("count", intType(), 0)),
        Rule.REQUIRED_FIELD_ADDED, "count");
  }

  @Test
  void doesNotRelaxPreExistingRequiredMap() {
    // A pre-existing required container stays required, so nothing changes and nothing is reported.
    assertCompatible(
        schema(requiredWithDefault("labels", requiredMap(), Collections.emptyMap())),
        schema(requiredWithDefault("labels", requiredMap(), Collections.emptyMap())));
  }

  @Test
  void relaxesPreExistingOptionalFieldNowRequiredWithMatchingDefault() {
    // Condition (a) of the normalizer: the field was already nullable, so tightening it to NOT NULL
    // is forgiven when it carries a default.
    assertCompatible(
        schema(optional("labels", requiredMap())),
        schema(requiredWithDefault("labels", requiredMap(), Collections.emptyMap())));
  }

  @Test
  void relaxesPreExistingOptionalListNowRequiredWithMatchingDefault() {
    assertCompatible(
        schema(optional("tags", requiredList())),
        schema(requiredWithDefault("tags", requiredList(), Collections.emptyList())));
  }

  @Test
  void relaxesNewlyAddedRequiredMapInsideExistingStruct() {
    assertCompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(optional("name", stringType())))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("name", stringType()),
                requiredWithDefault("labels", requiredMap(), Collections.emptyMap())))));
  }

  @Test
  void doesNotRelaxNestedRequiredMapWithoutMatchingDefault() {
    assertIncompatible(
        schema(
            optional("id", intType()),
            required("person", structOf(optional("name", stringType())))),
        schema(
            optional("id", intType()),
            required("person", structOf(
                optional("name", stringType()),
                required("labels", requiredMap())))),
        Rule.REQUIRED_FIELD_ADDED, "person.labels");
  }

  @Test
  void relaxesNewlyAddedRequiredFieldInsideMapValueStruct() {
    assertCompatible(
        schema(
            required("outer", mapOfOptional(stringType(), structOf(
                optional("name", stringType()))))),
        schema(
            required("outer", mapOfOptional(stringType(), structOf(
                optional("name", stringType()),
                requiredWithDefault("labels", requiredMap(), Collections.emptyMap()))))));
  }

  @Test
  void doesNotRelaxInnerFieldInMapValueStructWithoutMatchingDefault() {
    assertIncompatible(
        schema(
            required("outer", mapOfOptional(stringType(), structOf(
                optional("name", stringType()))))),
        schema(
            required("outer", mapOfOptional(stringType(), structOf(
                optional("name", stringType()),
                required("labels", requiredMap()))))),
        Rule.REQUIRED_FIELD_ADDED, "outer{}.labels");
  }

  @Test
  void relaxesNewlyAddedRequiredFieldInsideListElementStruct() {
    assertCompatible(
        schema(
            required("outer", listOfOptional(structOf(
                optional("name", stringType()))))),
        schema(
            required("outer", listOfOptional(structOf(
                optional("name", stringType()),
                requiredWithDefault("labels", requiredMap(), Collections.emptyMap()))))));
  }

  @Test
  void doesNotRelaxInnerFieldInListElementStructWithoutMatchingDefault() {
    assertIncompatible(
        schema(
            required("outer", listOfOptional(structOf(
                optional("name", stringType()))))),
        schema(
            required("outer", listOfOptional(structOf(
                optional("name", stringType()),
                required("labels", requiredMap()))))),
        Rule.REQUIRED_FIELD_ADDED, "outer[].labels");
  }

  @Test
  void relaxesInnerFieldInsidePreExistingRequiredOuterMap() {
    // The outer map is pre-existing and required, so it is not itself relaxed; the walk must still
    // descend into it and relax the newly added inner container.
    assertCompatible(
        schema(
            requiredWithDefault("outer", Schema.createMap(
                stringType().setNullable(false),
                structOf(optional("name", stringType()))), Collections.emptyMap())),
        schema(
            requiredWithDefault("outer", Schema.createMap(
                stringType().setNullable(false),
                structOf(
                    optional("name", stringType()),
                    requiredWithDefault("labels", requiredMap(), Collections.emptyMap()))),
                Collections.emptyMap())));
  }

  @Test
  void relaxesInnerFieldInsidePreExistingRequiredOuterList() {
    assertCompatible(
        schema(
            requiredWithDefault("outer", Schema.createArray(
                structOf(optional("name", stringType()))), Collections.emptyList())),
        schema(
            requiredWithDefault("outer", Schema.createArray(
                structOf(
                    optional("name", stringType()),
                    requiredWithDefault("labels", requiredMap(), Collections.emptyMap()))),
                Collections.emptyList())));
  }

  @Test
  void relaxesFieldNestedThroughStructMapValueAndListElement() {
    // struct -> map value -> list element -> struct, with the relaxation at the innermost level.
    assertCompatible(
        schema(
            required("person", structOf(
                optional("history", mapOfOptional(stringType(), listOfOptional(structOf(
                    optional("name", stringType())))))))),
        schema(
            required("person", structOf(
                optional("history", mapOfOptional(stringType(), listOfOptional(structOf(
                    optional("name", stringType()),
                    requiredWithDefault(
                        "labels", requiredMap(), Collections.emptyMap())))))))));
  }

  @Test
  void relaxesOuterMapAndNewlyAddedInnerFieldInOnePass() {
    // Both the newly added outer map and a newly added inner container are relaxed together.
    assertCompatible(
        schema(optional("id", intType())),
        schema(
            optional("id", intType()),
            requiredWithDefault("outer", Schema.createMap(
                stringType().setNullable(false),
                structOf(
                    optional("name", stringType()),
                    requiredWithDefault("labels", requiredMap(), Collections.emptyMap()))),
                Collections.emptyMap())));
  }

  @Test
  void relaxesRequiredMapInsideListElementStructWithMatchingDefault() {
    assertCompatible(
        schema(
            required("outer", listOfOptional(structOf(optional("name", stringType()))))),
        schema(
            required("outer", listOfOptional(structOf(
                optional("name", stringType()),
                requiredWithDefault("inner", requiredMap(), Collections.emptyMap()))))));
  }

  @Test
  void relaxesRequiredListInsideMapValueStructIncidentRepro() {
    // Incident repro carried over: a required list added inside a map-value struct.
    assertCompatible(
        schema(
            required("outer", mapOfOptional(stringType(), structOf(
                optional("name", stringType()))))),
        schema(
            required("outer", mapOfOptional(stringType(), structOf(
                optional("name", stringType()),
                requiredWithDefault("inner", requiredList(), Collections.emptyList()))))));
  }
}
