/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import java.util.List;
import org.junit.jupiter.api.Test;

class LogicalPolicyCheckerTest {

  // A struct with one field.
  private static final String RECORD_A =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
          + "{\"name\":\"a\",\"type\":\"int\"}]}";

  // Adds a second required field (no default) relative to RECORD_A -> REQUIRED_FIELD_ADDED.
  private static final String RECORD_A_B =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
          + "{\"name\":\"a\",\"type\":\"int\"},"
          + "{\"name\":\"b\",\"type\":\"int\"}]}";

  // An empty record -> derives to an empty struct, which is invalid under Iceberg (EMPTY_STRUCT).
  private static final String EMPTY_RECORD =
      "{\"type\":\"record\",\"name\":\"E\",\"fields\":[]}";

  private static ParsedSchemaHolder holder(String avro) {
    return new SimpleParsedSchemaHolder(new AvroSchema(avro));
  }

  // -- toLogicalType ------------------------------------------------------------------------------

  @Test
  void toLogicalTypeConvertsAvro() {
    assertTrue(LogicalPolicyChecker.toLogicalType(new AvroSchema(RECORD_A)) != null);
  }

  @Test
  void toLogicalTypeRejectsUnknownSchemaType() {
    ParsedSchema unknown = mock(ParsedSchema.class);
    when(unknown.schemaType()).thenReturn("XML");
    assertThrows(IllegalArgumentException.class,
        () -> LogicalPolicyChecker.toLogicalType(unknown));
  }

  // -- validity runs regardless of level / previous versions -------------------------------------

  @Test
  void validityRunsOnFirstRegistrationWithNoPreviousVersions() {
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(EMPTY_RECORD), List.of(), CompatibilityLevel.NONE);
    assertFalse(errors.isEmpty(), "empty struct should fail Iceberg validity even with no previous");
    assertTrue(errors.stream().anyMatch(e -> e.contains("EMPTY_STRUCT")), errors.toString());
  }

  @Test
  void validSchemaWithNoPreviousPasses() {
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A), List.of(), CompatibilityLevel.NONE);
    assertTrue(errors.isEmpty(), errors.toString());
  }

  // -- compatibility ------------------------------------------------------------------------------

  @Test
  void backwardReportsRequiredFieldAdded() {
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.BACKWARD);
    assertFalse(errors.isEmpty());
    assertTrue(errors.stream().anyMatch(e -> e.contains("REQUIRED_FIELD_ADDED")), errors.toString());
  }

  @Test
  void noneSkipsCompatibilityButKeepsValidity() {
    // Same incompatible pair, but level NONE -> no compatibility error, and RECORD_A_B is valid.
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.NONE);
    assertTrue(errors.isEmpty(), errors.toString());
  }

  @Test
  void backwardAndForwardCheckOppositeDirections() {
    // BACKWARD: new(A_B) must read old(A) -- adding required field 'b' breaks that ->
    // REQUIRED_FIELD_ADDED, but nothing is FIELD_DELETED since A_B has every field A has.
    List<String> backwardErrors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.BACKWARD);
    assertTrue(backwardErrors.stream().anyMatch(e -> e.contains("REQUIRED_FIELD_ADDED")),
        backwardErrors.toString());
    assertFalse(backwardErrors.stream().anyMatch(e -> e.contains("FIELD_DELETED")),
        backwardErrors.toString());

    // FORWARD: old(A) must read new(A_B) -- from A_B's perspective, 'b' is now FIELD_DELETED
    // (ICEBERG_V2 only; FLINK has no such rule), not a required-field addition.
    List<String> forwardErrors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.FORWARD);
    assertTrue(forwardErrors.stream().anyMatch(e -> e.contains("FIELD_DELETED")),
        forwardErrors.toString());
    assertFalse(forwardErrors.stream().anyMatch(e -> e.contains("REQUIRED_FIELD_ADDED")),
        forwardErrors.toString());
  }

  @Test
  void fullChecksBothDirections() {
    // FULL runs both comparisons, so both directions' findings should be present together.
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.FULL);
    assertTrue(errors.stream().anyMatch(e -> e.contains("REQUIRED_FIELD_ADDED")), errors.toString());
    assertTrue(errors.stream().anyMatch(e -> e.contains("FIELD_DELETED")), errors.toString());
  }

  // -- transitivity: which previous versions are compared ----------------------------------------

  @Test
  void nonTransitiveComparesLatestOnly() {
    // old (index 0) lacks b; latest (index 1) already has b, as does new -> compatible with latest.
    // Non-transitive should only compare against latest and find nothing.
    List<ParsedSchemaHolder> previous = List.of(holder(RECORD_A), holder(RECORD_A_B));
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), previous, CompatibilityLevel.BACKWARD);
    assertTrue(errors.isEmpty(), errors.toString());
  }

  @Test
  void transitiveComparesAllPreviousVersions() {
    // Same lists, but transitive should also compare against the older version (index 0), which is
    // missing b -> REQUIRED_FIELD_ADDED.
    List<ParsedSchemaHolder> previous = List.of(holder(RECORD_A), holder(RECORD_A_B));
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), previous, CompatibilityLevel.BACKWARD_TRANSITIVE);
    assertFalse(errors.isEmpty());
    assertTrue(errors.stream().anyMatch(e -> e.contains("REQUIRED_FIELD_ADDED")), errors.toString());
  }

  // -- conversion failures ------------------------------------------------------------------------

  @Test
  void unconvertibleNewSchemaIsRejected() {
    ParsedSchema unconvertible = mock(ParsedSchema.class);
    when(unconvertible.schemaType()).thenReturn("XML");
    List<String> errors = LogicalPolicyChecker.check(
        unconvertible, List.of(), CompatibilityLevel.BACKWARD);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("cannot be represented as a logical type"), errors.toString());
  }

  // -- merging findings across modes ---------------------------------------------------------

  @Test
  void mergesRequiredFieldAddedAcrossFlinkAndIcebergIntoOneLine() {
    // Both FLINK and ICEBERG_V2 report REQUIRED_FIELD_ADDED at the same path ('b') for this
    // change, so they should collapse into a single tagged line rather than two near-duplicates.
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.BACKWARD);

    long mergedLines = errors.stream()
        .filter(e -> e.contains("REQUIRED_FIELD_ADDED")
            && e.contains("category:[\"FLINK\", \"ICEBERG_V2\"]"))
        .count();
    assertEquals(1, mergedLines, errors.toString());
    // Only one message should appear for the merged finding, not one per mode.
    long occurrencesOfRule = errors.stream()
        .filter(e -> e.contains("REQUIRED_FIELD_ADDED"))
        .count();
    assertEquals(1, occurrencesOfRule, errors.toString());
  }

  @Test
  void doesNotMergeFindingsUniqueToOneMode() {
    // Dropping field 'b' is FIELD_DELETED under ICEBERG_V2 only -- FLINK has no such rule -- so
    // that finding must stay on its own single-mode line, not be folded away.
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A), List.of(holder(RECORD_A_B)), CompatibilityLevel.BACKWARD);

    assertTrue(errors.stream().anyMatch(
        e -> e.contains("FIELD_DELETED") && e.contains("category:[\"ICEBERG_V2\"]")),
        errors.toString());
    assertFalse(errors.stream().anyMatch(e -> e.contains("FIELD_DELETED") && e.contains("FLINK")),
        errors.toString());
  }

  @Test
  void singleModeAndMultiModeFindingsShareTheSameHeaderShape() {
    // A single-mode finding (FIELD_DELETED, ICEBERG_V2 only) and a multi-mode finding
    // (REQUIRED_FIELD_ADDED, both modes) should both read as
    // "{errorType:\"<rule>\", category:[\"<mode>\", ...], description:\"...\",
    // additionalInfo:\"...\"}", not switch to a different shape depending on how many modes are
    // involved -- with no label in front of the object, and no mention of "backward"/"forward".
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A), List.of(holder(RECORD_A_B)), CompatibilityLevel.BACKWARD);

    String singleModeLine = errors.stream()
        .filter(e -> e.contains("FIELD_DELETED"))
        .findFirst()
        .orElseThrow();
    assertTrue(singleModeLine.startsWith(
        "{errorType:\"FIELD_DELETED\", category:[\"ICEBERG_V2\"], description:\""),
        singleModeLine);
    assertFalse(singleModeLine.contains("backward") || singleModeLine.contains("forward"),
        singleModeLine);
  }

  @Test
  void unconvertiblePreviousSchemaIsSkippedNotFatal() {
    ParsedSchema unconvertiblePrev = mock(ParsedSchema.class);
    when(unconvertiblePrev.schemaType()).thenReturn("XML");
    // New schema is valid; the only previous version can't be converted -> its comparison is
    // skipped, so nothing is reported.
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A),
        List.of(new SimpleParsedSchemaHolder(unconvertiblePrev)),
        CompatibilityLevel.BACKWARD);
    assertTrue(errors.isEmpty(), errors.toString());
  }
}
