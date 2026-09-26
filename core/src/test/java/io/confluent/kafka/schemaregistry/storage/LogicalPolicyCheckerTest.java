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
import io.confluent.kafka.schemaregistry.json.JsonSchema;
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

  // A pair differing only in field 'n's type (BIGINT vs INT), for exercising the shared
  // type-change message (describeChange / UNSUPPORTED_TYPE_CHANGE) rather than a field-presence
  // one -- long -> int is a narrowing, so comparing WIDE against NARROW in one direction rejects.
  private static final String RECORD_A_LONG =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
          + "{\"name\":\"n\",\"type\":\"long\"}]}";
  private static final String RECORD_A_NARROW =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
          + "{\"name\":\"n\",\"type\":\"int\"}]}";

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
  void backwardAndForwardMessagesNameTheWriterAndReaderCorrectly() {
    // Same pair as backwardAndForwardCheckOppositeDirections, but pinning the actual wording
    // rather than just which Rule fired -- this is exactly the fact that swapping which schema is
    // "original" and which is "update" between the two directions is meant to preserve.
    //
    // REQUIRED_FIELD_ADDED fires in both FLINK and ICEBERG_V2 at the same path, and
    // describeFinding keeps only one representative message -- FLINK's, since it precedes
    // ICEBERG_V2 in MODES -- so this pins FLINK's wording ("column..."), not ICEBERG's ("field...").
    // FIELD_DELETED has no FlinkComparison counterpart, so that one is unambiguous.
    List<String> backwardErrors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.BACKWARD);
    assertTrue(backwardErrors.stream().anyMatch(e -> e.contains(
        "column is required by the reader's schema but missing from the writer's schema")),
        backwardErrors.toString());

    List<String> forwardErrors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.FORWARD);
    assertTrue(forwardErrors.stream().anyMatch(e -> e.contains(
        "field present in the writer's schema is missing from the reader's schema")),
        forwardErrors.toString());
  }

  @Test
  void backwardAndForwardTypeChangeMessagesNameTheWriterAndReaderCorrectly() {
    // A shared-helper message (describeChange, behind UNSUPPORTED_TYPE_CHANGE) rather than a
    // field-presence one. Unlike field presence, narrowing is genuinely asymmetric: long-write /
    // int-read is unsafe (truncates), but int-write / long-read is safe (widens) -- so reusing the
    // same (new, previous) pair across both directions, as the test above does, would exercise two
    // different real changes rather than the same one told two ways. To pin the same underlying
    // hazard (data written as BIGINT, read as INT) via both directions, BACKWARD takes it with
    // new=NARROW/previous=LONG, and FORWARD takes it with the pair swapped, new=LONG/previous=
    // NARROW -- both then resolve to writer=LONG, reader=NARROW, and both must report the same
    // message.
    List<String> backwardErrors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_NARROW), List.of(holder(RECORD_A_LONG)),
        CompatibilityLevel.BACKWARD);
    assertTrue(backwardErrors.stream().anyMatch(e -> e.contains(
        "type is BIGINT in the writer's schema and INT in the reader's schema")),
        backwardErrors.toString());

    List<String> forwardErrors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_LONG), List.of(holder(RECORD_A_NARROW)),
        CompatibilityLevel.FORWARD);
    assertTrue(forwardErrors.stream().anyMatch(e -> e.contains(
        "type is BIGINT in the writer's schema and INT in the reader's schema")),
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

  @Test
  void unconvertibleNewSchemaIsRenderedAsAStructuredFindingNotABareString() {
    // Regression test: this used to be a bare string sitting next to describeFinding's
    // {errorType, category, description, additionalInfo} objects in the same details array,
    // which breaks a client that parses every element as one of those objects. The loose
    // substring check in unconvertibleNewSchemaIsRejected above would not have caught that.
    ParsedSchema unconvertible = mock(ParsedSchema.class);
    when(unconvertible.schemaType()).thenReturn("XML");
    List<String> errors = LogicalPolicyChecker.check(
        unconvertible, List.of(), CompatibilityLevel.BACKWARD);
    assertEquals(1, errors.size());
    assertEquals("{errorType:\"UNREPRESENTABLE_TYPE\", category:[\"FLINK\", \"ICEBERG_V2\"], "
        + "description:\"Schema cannot be represented as a logical type: "
        + "format=logical is not supported for schema type 'XML'\", additionalInfo:\"\"}",
        errors.get(0));
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

  // -- edition: check() must derive JSON under V1, not the format=logical V2 default -------------

  @Test
  void checkComparesJsonUnderV1SoASingletonOneofCollapseIsFlagged() {
    // Regression test for the production path, not just the converter it calls: under the V2
    // canonical reading (what format=logical exposes), a singleton oneOf with no null collapses
    // to its member type, so dropping the oneOf wrapper would be invisible and this would report
    // nothing. check() must derive JSON under V1 instead, matching provenance, where the oneOf
    // stays a first-class UNION and the same edit is a structural kind change.
    String wrapped = "{\"type\":\"object\",\"properties\":{\"u\":{\"oneOf\":[{\"type\":\"integer\"}]}}}";
    String unwrapped = "{\"type\":\"object\",\"properties\":{\"u\":{\"type\":\"integer\"}}}";

    List<String> errors = LogicalPolicyChecker.check(
        new JsonSchema(unwrapped),
        List.of(new SimpleParsedSchemaHolder(new JsonSchema(wrapped))),
        CompatibilityLevel.BACKWARD);

    assertTrue(errors.stream().anyMatch(e -> e.contains("TYPE_MISMATCH")), errors.toString());
  }
}
