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
    assertTrue(errors.stream().anyMatch(e -> e.contains("validity")));
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
    assertTrue(errors.stream().anyMatch(e -> e.contains("compatibility") && e.contains("backward")),
        errors.toString());
  }

  @Test
  void noneSkipsCompatibilityButKeepsValidity() {
    // Same incompatible pair, but level NONE -> no compatibility error, and RECORD_A_B is valid.
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.NONE);
    assertTrue(errors.isEmpty(), errors.toString());
  }

  @Test
  void backwardLabelsDirectionBackward() {
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.BACKWARD);
    assertTrue(errors.stream().allMatch(e -> !e.contains("forward")), errors.toString());
    assertTrue(errors.stream().anyMatch(e -> e.contains("backward")), errors.toString());
  }

  @Test
  void forwardLabelsDirectionForward() {
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.FORWARD);
    assertTrue(errors.stream().anyMatch(e -> e.contains("forward")), errors.toString());
  }

  @Test
  void fullChecksBothDirections() {
    List<String> errors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.FULL);
    assertTrue(errors.stream().anyMatch(e -> e.contains("backward")), errors.toString());
    assertTrue(errors.stream().anyMatch(e -> e.contains("forward")), errors.toString());
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
    assertTrue(errors.stream().anyMatch(e -> e.contains("compatibility")), errors.toString());
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
