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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Type;
import io.confluent.kafka.schemaregistry.type.logical.policy.Incompatibility.Rule;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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

  @Test
  void theCheckOfAMessageSharedAlongManyPathsDoesNotPlaceItsDefaults() {
    // Each message holds the next twice: 2^30 paths to one default the check never reads.
    StringBuilder text = new StringBuilder("syntax = \"proto3\";\npackage p;\n");
    for (int i = 0; i < 30; i++) {
      text.append("message M").append(i).append(" { M").append(i + 1).append(" a = 1; M")
          .append(i + 1).append(" b = 2; }\n");
    }
    ProtobufSchema diamond = new ProtobufSchema(text.append("message M30 { int32 x = 1; }\n")
        .toString());
    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> LogicalPolicyChecker.check(diamond,
        List.of(new SimpleParsedSchemaHolder(diamond)), CompatibilityLevel.BACKWARD));
  }

  @Test
  void aChainOfMessagesNestingTooDeepIsRejectedRatherThanOverflowing() throws Exception {
    // Deep only by reference, on a request thread's small stack: the walk overflowed into a 500.
    AtomicReference<Object> result = new AtomicReference<>();
    Thread check = new Thread(null, () -> {
      try {
        result.set(LogicalPolicyChecker.check(chain(2000, true),
            List.of(new SimpleParsedSchemaHolder(chain(2000, false))),
            CompatibilityLevel.BACKWARD_TRANSITIVE));
      } catch (Throwable e) {
        result.set(e);
      }
    }, "check", 512 << 10);
    check.start();
    check.join(30_000);
    assertFalse(check.isAlive(), "the check did not finish");
    assertTrue(result.get() instanceof List, "the check threw " + result.get());
    List<?> errors = (List<?>) result.get();
    assertEquals(1, errors.size(), errors.toString());
    assertTrue(errors.get(0).toString().contains("nesting exceeds"), errors.toString());
  }

  @Test
  void aChainOfMessagesWithinTheDepthLimitIsChecked() {
    // Compared to its end: the field added at the deepest message is found.
    List<String> errors = LogicalPolicyChecker.check(chain(90, true),
        List.of(new SimpleParsedSchemaHolder(chain(90, false))), CompatibilityLevel.BACKWARD);
    assertEquals(1, errors.size(), errors.toString());
    assertTrue(errors.get(0).contains("REQUIRED_FIELD_ADDED")
        && errors.get(0).contains(".next.y"), errors.toString());
  }

  @Test
  void aPreviousVersionNestingTooDeepIsSkipped() {
    // Too deep to convert, as for provenance: compared to nothing, as any unconvertible previous
    // version is, so the change at its deepest message goes unreported.
    List<String> errors = LogicalPolicyChecker.check(chain(150, false),
        List.of(new SimpleParsedSchemaHolder(chain(150, true))), CompatibilityLevel.BACKWARD);
    assertEquals(1, errors.size(), errors.toString());
    assertTrue(errors.get(0).contains("nesting exceeds"), errors.toString());
    errors = LogicalPolicyChecker.check(chain(90, false),
        List.of(new SimpleParsedSchemaHolder(chain(150, true))), CompatibilityLevel.BACKWARD);
    assertTrue(errors.isEmpty(), errors.toString());
  }

  // A file of n messages, each holding the next: deep by reference, never by inline nesting.
  private static ProtobufSchema chain(int n, boolean extra) {
    StringBuilder text = new StringBuilder("syntax = \"proto3\";\npackage p;\n"
        + "message Root { int32 id = 1; M1 c = 2; }\n");
    for (int i = 1; i <= n; i++) {
      text.append("message M").append(i).append(" { ")
          .append(i < n ? "M" + (i + 1) + " next = 1; " : "").append("int32 x = 2; ")
          .append(extra && i == n ? "int32 y = 3; " : "").append("}\n");
    }
    return new ProtobufSchema(text.toString());
  }

  // Protobuf files with one or several top-level messages, as Flink reads them.
  private static final String ORDER = "message Order { int32 id = 1; }\n";
  private static final String SHIP = "message Ship { string to = 1; }\n";

  private static ProtobufSchema proto(String... messages) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\n" + String.join("", messages));
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

  @Test
  void thePolicyReadsAMultiMessageProtobufFileAsFlinkDoes() {
    // As Flink wraps them: one field per top-level message, by its full name, in file order.
    LogicalType logical = LogicalPolicyChecker.toPolicyLogicalType(proto(ORDER, SHIP));
    assertEquals(List.of("p.Order", "p.Ship"), names(logical));
    // A file with one message is that message, unwrapped.
    assertEquals(List.of("id"), names(LogicalPolicyChecker.toPolicyLogicalType(proto(ORDER))));
  }

  @Test
  void formatLogicalShowsAMultiMessageProtobufFileAsAllItsMessages() {
    // Naming no root, its records may carry either message, as the policy reads it.
    assertEquals(List.of("p.Order", "p.Ship"),
        names(LogicalPolicyChecker.toLogicalType(proto(ORDER, SHIP))));
  }

  @Test
  void aProtobufFileNamingItsRootIsReadAsThatMessage() {
    ProtobufSchema rooted = rooted("p.Ship", ORDER, SHIP);
    assertEquals(List.of("to"), names(LogicalPolicyChecker.toPolicyLogicalType(rooted)));
    assertEquals(List.of("to"), names(LogicalPolicyChecker.toLogicalType(rooted)));
  }

  private static ProtobufSchema rooted(String root, String... messages) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"confluent/meta.proto\";\n"
        + "option (confluent.file_meta) = { params: [ { key: \"logical.root\", value: \"" + root
        + "\" } ] };\n" + String.join("", messages));
  }

  private static List<String> names(LogicalType logical) {
    return logical.getRootSchema().getFields().stream().map(Field::getName)
        .collect(Collectors.toList());
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

  @Test
  void aJsonDefinitionReferringOnlyToItselfIsReportedRatherThanLooping() {
    String schema = "{\"type\":\"object\",\"properties\":{\"d\":{\"$ref\":\"#/definitions/D\"}},"
        + "\"definitions\":{\"D\":{\"$ref\":\"#/definitions/D\"}}}";
    List<String> errors = assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
        LogicalPolicyChecker.check(new JsonSchema(schema), List.of(), CompatibilityLevel.NONE));
    assertTrue(errors.stream().anyMatch(e -> e.contains("CYCLIC_TYPE")), errors.toString());
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
  void aProtobufFileGainingOrLosingAMessageIsRejected() {
    // Flink's row reshapes between one message and several: every field moves under a row.
    assertFalse(LogicalPolicyChecker.check(proto(ORDER, SHIP),
        List.of(new SimpleParsedSchemaHolder(proto(ORDER))), CompatibilityLevel.BACKWARD).isEmpty());
    assertFalse(LogicalPolicyChecker.check(proto(ORDER),
        List.of(new SimpleParsedSchemaHolder(proto(ORDER, SHIP))), CompatibilityLevel.FORWARD)
        .isEmpty());
  }

  @Test
  void aProtobufFileThatOnlyReExportsAnotherIsCheckedAsTheFileItReExports() {
    // Its records carry the re-exported file's messages, and Flink reads that file's row: valid,
    // and interchangeable with the file itself, one message or several.
    String one = "syntax = \"proto3\";\npackage d;\n" + ORDER;
    String two = one + SHIP;
    assertTrue(LogicalPolicyChecker.check(reExport(one), List.of(), CompatibilityLevel.NONE)
        .isEmpty());
    assertTrue(checkFull(reExport(one), new ProtobufSchema(one)).isEmpty());
    assertTrue(checkFull(new ProtobufSchema(one), reExport(one)).isEmpty());
    assertTrue(checkFull(reExport(two), new ProtobufSchema(two)).isEmpty());
    // Re-exporting a file of two messages after one reshapes Flink's row, as the file would.
    assertFalse(checkFull(reExport(two), new ProtobufSchema(one)).isEmpty());
  }

  @Test
  void aReExportOfABuiltInFileIsCheckedAsTheFileItself() {
    // confluent/type/decimal.proto is bundled, not referenced: the wrapper is that file.
    ProtobufSchema wrapper = new ProtobufSchema(
        "syntax = \"proto3\";\nimport public \"confluent/type/decimal.proto\";\n");
    ProtobufSchema direct = new ProtobufSchema(
        new ProtobufSchema(wrapper.toDescriptor()).canonicalString());
    assertEquals(LogicalPolicyChecker.toPolicyLogicalType(direct).getRootSchema(),
        LogicalPolicyChecker.toPolicyLogicalType(wrapper).getRootSchema());
    assertEquals(LogicalPolicyChecker.check(direct, List.of(), CompatibilityLevel.NONE),
        LogicalPolicyChecker.check(wrapper, List.of(), CompatibilityLevel.NONE));
    assertTrue(checkFull(wrapper, direct).isEmpty());
  }

  @Test
  void aWrapperRootChangedByItsRootParamIsChecked() {
    // The root param picks the message records carry, so narrowing it is caught.
    ProtobufSchema wide = rootedWrapper("google.protobuf.Int64Value");
    ProtobufSchema narrow = rootedWrapper("google.protobuf.Int32Value");
    assertEquals(Type.BIGINT,
        LogicalPolicyChecker.toPolicyLogicalType(wide).getRootSchema().getType());
    assertFalse(LogicalPolicyChecker.check(narrow, List.of(new SimpleParsedSchemaHolder(wide)),
        CompatibilityLevel.BACKWARD).isEmpty());
    assertTrue(LogicalPolicyChecker.check(wide, List.of(new SimpleParsedSchemaHolder(narrow)),
        CompatibilityLevel.BACKWARD).isEmpty());
  }

  @Test
  void aProtobufFileThatOnlyReExportsAnotherIsReadAsTheFileItReExports() {
    // Naming no root, its records may carry any message of the file it re-exports.
    String dependency = "syntax = \"proto3\";\npackage d;\n" + ORDER + SHIP;
    ProtobufSchema reExporting = new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\nimport public \"d.proto\";\n",
        List.of(new SchemaReference("d.proto", "d", 1)), Map.of("d.proto", dependency), null, null);
    assertEquals(List.of("d.Order", "d.Ship"),
        names(LogicalPolicyChecker.toPolicyLogicalType(reExporting)));
  }

  @Test
  void aWrapperRootIsCheckedAsTheMessageItNames() {
    // KTable's primitive roots: widening FLOAT to DOUBLE is safe, narrowing BIGINT to INT is not.
    ProtobufSchema floatRoot = rootedWrapper("google.protobuf.FloatValue");
    ProtobufSchema doubleRoot = rootedWrapper("google.protobuf.DoubleValue");
    assertEquals(Type.DOUBLE,
        LogicalPolicyChecker.toPolicyLogicalType(doubleRoot).getRootSchema().getType());
    assertTrue(LogicalPolicyChecker.check(doubleRoot,
        List.of(new SimpleParsedSchemaHolder(floatRoot)), CompatibilityLevel.BACKWARD).isEmpty());
    assertFalse(LogicalPolicyChecker.check(rootedWrapper("google.protobuf.Int32Value"),
        List.of(new SimpleParsedSchemaHolder(rootedWrapper("google.protobuf.Int64Value"))),
        CompatibilityLevel.BACKWARD).isEmpty());
  }

  private static ProtobufSchema rootedWrapper(String root) {
    return new ProtobufSchema("syntax = \"proto3\";\nimport \"confluent/meta.proto\";\n"
        + "import public \"google/protobuf/wrappers.proto\";\n"
        + "option (confluent.file_meta) = { params: [ { key: \"logical.root\", value: \"" + root
        + "\" } ] };\n");
  }

  private static ProtobufSchema reExport(String file) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\nimport public \"d.proto\";\n",
        List.of(new SchemaReference("d.proto", "d", 1)), Map.of("d.proto", file), null, null);
  }

  private static List<String> checkFull(ProtobufSchema next, ProtobufSchema previous) {
    return LogicalPolicyChecker.check(next, List.of(new SimpleParsedSchemaHolder(previous)),
        CompatibilityLevel.FULL);
  }

  @Test
  void aChangeToAProtobufFilesSecondMessageIsChecked() {
    // Every message is a row of the table, so narrowing a field of the second one is caught.
    ProtobufSchema wide = proto(ORDER, "message Ship { int64 weight = 1; }\n");
    ProtobufSchema narrow = proto(ORDER, "message Ship { int32 weight = 1; }\n");
    List<String> errors = LogicalPolicyChecker.check(
        narrow, List.of(new SimpleParsedSchemaHolder(wide)), CompatibilityLevel.BACKWARD);
    assertFalse(errors.isEmpty());
    assertTrue(errors.stream().anyMatch(e -> e.contains("weight")), errors.toString());
  }

  @Test
  void aProtobufFilesMessagesReorderedAreAFieldReordered() {
    // Each message is a column of the table, in file order; one finding per direction.
    List<String> errors = LogicalPolicyChecker.check(proto(SHIP, ORDER),
        List.of(new SimpleParsedSchemaHolder(proto(ORDER, SHIP))), CompatibilityLevel.FULL);
    assertEquals(2, errors.size(), errors.toString());
    assertTrue(errors.stream().allMatch(e -> e.contains("FIELD_REORDERED")
        && e.contains("category:[\"ICEBERG_V2\"]")), errors.toString());
  }

  @Test
  void aProtobufFieldRenamedUnderItsNumberIsFieldRenamedUnlessDisabled() {
    // Sequential numbers are not recorded, so the policy has to say the format for them to count.
    ProtobufSchema before = proto("message Order { int32 id = 1; string note = 2; }\n");
    ProtobufSchema after = proto("message Order { int32 id = 1; string memo = 2; }\n");
    List<ParsedSchemaHolder> previous = List.of(new SimpleParsedSchemaHolder(before));

    List<String> errors = LogicalPolicyChecker.check(after, previous, CompatibilityLevel.FULL);
    assertTrue(errors.stream().anyMatch(e -> e.contains("FIELD_RENAMED")), errors.toString());
    assertFalse(errors.stream().anyMatch(e -> e.contains("FIELD_DELETED")), errors.toString());

    // Only Iceberg pairs renames: Flink pairs by name, so it still sees a required field added.
    Set<Rule> withoutRenamed = LogicalPolicyChecker.parseChecks(List.of("-FIELD_RENAMED"));
    errors = LogicalPolicyChecker.check(after, previous, CompatibilityLevel.FULL, withoutRenamed);
    assertFalse(errors.isEmpty());
    assertTrue(errors.stream().allMatch(e -> e.contains("REQUIRED_FIELD_ADDED")
        && e.contains("category:[\"FLINK\"]")), errors.toString());
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
  void backwardAndForwardMessagesNameTheFieldCorrectly() {
    // Same pair as backwardAndForwardCheckOppositeDirections, but pinning the actual wording
    // rather than just which Rule fired -- this is exactly the fact that swapping which schema is
    // "original" and which is "update" between the two directions must still name the same field
    // correctly, per the customer-facing error-message spec (no "writer's/reader's schema").
    //
    // REQUIRED_FIELD_ADDED fires in both FLINK and ICEBERG_V2 at the same path, and
    // describeFinding keeps only one representative message -- FLINK's, since it precedes
    // ICEBERG_V2 in MODES -- so this pins FLINK's wording ("This schema adds..."), not ICEBERG's.
    // FIELD_DELETED has no FlinkComparison counterpart, so that one is unambiguous.
    List<String> backwardErrors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.BACKWARD);
    assertTrue(backwardErrors.stream().anyMatch(e -> e.contains(
        "This schema adds 'b' as a required field. Add it as an optional field by setting its "
            + "default value to null.")),
        backwardErrors.toString());

    List<String> forwardErrors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_B), List.of(holder(RECORD_A)), CompatibilityLevel.FORWARD);
    assertTrue(forwardErrors.stream().anyMatch(e -> e.contains(
        "Schema no longer includes field 'b'. Restore the field.")),
        forwardErrors.toString());
  }

  @Test
  void backwardAndForwardTypeChangeMessagesNameTheFieldCorrectly() {
    // A shared-helper message (describeChange, behind UNSUPPORTED_TYPE_CHANGE) rather than a
    // field-presence one. Unlike field presence, narrowing is genuinely asymmetric: long-write /
    // int-read is unsafe (truncates), but int-write / long-read is safe (widens) -- so reusing the
    // same (new, previous) pair across both directions, as the test above does, would exercise two
    // different real changes rather than the same one told two ways. To pin the same underlying
    // hazard (data written as BIGINT, read as INT) via both directions, BACKWARD takes it with
    // new=NARROW/previous=LONG, and FORWARD takes it with the pair swapped, new=LONG/previous=
    // NARROW -- both then resolve to writer=LONG, reader=NARROW, and both must report the same
    // message. FLINK's wording wins the merge (it precedes ICEBERG_V2 in MODES): BIGINT->INT
    // isn't a supported Flink implicit cast at all, so Part A's generic reason fires, not
    // Iceberg's more specific "can't hold all values of" one.
    List<String> backwardErrors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_NARROW), List.of(holder(RECORD_A_LONG)),
        CompatibilityLevel.BACKWARD);
    assertTrue(backwardErrors.stream().anyMatch(e -> e.contains(
        "The type of field 'n' changed from 'BIGINT' to 'INT'. Keep the original type.")),
        backwardErrors.toString());

    List<String> forwardErrors = LogicalPolicyChecker.check(
        new AvroSchema(RECORD_A_LONG), List.of(holder(RECORD_A_NARROW)),
        CompatibilityLevel.FORWARD);
    assertTrue(forwardErrors.stream().anyMatch(e -> e.contains(
        "The type of field 'n' changed from 'BIGINT' to 'INT'. Keep the original type.")),
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
    assertTrue(errors.get(0).contains("can't be converted to a table"), errors.toString());
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
        + "description:\"This schema can't be converted to a table. "
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

  @Test
  void parseChecksWithNoEntriesIsEveryRule() {
    Set<Rule> checks = LogicalPolicyChecker.parseChecks(List.of());
    assertEquals(Rule.defaults(), checks);
    assertEquals(EnumSet.allOf(Rule.class), checks);
  }

  @Test
  void parseChecksAppliesEachEntryOverItsRuleDefault() {
    // Disabling one rule leaves the other at its default rather than resetting it.
    Set<Rule> checks = LogicalPolicyChecker.parseChecks(List.of("-FIELD_DELETED"));
    assertFalse(checks.contains(Rule.FIELD_DELETED));
    assertTrue(checks.contains(Rule.FIELD_REORDERED));

    checks = LogicalPolicyChecker.parseChecks(List.of(" -field_reordered ", "-FIELD_DELETED"));
    assertFalse(checks.contains(Rule.FIELD_REORDERED));
    assertFalse(checks.contains(Rule.FIELD_DELETED));
    assertTrue(checks.contains(Rule.ENUM_DELETED));
  }

  @Test
  void parseChecksRejectsAMissingSignAnUnknownRuleAndAConflict() {
    assertThrows(IllegalArgumentException.class,
        () -> LogicalPolicyChecker.parseChecks(List.of("FIELD_DELETED")));
    assertThrows(IllegalArgumentException.class,
        () -> LogicalPolicyChecker.parseChecks(List.of("")));
    assertThrows(IllegalArgumentException.class,
        () -> LogicalPolicyChecker.parseChecks(List.of("-FIELD_DELETD")));
    assertThrows(IllegalArgumentException.class,
        () -> LogicalPolicyChecker.parseChecks(List.of("+FIELD_DELETED", "-FIELD_DELETED")));
  }
}
