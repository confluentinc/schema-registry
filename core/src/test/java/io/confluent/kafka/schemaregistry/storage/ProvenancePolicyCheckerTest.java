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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.storage.ProvenancePolicyChecker.PairBy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ProvenancePolicyCheckerTest {

  private static final CompatibilityLevel[] LEVELS = {CompatibilityLevel.NONE,
      CompatibilityLevel.BACKWARD, CompatibilityLevel.BACKWARD_TRANSITIVE, CompatibilityLevel.FULL,
      CompatibilityLevel.FULL_TRANSITIVE};
  private static final String INT = "{\"type\":\"integer\"}";

  // --- By name, as LogicalPolicyChecker -------------------------------------------------------

  @Test
  void byNameItFindsWhatLogicalPolicyCheckerFinds() {
    List<List<ParsedSchema>> histories = Arrays.asList(
        Arrays.asList(avro("{\"name\":\"a\",\"type\":\"long\"}"),
            avro("{\"name\":\"a\",\"type\":\"int\"}"),
            avro("{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\"]}")),
        Arrays.asList(proto("int64 a = 1;"), proto("int32 a = 1;\n  string s = 2;"),
            proto("int32 b = 1;")),
        Arrays.asList(json("{\"a\":" + INT + "}"), json("{\"a\":{\"type\":\"string\"}}"),
            json("{\"b\":" + INT + "}")));
    for (List<ParsedSchema> history : histories) {
      for (int k = 1; k < history.size(); k++) {
        for (CompatibilityLevel level : LEVELS) {
          assertEquals(LogicalPolicyChecker.check(history.get(k), holders(history, k), level),
              check(history, k, level, PairBy.NAME), history + " v" + k + " " + level);
        }
      }
    }
  }

  // --- By check id -----------------------------------------------------------------------------

  @Test
  void aRenameIsOneColumnByCheckId() {
    // JSON has no renames: a property's name is its identity.
    List<List<ParsedSchema>> histories = Arrays.asList(
        Arrays.asList(avro("{\"name\":\"a\",\"type\":\"int\"}"),
            avro("{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\"]}")),
        Arrays.asList(proto("int32 a = 1;"), proto("int32 b = 1;")));
    for (List<ParsedSchema> history : histories) {
      assertTrue(has(check(history, 1, CompatibilityLevel.BACKWARD, PairBy.NAME),
          "FIELD_RENAMED"));
      assertEquals(List.of(), check(history, 1, CompatibilityLevel.BACKWARD, PairBy.CHECK_ID));
    }
  }

  @Test
  void aKindChangeIsStillATypeMismatchByCheckId() {
    List<List<ParsedSchema>> histories = Arrays.asList(
        Arrays.asList(avro("{\"name\":\"f\",\"type\":\"int\"}"),
            avro("{\"name\":\"f\",\"type\":{\"type\":\"record\",\"name\":\"M\","
                + "\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}}")),
        Arrays.asList(proto("int32 f = 1;"), proto("M f = 1;\n  message M { int32 a = 1; }")),
        Arrays.asList(json("{\"f\":" + INT + "}"),
            json("{\"f\":{\"type\":\"object\",\"properties\":{\"a\":" + INT + "}}}")));
    for (List<ParsedSchema> history : histories) {
      List<String> byName = check(history, 1, CompatibilityLevel.BACKWARD, PairBy.NAME);
      assertTrue(has(byName, "TYPE_MISMATCH"), byName.toString());
      assertEquals(byName, check(history, 1, CompatibilityLevel.BACKWARD, PairBy.CHECK_ID));
    }
  }

  @Test
  void aRenameInAProtobufFileOfSeveralMessagesIsOneColumnByCheckId() {
    String file = "syntax = \"proto3\";\npackage p;\nmessage A {\n  int32 x = 1;\n}\n"
        + "message B {\n  int32 %s = 1;\n}\n";
    List<ParsedSchema> history = Arrays.asList(new ProtobufSchema(String.format(file, "y")),
        new ProtobufSchema(String.format(file, "z")));
    assertTrue(has(check(history, 1, CompatibilityLevel.BACKWARD, PairBy.NAME), "FIELD_RENAMED"));
    assertEquals(List.of(), check(history, 1, CompatibilityLevel.BACKWARD, PairBy.CHECK_ID));
  }

  @Test
  void aProtobufFileGainingAMessageIsStillAReshapeByCheckId() {
    // Checked as its one message, then as two wrapped: by name and by check id alike.
    List<ParsedSchema> history = Arrays.asList(
        new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage A {\n  int32 x = 1;\n}\n"),
        new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage A {\n  int32 x = 1;\n}\n"
            + "message B {\n  int32 y = 1;\n}\n"));
    List<String> byName = check(history, 1, CompatibilityLevel.BACKWARD, PairBy.NAME);
    assertFalse(byName.isEmpty());
    assertEquals(LogicalPolicyChecker.check(history.get(1), holders(history, 1),
        CompatibilityLevel.BACKWARD), byName);
    assertEquals(byName, check(history, 1, CompatibilityLevel.BACKWARD, PairBy.CHECK_ID));
  }

  @Test
  void aWrappersRootIsPairedByItsOwnCheckIds() {
    // The root param picks B, the second of the re-exported file's two messages, so its check ids
    // are B's: renaming x under its number continues it, and B's third field has one too.
    String file = "syntax = \"proto3\";\npackage q;\nmessage A {\n  int32 a = 1;\n}\n"
        + "message B {\n  int32 %s = 1;\n  string y = 2;\n  int64 w = 3;\n}\n";
    List<ParsedSchema> history = Arrays.asList(rootedWrapper(String.format(file, "x"), "q.B"),
        rootedWrapper(String.format(file, "z"), "q.B"));
    assertEquals(List.of(), check(history, 1, CompatibilityLevel.BACKWARD, PairBy.CHECK_ID));
    assertTrue(has(check(history, 1, CompatibilityLevel.BACKWARD, PairBy.NAME), "FIELD_RENAMED"));
  }

  @Test
  void aWrapperRootedInsideAnotherMessageIsPairedByItsOwnCheckIds() {
    // A nested root is that message, by its full name: renaming c under its number continues it.
    String file = "syntax = \"proto3\";\npackage q;\n"
        + "message P {\n  message C {\n    int32 %s = 1;\n  }\n  C c = 1;\n}\n";
    List<ParsedSchema> history = Arrays.asList(rootedWrapper(String.format(file, "c"), "q.P.C"),
        rootedWrapper(String.format(file, "d"), "q.P.C"));
    assertEquals(List.of(), check(history, 1, CompatibilityLevel.BACKWARD, PairBy.CHECK_ID));
    assertTrue(has(check(history, 1, CompatibilityLevel.BACKWARD, PairBy.NAME), "FIELD_RENAMED"));
  }

  // --- Provenance that cannot be computed -----------------------------------------------------

  @Test
  void aHistoryProvenanceCannotBeComputedThroughRejects() {
    // c aliases both a and b: no single match, so no provenance for the transition.
    List<ParsedSchema> history = Arrays.asList(
        avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}"),
        avro("{\"name\":\"c\",\"type\":[\"null\",\"int\"],\"default\":null,"
            + "\"aliases\":[\"a\",\"b\"]}"));
    for (PairBy pairBy : PairBy.values()) {
      assertTrue(has(check(history, 1, CompatibilityLevel.BACKWARD, pairBy),
          "PROVENANCE_UNAVAILABLE"));
      // Nothing compared, nothing computed.
      assertEquals(List.of(), check(history, 1, CompatibilityLevel.NONE, pairBy));
    }
  }

  @Test
  void aCandidateBlockingProvenanceIsNamedByItsVersionNumber() {
    // Registered as versions 3 and 7, the candidate is version 8; the computer counts from 0.
    List<ParsedSchema> history = Arrays.asList(
        avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}"),
        avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"},"
            + "{\"name\":\"k\",\"type\":[\"null\",\"int\"],\"default\":null}"));
    ParsedSchema candidate = avro("{\"name\":\"c\",\"type\":[\"null\",\"int\"],"
        + "\"default\":null,\"aliases\":[\"a\",\"b\"]}");
    List<String> errors = ProvenancePolicyChecker.check("s", candidate, holders(history, 2),
        List.of(3, 7), List.of(0, 1), CompatibilityLevel.BACKWARD, PairBy.CHECK_ID, 8, true);
    assertTrue(has(errors, "PROVENANCE_UNAVAILABLE"), errors.toString());
    assertTrue(errors.toString().contains("at version 8"), errors.toString());
  }

  // --- An earlier version blocking provenance -------------------------------------------------

  @Test
  void anEarlierVersionBlockingProvenanceLeavesTheCheckToNamePairing() {
    // Each history's second version blocks provenance, as only a version registered before the
    // subject was checked this way can: the next one is checked by name, as before.
    String node = "{\"type\":\"record\",\"name\":\"N\",\"fields\":[{\"name\":\"next\","
        + "\"type\":[\"null\",\"N\"],\"default\":null}]}";
    String recursive = "{\"type\":\"object\",\"properties\":{\"n\":{\"$ref\":\"#/definitions/N\"}},"
        + "\"definitions\":{\"N\":{\"type\":\"object\",\"properties\":{\"next\":"
        + "{\"$ref\":\"#/definitions/N\"}}}}}";
    List<List<ParsedSchema>> histories = Arrays.asList(
        Arrays.asList(avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}"),
            avro("{\"name\":\"c\",\"type\":[\"null\",\"int\"],\"default\":null,"
                + "\"aliases\":[\"a\",\"b\"]}"),
            avro("{\"name\":\"c\",\"type\":[\"null\",\"long\"],\"default\":null}")),
        Arrays.asList(avro("{\"name\":\"a\",\"type\":\"int\"}"),
            avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"n\",\"type\":" + node + "}"),
            avro("{\"name\":\"a\",\"type\":\"long\"}")),
        Arrays.asList(proto("int32 a = 1;"),
            proto("int32 a = 1;\n  N n = 2;\n  message N { N next = 1; }"),
            proto("int64 a = 1;")),
        Arrays.asList(json("{\"a\":" + INT + "}"), new JsonSchema(recursive),
            json("{\"a\":{\"type\":\"number\"}}")));
    for (List<ParsedSchema> history : histories) {
      // As a candidate, the blocking version is rejected.
      assertTrue(has(check(history, 1, CompatibilityLevel.BACKWARD, PairBy.CHECK_ID),
          "PROVENANCE_UNAVAILABLE"), history.toString());
      for (PairBy pairBy : PairBy.values()) {
        for (CompatibilityLevel level : LEVELS) {
          assertEquals(LogicalPolicyChecker.check(history.get(2), holders(history, 2), level),
              check(history, 2, level, pairBy), history + " " + pairBy + " " + level);
        }
      }
    }
  }

  @Test
  void aBlockBeforeTheRangeLeavesTheCheckToCheckIds() {
    // v=1 blocks provenance; the candidate renames a field of v=2. A non-transitive level
    // computes from v=2 and pairs the rename by id; a transitive one reaches v=1 and pairs by name.
    String node = "{\"type\":\"record\",\"name\":\"N\",\"fields\":[{\"name\":\"next\","
        + "\"type\":[\"null\",\"N\"],\"default\":null}]}";
    List<List<ParsedSchema>> histories = Arrays.asList(
        Arrays.asList(avro("{\"name\":\"a\",\"type\":\"int\"}"),
            avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"n\",\"type\":" + node + "}"),
            avro("{\"name\":\"a\",\"type\":\"int\"}"),
            avro("{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\"]}")),
        Arrays.asList(proto("int32 a = 1;"),
            proto("int32 a = 1;\n  N n = 2;\n  message N { N next = 1; }"),
            proto("int32 a = 1;"),
            proto("int32 b = 1;")));
    // JSON has no renames: a property's name is its identity.
    for (List<ParsedSchema> history : histories) {
      for (CompatibilityLevel level : new CompatibilityLevel[] {CompatibilityLevel.BACKWARD,
          CompatibilityLevel.FULL}) {
        assertEquals(List.of(), check(history, 3, level, PairBy.CHECK_ID), history + " " + level);
      }
      for (CompatibilityLevel level : new CompatibilityLevel[] {
          CompatibilityLevel.BACKWARD_TRANSITIVE, CompatibilityLevel.FULL_TRANSITIVE}) {
        assertEquals(LogicalPolicyChecker.check(history.get(3), holders(history, 3), level),
            check(history, 3, level, PairBy.CHECK_ID), history + " " + level);
        assertTrue(has(check(history, 3, level, PairBy.CHECK_ID), "FIELD_DELETED"),
            history + " " + level);
      }
    }
  }

  @Test
  void aStoredVersionJudgedPastAnEarlierBlockingVersionIsRejected() {
    // v=2 has no logical form and v=3 drops b of v=1. Registration checks v=3 by name and skips
    // v=2; a stored v=3 judged against v=2 alone cannot be paired, so it is rejected.
    String node = "{\"type\":\"record\",\"name\":\"N\",\"fields\":[{\"name\":\"next\","
        + "\"type\":[\"null\",\"N\"],\"default\":null}]}";
    String recursive = "{\"type\":\"object\",\"properties\":{\"n\":{\"$ref\":\"#/definitions/N\"}},"
        + "\"definitions\":{\"N\":{\"type\":\"object\",\"properties\":{\"next\":"
        + "{\"$ref\":\"#/definitions/N\"}}}}}";
    List<List<ParsedSchema>> histories = Arrays.asList(
        Arrays.asList(avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}"),
            avro("{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"n\",\"type\":" + node + "}"),
            avro("{\"name\":\"a\",\"type\":\"int\"}")),
        Arrays.asList(proto("int32 a = 1;\n  int32 b = 2;"),
            proto("int32 a = 1;\n  N n = 3;\n  message N { N next = 1; }"),
            proto("int32 a = 1;")),
        Arrays.asList(json("{\"a\":" + INT + ",\"b\":" + INT + "}"), new JsonSchema(recursive),
            json("{\"a\":" + INT + "}")));
    for (List<ParsedSchema> history : histories) {
      List<String> errors = ProvenancePolicyChecker.check("s", history.get(2),
          holders(history, 2), List.of(1, 2), List.of(1), CompatibilityLevel.BACKWARD,
          PairBy.CHECK_ID, 3, false);
      assertTrue(has(errors, "PROVENANCE_UNAVAILABLE"), history.toString());
      assertTrue(errors.toString().contains("through an earlier version"), errors.toString());
    }
  }

  // -------------------------------------------------------------------------------------------

  // Version k registered after versions 0..k-1, numbered from 1, every one compared.
  private static List<String> check(List<ParsedSchema> history, int k, CompatibilityLevel level,
      PairBy pairBy) {
    List<Integer> previous = IntStream.range(0, k).boxed().collect(Collectors.toList());
    List<Integer> numbers = IntStream.rangeClosed(1, k).boxed().collect(Collectors.toList());
    return ProvenancePolicyChecker.check("s", history.get(k), holders(history, k), numbers,
        previous, level, pairBy, k + 1, true);
  }

  private static boolean has(List<String> errors, String rule) {
    return errors.stream().anyMatch(e -> e.contains("errorType:\"" + rule + "\""));
  }

  private static List<ParsedSchemaHolder> holders(List<ParsedSchema> history, int k) {
    List<ParsedSchemaHolder> holders = new ArrayList<>();
    for (ParsedSchema schema : history.subList(0, k)) {
      holders.add(new SimpleParsedSchemaHolder(schema));
    }
    return holders;
  }

  private static AvroSchema avro(String fields) {
    return new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"fields\":[" + fields + "]}");
  }

  private static ProtobufSchema proto(String body) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage Row {\n  " + body
        + "\n}\n");
  }

  private static ProtobufSchema rootedWrapper(String file, String root) {
    return new ProtobufSchema("syntax = \"proto3\";\nimport \"confluent/meta.proto\";\n"
        + "import public \"o.proto\";\n"
        + "option (confluent.file_meta) = { params: [ { key: \"root\", value: \"" + root
        + "\" } ] };\n", Collections.singletonList(new SchemaReference("o.proto", "o", 1)),
        Collections.singletonMap("o.proto", file), null, null);
  }

  private static JsonSchema json(String properties) {
    return new JsonSchema("{\"type\":\"object\",\"properties\":" + properties + "}");
  }
}
