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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Verifies that proto3 scalar fields without explicit presence get the
 * proto-spec implicit default in {@link LogicalType#getDefaultValues()}.
 * Fields with presence (proto3 {@code optional}, fields in oneofs, MESSAGE
 * types, repeated) are skipped.
 */
class ProtoReaderProto3ImplicitDefaultsTest {

  @Test
  void implicitScalarsCaptured() {
    String protoText =
        "syntax = \"proto3\";\n"
            + "package test;\n"
            + "message Row {\n"
            + "  int32 i = 1;\n"           // no presence -> implicit 0
            + "  int64 l = 2;\n"           // no presence -> implicit 0L
            + "  float f = 3;\n"           // no presence -> implicit 0.0f
            + "  double d = 4;\n"          // no presence -> implicit 0.0
            + "  bool b = 5;\n"            // no presence -> implicit false
            + "  string s = 6;\n"          // no presence -> implicit ""
            + "  bytes by = 7;\n"          // no presence -> implicit empty bytes
            + "  Color c = 8;\n"           // no presence -> implicit RED (first decl)
            + "  optional int32 oi = 9;\n" // presence -> SKIP
            + "}\n"
            + "enum Color { RED = 0; GREEN = 1; BLUE = 2; }\n";

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(
        new ProtobufSchema(protoText));

    Map<List<Integer>, Object> defaults = lt.getDefaultValues();
    assertThat(defaults).containsEntry(List.of(0), 0);
    assertThat(defaults).containsEntry(List.of(1), 0L);
    assertThat(defaults).containsEntry(List.of(2), 0.0f);
    assertThat(defaults).containsEntry(List.of(3), 0.0);
    assertThat(defaults).containsEntry(List.of(4), false);
    assertThat(defaults).containsEntry(List.of(5), "");
    // bytes implicit default is ByteString.EMPTY.
    assertThat(defaults).containsEntry(List.of(6), ByteString.EMPTY);
    // enum implicit default is the first declared value's EnumValueDescriptor.
    assertThat(defaults.get(List.of(7))).isInstanceOf(EnumValueDescriptor.class);
    assertThat(((EnumValueDescriptor) defaults.get(List.of(7))).getName())
        .isEqualTo("RED");
    // optional int32 has presence -> no implicit default recorded.
    assertThat(defaults).doesNotContainKey(List.of(8));
  }

  @Test
  void repeatedAndMessageSkipped() {
    // Row is the root (first message); Inner is a peer, so its defaults sit under Row.nested.
    String protoText =
        "syntax = \"proto3\";\n"
            + "package test;\n"
            + "message Row {\n"
            + "  repeated int32 arr = 1;\n"   // repeated -> emptyList default
            + "  map<string, int32> m = 2;\n" // map      -> emptyMap default
            + "  Inner nested = 3;\n"         // MESSAGE -> SKIP (no scalar default)
            + "  int32 scalar = 4;\n"         // implicit 0
            + "}\n"
            + "message Inner { int32 x = 1; }\n";

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(
        new ProtobufSchema(protoText));

    Map<List<Integer>, Object> defaults = lt.getDefaultValues();
    // Empty-collection defaults (verify the proto3-implicit pass doesn't
    // double-write or override the repeated/map empty defaults):
    assertThat(defaults).containsEntry(List.of(0), Collections.emptyList());
    assertThat(defaults).containsEntry(List.of(1), Collections.emptyMap());
    // MESSAGE field -> no entry at the field's own indexPath.
    assertThat(defaults).doesNotContainKey(List.of(2));
    // Scalar implicit default still fires for the proto3 int32.
    assertThat(defaults).containsEntry(List.of(3), 0);
    // Inner.x (proto3 scalar) gets the implicit default too, under the field using Inner.
    assertThat(defaults).containsEntry(List.of(2, 0), 0);
  }

  @Test
  void anImportedMessagesDefaultsAreUnderTheFieldUsingIt() {
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(withLeaf(
        "message Row {\n  int32 id = 1;\n  com.Foo foo = 2;\n}\n",
        "message Foo {\n  string id = 1;\n}\n"));

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0), 0), Map.entry(List.of(1, 0), ""));
  }

  @Test
  void aMessageImportedTwiceHasItsDefaultsUnderEachUse() {
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(withLeaf(
        "message Row {\n  int32 id = 1;\n  bool b = 2;\n  com.Foo f1 = 3;\n  com.Foo f2 = 4;\n}\n",
        "message Foo {\n  string s = 1;\n  int64 n = 2;\n}\n"));

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0), 0), Map.entry(List.of(1), false),
        Map.entry(List.of(2, 0), ""), Map.entry(List.of(2, 1), 0L),
        Map.entry(List.of(3, 0), ""), Map.entry(List.of(3, 1), 0L));
  }

  @Test
  void aPeerMessagesDefaultsAreUnderTheFieldUsingItNotItsFileIndex() {
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\n"
            + "message Row {\n  int32 id = 1;\n  bool b = 2;\n  Foo foo = 3;\n}\n"
            + "message Foo {\n  string id = 1;\n}\n"));

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0), 0), Map.entry(List.of(1), false), Map.entry(List.of(2, 0), ""));
  }

  @Test
  void aSharedMessageAsARepeatedElementOrMapValueHasItsDefaultsWhereInliningPutsThem() {
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(withLeaf(
        "message Row {\n  repeated com.Foo items = 1;\n  map<string, com.Foo> byKey = 2;\n}\n",
        "message Foo {\n  int32 x = 1;\n}\n"));

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0), Collections.emptyList()), Map.entry(List.of(0, 0), 0),
        Map.entry(List.of(1), Collections.emptyMap()), Map.entry(List.of(1, 1, 0), 0));
  }

  @Test
  void aRecursiveNestedMessagesDefaultsStopAtItsFirstRecurrence() {
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\n"
            + "message Row {\n  Node n = 1;\n"
            + "  message Node {\n    int32 v = 1;\n    Node next = 2;\n  }\n}\n"));

    assertThat(lt.getDefaultValues()).containsOnly(Map.entry(List.of(0, 0), 0));
  }

  @Test
  void aMultiMessageRootHasEachMessagesDefaultsUnderItsColumn() {
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\n"
            + "message Row {\n  int32 id = 1;\n  Foo foo = 2;\n}\n"
            + "message Foo {\n  string s = 1;\n}\n"), true);

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0, 0), 0), Map.entry(List.of(0, 1, 0), ""),
        Map.entry(List.of(1, 0), ""));
  }

  @Test
  void aMessageShapedLikeAMapEntryRecordsItsFieldsWhereItIsAStruct() {
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\n"
            + "message Row {\n  repeated MapEntry props = 1;\n  MapEntry one = 2;\n}\n"
            + "message MapEntry {\n  string key = 1;\n  string value = 2;\n}\n"));

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0), Collections.emptyMap()),
        Map.entry(List.of(1, 0), ""), Map.entry(List.of(1, 1), ""));
  }

  @Test
  void aChainOfPeerMessagesConvertsWithItsLastDefaultAtTheEnd() throws Exception {
    // Each message holds the next: references, not nesting, so no depth limit applies, and
    // placing the defaults takes no stack per link, even on a small one.
    ProtobufSchema schema = new ProtobufSchema(chain(10000, ""));
    AtomicReference<Object> result = new AtomicReference<>();
    Thread small = new Thread(null, () -> {
      try {
        result.set(ProtoToLogicalTypeConverter.toLogicalType(schema).getDefaultValues());
      } catch (RuntimeException e) {
        result.set(e);
      }
    }, "small-stack", 512 * 1024);
    small.start();
    small.join();

    assertThat(result.get()).isEqualTo(Map.of(Collections.nCopies(10001, 0), 0));
  }

  @Test
  void aChainWithADefaultAtEveryLinkHasEachOnceAtItsDepth() {
    // Placed straight into the result: memory follows its size, not every suffix of the chain.
    LogicalType lt = assertTimeoutPreemptively(Duration.ofSeconds(30), () ->
        ProtoToLogicalTypeConverter.toLogicalType(
            new ProtobufSchema(chain(3000, "int32 v = 2; "))));

    Map<List<Integer>, Object> defaults = lt.getDefaultValues();
    assertThat(defaults).hasSize(3001);
    // v is each message's field 0 and next its field 1, so M3000's x is 3,000 nexts down.
    List<Integer> deepest = new ArrayList<>(Collections.nCopies(3000, 1));
    deepest.add(0);
    assertThat(defaults).containsEntry(deepest, 0);
  }

  @Test
  void mutuallyRecursiveMessagesUsedApartEachStopOnlyWhereTheyRecur() {
    // Each use of A or B enters their cycle anew: each is placed once there, never below itself.
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\n"
            + "message Root {\n  A a = 1;\n  B b = 2;\n  int32 n = 3;\n}\n"
            + "message A {\n  int32 x = 1;\n  B b = 2;\n}\n"
            + "message B {\n  int32 y = 1;\n  A a = 2;\n}\n"));

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0, 0), 0), Map.entry(List.of(0, 1, 0), 0),
        Map.entry(List.of(1, 0), 0), Map.entry(List.of(1, 1, 0), 0), Map.entry(List.of(2), 0));
  }

  @Test
  void aRecursiveMessageUsedTwiceAsSiblingsHasItsDefaultsAtBoth() {
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\n"
            + "message A {\n  B first = 1;\n  B second = 2;\n}\n"
            + "message B {\n  int32 x = 1;\n  A back = 2;\n}\n"));

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0, 0), 0), Map.entry(List.of(1, 0), 0));
  }

  @Test
  void defaultsMatchInliningCutWhereAMessageRecursForRandomFiles() {
    // An independent oracle: each message inlined at its uses, cut where it recurs on the path.
    Random random = new Random(20261006);
    for (int file = 0; file < 500; file++) {
      int messages = 2 + random.nextInt(5);
      List<List<Integer>> fields = new ArrayList<>();
      StringBuilder text = new StringBuilder("syntax = \"proto3\";\npackage p;\n");
      for (int m = 0; m < messages; m++) {
        // Each field an int32 (-1) or the number of the message it holds, itself included.
        List<Integer> own = new ArrayList<>();
        text.append("message M").append(m).append(" {");
        for (int f = 0, count = 1 + random.nextInt(3); f < count; f++) {
          int held = random.nextInt(messages + 1) - 1;
          own.add(held);
          text.append(held < 0 ? " int32" : " M" + held).append(" f").append(f)
              .append(" = ").append(f + 1).append(";");
        }
        fields.add(own);
        text.append(" }\n");
      }
      Map<List<Integer>, Object> expected = new HashMap<>();
      inline(fields, 0, new ArrayList<>(), new HashSet<>(Collections.singleton(0)), expected);

      assertThat(ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(text.toString()))
          .getDefaultValues()).as(text.toString()).isEqualTo(expected);
    }
  }

  private static void inline(List<List<Integer>> fields, int message, List<Integer> path,
      Set<Integer> onPath, Map<List<Integer>, Object> out) {
    List<Integer> own = fields.get(message);
    for (int f = 0; f < own.size(); f++) {
      List<Integer> at = new ArrayList<>(path);
      at.add(f);
      int held = own.get(f);
      if (held < 0) {
        out.put(at, 0);
      } else if (onPath.add(held)) {
        inline(fields, held, at, onPath, out);
        onPath.remove(held);
      }
    }
  }

  @Test
  void aLongCycleWithItsOnlyDefaultAtTheEndIsSearchedOnce() {
    // M0 holds M1, …, the last holds M0 again and the only default: one path, found once.
    StringBuilder ring = new StringBuilder("syntax = \"proto3\";\npackage p;\n");
    for (int i = 0; i < 19999; i++) {
      ring.append("message M").append(i).append(" { M").append(i + 1).append(" next = 1; }\n");
    }
    ring.append("message M19999 { M0 next = 1; int32 x = 2; }\n");
    ProtobufSchema schema = new ProtobufSchema(ring.toString());
    LogicalType lt = assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
        ProtoToLogicalTypeConverter.toLogicalType(schema));

    List<Integer> last = new ArrayList<>(Collections.nCopies(19999, 0));
    last.add(1);
    assertThat(lt.getDefaultValues()).containsOnly(Map.entry(last, 0));
  }

  @Test
  void siblingsLeadingOnlyBackToAnOpenTypeAreRejectedOnce() {
    // A holds the only default and B1..B20000; each Bi leads only back to A: the first search
    // proves every Bi dead, and the rest are rejected without searching again.
    StringBuilder fan = new StringBuilder(
        "syntax = \"proto3\";\npackage p;\nmessage A { int32 x = 1;");
    for (int i = 1; i <= 20000; i++) {
      fan.append(" B").append(i).append(" b").append(i).append(" = ").append(i + 1).append(";");
    }
    fan.append(" }\n");
    for (int i = 1; i < 20000; i++) {
      fan.append("message B").append(i).append(" { B").append(i + 1).append(" next = 1; }\n");
    }
    fan.append("message B20000 { A a = 1; }\n");
    ProtobufSchema schema = new ProtobufSchema(fan.toString());
    LogicalType lt = assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
        ProtoToLogicalTypeConverter.toLogicalType(schema));

    assertThat(lt.getDefaultValues()).containsOnly(Map.entry(List.of(0), 0));
  }

  @Test
  void aConversionPlacesNoDefaultsUntilTheyAreRead() {
    // Each message holds the next twice: 2^30 paths to the last one's default. Converting costs
    // nothing for them; callers that never read defaults never pay for them.
    LogicalType lt = assertTimeoutPreemptively(Duration.ofSeconds(3), () ->
        ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(diamond(30))));
    assertThat(lt.getRootSchema().getFields()).hasSize(2);

    Map<List<Integer>, Object> small =
        ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(diamond(3))).getDefaultValues();
    assertThat(small).hasSize(8).containsEntry(List.of(1, 0, 1, 0), 0);
  }

  // M0 .. M<n-1>, each holding the next as a and b; M<n> holds the only default.
  private static String diamond(int n) {
    StringBuilder text = new StringBuilder("syntax = \"proto3\";\npackage p;\n");
    for (int i = 0; i < n; i++) {
      text.append("message M").append(i).append(" { M").append(i + 1).append(" a = 1; M")
          .append(i + 1).append(" b = 2; }\n");
    }
    return text.append("message M").append(n).append(" { int32 x = 1; }\n").toString();
  }

  @Test
  void aBranchingCycleIsNotWalkedPathByPath() {
    // A holds B0, each Bi holds the next twice, the last holds A: 2^30 paths, one cycle, whose
    // types are each placed once.
    StringBuilder text = new StringBuilder("syntax = \"proto3\";\npackage p;\n"
        + "message Root {\n  A a = 1;\n}\nmessage A {\n  int32 x = 1;\n  B0 b = 2;\n}\n");
    for (int i = 0; i < 30; i++) {
      String next = i < 29 ? "B" + (i + 1) : "A";
      text.append("message B").append(i).append(" { ").append(next).append(" l = 1; ")
          .append(next).append(" r = 2; }\n");
    }
    LogicalType lt = assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
        ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(text.toString())));

    assertThat(lt.getDefaultValues()).containsOnly(Map.entry(List.of(0, 0), 0));
  }

  // A file of n messages, each holding the next after its own fields, ending in M<n> { x }.
  private static String chain(int n, String fields) {
    StringBuilder chain = new StringBuilder("syntax = \"proto3\";\npackage p;\n");
    for (int i = 0; i < n; i++) {
      chain.append("message M").append(i).append(" { ").append(fields)
          .append("M").append(i + 1).append(" next = 1; }\n");
    }
    return chain.append("message M").append(n).append(" { int32 x = 1; }\n").toString();
  }

  // A proto3 file in package p importing leaf.proto, whose messages are in package com.
  private static ProtobufSchema withLeaf(String messages, String leafMessages) {
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("leaf.proto", "syntax = \"proto3\";\npackage com;\n" + leafMessages);
    return new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\nimport \"leaf.proto\";\n" + messages,
        List.of(new SchemaReference("leaf.proto", "leaf", 1)), resolved, null, null, null, null);
  }

  @Test
  void proto2WithoutExplicitDefaultStillSkipped() {
    // Proto2 scalars have presence even without [optional], so !hasPresence()
    // filters them out and they get NO implicit default.
    String protoText =
        "syntax = \"proto2\";\n"
            + "package test;\n"
            + "message Row {\n"
            + "  optional int32 a = 1;\n"          // no explicit default
            + "  optional int32 b = 2 [default = 7];\n" // explicit default
            + "}\n";

    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(
        new ProtobufSchema(protoText));

    Map<List<Integer>, Object> defaults = lt.getDefaultValues();
    // No implicit default for 'a' — proto2 fields have presence.
    assertThat(defaults).doesNotContainKey(List.of(0));
    // Explicit proto2 default for 'b' still captured.
    assertThat(defaults).containsEntry(List.of(1), 7);
  }
}
