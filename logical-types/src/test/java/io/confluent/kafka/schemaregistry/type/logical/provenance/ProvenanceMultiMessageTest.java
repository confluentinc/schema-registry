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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProvenanceMultiMessageTest {

  private static final String ORDER = "message Order { int32 id = 1; string item = 2; Line line = 3; }";
  private static final String REFUND = "message Refund { int32 id = 1; int32 amount = 2; }";
  private static final String LINE = "message Line { string sku = 1; }";

  @Test
  void theRootIsAStructOfEveryTopLevelMessageInFileOrder() {
    LogicalType lt = ProtoToLogicalTypeConverter.toMultiMessageLogicalType(proto(ORDER, REFUND, LINE));

    Schema root = lt.getRootSchema();
    assertThat(root.getFields()).extracting(Schema.Field::getName)
        .containsExactly("io.confluent.Order", "io.confluent.Refund", "io.confluent.Line");
    assertThat(root.getFields().get(0).getSchema().getType())
        .isEqualTo(Schema.Type.NAMED_TYPE_REF);
    assertThat(lt.getNamedTypes())
        .containsKeys("io.confluent.Order", "io.confluent.Refund", "io.confluent.Line");
  }

  @Test
  void aSingleMessageFileIsWrappedAllTheSame() {
    LogicalType lt = ProtoToLogicalTypeConverter.toMultiMessageLogicalType(proto(REFUND));

    assertThat(lt.getRootSchema().getFields()).extracting(Schema.Field::getName)
        .containsExactly("io.confluent.Refund");
  }

  @Test
  void pathsTakeTheMessageStepAndEachLocationItsOwnId() {
    Map<List<Integer>, Integer> v1 = pids(compute(proto(ORDER, REFUND, LINE)).get(0));

    assertThat(v1.keySet()).contains(
        path(0), path(0, 0), path(0, 1), path(0, 2), path(0, 2, 0),
        path(1), path(1, 0), path(1, 1), path(2), path(2, 0));
    // Line is both a top-level message and Order's field type: two locations, two ids.
    assertThat(v1.get(path(2, 0))).isNotEqualTo(v1.get(path(0, 2, 0)));
  }

  @Test
  void reorderingTheFilesMessagesKeepsEveryId() {
    List<ProvenanceVersion> versions = compute(proto(ORDER, REFUND, LINE), proto(REFUND, LINE, ORDER));
    Map<List<Integer>, Integer> v1 = pids(versions.get(0));
    Map<List<Integer>, Integer> v2 = pids(versions.get(1));

    assertThat(v2.get(path(2))).isEqualTo(v1.get(path(0)));        // Order
    assertThat(v2.get(path(2, 1))).isEqualTo(v1.get(path(0, 1)));  // Order.item
    assertThat(v2.get(path(0, 1))).isEqualTo(v1.get(path(1, 1)));  // Refund.amount
    assertThat(v2.get(path(1, 0))).isEqualTo(v1.get(path(2, 0)));  // Line.sku
  }

  @Test
  void renamingOneMessageRestartsOnlyThatMessage() {
    String returned = "message Return { int32 id = 1; int32 amount = 2; }";
    List<ProvenanceVersion> versions = compute(proto(ORDER, REFUND, LINE),
        proto(ORDER, returned, LINE));
    Map<List<Integer>, Integer> v1 = pids(versions.get(0));
    Map<List<Integer>, Integer> v2 = pids(versions.get(1));

    assertThat(v2.get(path(1))).isNotIn(v1.values());               // Return
    assertThat(v2.get(path(1, 1))).isNotIn(v1.values());            // Return.amount
    assertThat(v2.get(path(0, 1))).isEqualTo(v1.get(path(0, 1)));  // Order.item
    assertThat(v2.get(path(0, 2, 0))).isEqualTo(v1.get(path(0, 2, 0)));  // Order.line.sku
    assertThat(v2.get(path(2, 0))).isEqualTo(v1.get(path(2, 0)));  // Line.sku
  }

  @Test
  void aSubjectGrowingFromOneMessageToSeveralStartsOver() {
    // Read as its one message, then as all of them: each field moves under its message, a
    // reshape, as the LOGICAL policy and the Metastore have it.
    List<ProvenanceVersion> versions = compute(proto(REFUND), proto(ORDER, REFUND, LINE));
    Map<List<Integer>, Integer> v1 = pids(versions.get(0));
    Map<List<Integer>, Integer> v2 = pids(versions.get(1));

    assertThat(v1).containsOnlyKeys(path(0), path(1));
    assertThat(v2.get(path(1, 1))).isNotIn(v1.values());
  }

  @Test
  void aRootThatMovesToAnotherMessageAndBackStartsOver() {
    // As a reader rooted at each version's root sees it: B is no column of v2, rooted at A.
    String a = "message A { int32 a = 1; }";
    String b = "message B { int32 %s = 1; }";
    List<ProvenanceVersion> versions = compute(rooted("io.confluent.B", a, String.format(b, "x")),
        rooted("io.confluent.A", a, String.format(b, "x")),
        rooted("io.confluent.B", a, String.format(b, "y")));

    assertThat(pids(versions.get(0))).containsOnlyKeys(path(0));
    assertThat(pids(versions.get(2)).get(path(0))).isNotIn(pids(versions.get(0)).values());
  }

  @Test
  void aRecursiveMessageBesideTheRootIsNotWalked() {
    List<ProvenanceVersion> versions = compute(
        rooted("io.confluent.B", "message A { A next = 1; }", "message B { int32 x = 1; }"));

    assertThat(pids(versions.get(0))).containsOnlyKeys(path(0));
  }

  @Test
  void nestedRootsOfTwoMessagesAreTwoMessages() {
    // Both named C, one inside P and one inside Q: each is its own message, not io.confluent.C.
    String file = "syntax = \"proto3\";\npackage io.confluent;\n"
        + "message P { message C { int32 c = 1; } }\nmessage Q { message C { int32 c = 1; } }\n";
    List<ProvenanceVersion> versions = compute(wrapper(file, "io.confluent.P.C"),
        wrapper(file, "io.confluent.Q.C"));

    assertThat(pids(versions.get(1)).get(path(0))).isNotIn(pids(versions.get(0)).values());
  }

  @Test
  void aFileNamingItsRootIsReadAsThatMessage() {
    Map<List<Integer>, Integer> v1 =
        pids(compute(rooted("io.confluent.Refund", ORDER, REFUND, LINE)).get(0));

    assertThat(v1).containsOnlyKeys(path(0), path(1));
  }

  @Test
  void aWrapperIsRootedAtTheMessageItsRootParamPicks() {
    // Int64Value, the third of wrappers.proto's messages, not the file's first, DoubleValue.
    ProtobufSchema wrapper = new ProtobufSchema("syntax = \"proto3\";\n"
        + "import \"confluent/meta.proto\";\nimport public \"google/protobuf/wrappers.proto\";\n"
        + "option (confluent.file_meta) = { params: [ { key: \"logical.root\", value: "
        + "\"google.protobuf.Int64Value\" } ] };\n");
    assertThat(ProvenanceHistory.logicalTypeOf(wrapper).getRootSchema().getType())
        .isEqualTo(Schema.Type.BIGINT);
    // With it, every message is a member, whatever the root param picks.
    assertThat(ProvenanceHistory.wholeLogicalTypeOf(wrapper).getRootSchema().getFields())
        .hasSize(9);
  }

  // -------------------------------------------------------------------------------------------

  private static List<ProvenanceVersion> compute(ProtobufSchema... versions) {
    List<SchemaMetadata> history = new ArrayList<>();
    for (int i = 0; i < versions.length; i++) {
      history.add(new SchemaMetadata(i + 1, i + 1, "PROTOBUF", Collections.emptyList(), ""));
    }
    return ProvenanceHistory.compute("s", history,
        ProvenanceHistory.held(Arrays.<ParsedSchema>asList(versions)))
        .getVersions();
  }

  private static Map<List<Integer>, Integer> pids(ProvenanceVersion version) {
    Map<List<Integer>, Integer> pids = new HashMap<>();
    for (ProvenanceField field : version.getFields()) {
      pids.put(field.getPath(), field.getPid());
    }
    return pids;
  }

  private static List<Integer> path(Integer... steps) {
    return Arrays.asList(steps);
  }

  private static ProtobufSchema rooted(String root, String... messages) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage io.confluent;\n"
        + "import \"confluent/meta.proto\";\n"
        + "option (confluent.file_meta) = { params: [ { key: \"logical.root\", value: \"" + root
        + "\" } ] };\n" + String.join("\n", messages) + "\n");
  }

  // A file of nothing but a public import of file, rooted at root.
  private static ProtobufSchema wrapper(String file, String root) {
    return new ProtobufSchema("syntax = \"proto3\";\nimport \"confluent/meta.proto\";\n"
        + "import public \"leaf.proto\";\n"
        + "option (confluent.file_meta) = { params: [ { key: \"logical.root\", value: \"" + root
        + "\" } ] };\n", Collections.singletonList(new SchemaReference("leaf.proto", "leaf", 1)),
        Collections.singletonMap("leaf.proto", file), null, null);
  }

  private static ProtobufSchema proto(String... messages) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage io.confluent;\n"
        + String.join("\n", messages) + "\n");
  }
}
