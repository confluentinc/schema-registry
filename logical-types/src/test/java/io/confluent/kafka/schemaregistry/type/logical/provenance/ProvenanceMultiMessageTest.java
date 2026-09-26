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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
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
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(proto(ORDER, REFUND, LINE), true);

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
    LogicalType lt = ProtoToLogicalTypeConverter.toLogicalType(proto(REFUND), true);

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
  void aSubjectGrowingFromOneMessageToTwoKeepsItsIds() {
    List<ProvenanceVersion> versions = compute(proto(REFUND), proto(ORDER, REFUND, LINE));
    Map<List<Integer>, Integer> v1 = pids(versions.get(0));
    Map<List<Integer>, Integer> v2 = pids(versions.get(1));

    assertThat(v2.get(path(1))).isEqualTo(v1.get(path(0)));
    assertThat(v2.get(path(1, 1))).isEqualTo(v1.get(path(0, 1)));
  }

  @Test
  void withoutTheFlagOnlyTheFirstMessageIsRoot() {
    List<ParsedSchema> schemas = Arrays.asList(proto(ORDER, REFUND, LINE));
    SchemaProvenance provenance = ProvenanceHistory.compute("s",
        Arrays.asList(new ProvenanceHistory.Entry(1, 1, false)), schemas, false);

    assertThat(pids(provenance.getVersions().get(0)).keySet())
        .contains(path(0), path(1), path(2), path(2, 0))
        .doesNotContain(path(1, 1));
  }

  // -------------------------------------------------------------------------------------------

  private static List<ProvenanceVersion> compute(ProtobufSchema... versions) {
    List<ProvenanceHistory.Entry> history = new ArrayList<>();
    for (int i = 0; i < versions.length; i++) {
      history.add(new ProvenanceHistory.Entry(i + 1, i + 1, false));
    }
    return ProvenanceHistory.compute("s", history, Arrays.asList(versions), true).getVersions();
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

  private static ProtobufSchema proto(String... messages) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage io.confluent;\n"
        + String.join("\n", messages) + "\n");
  }
}
