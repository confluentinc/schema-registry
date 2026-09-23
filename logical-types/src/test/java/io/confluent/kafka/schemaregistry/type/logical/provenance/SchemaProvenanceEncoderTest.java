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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaProvenanceEncoderTest {

  private static final AvroSchema V1 = new AvroSchema(
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}");
  private static final AvroSchema V2 = new AvroSchema(
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"int\"},"
          + "{\"name\":\"full_name\",\"type\":\"string\",\"aliases\":[\"name\"]},"
          + "{\"name\":\"since\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"},"
          + "\"default\":1}]}");

  @Test
  void carriesSchemaIdsVersionsAndPids() {
    SchemaProvenance encoded = encode(Arrays.asList(1001, 1002), Arrays.asList(1, 2), true);

    assertThat(encoded.getSubject()).isEqualTo("s");
    assertThat(encoded.getVersions()).extracting(ProvenanceVersion::getVersion)
        .containsExactly(1, 2);
    assertThat(encoded.getVersions()).extracting(ProvenanceVersion::getId)
        .containsExactly(1001, 1002);
    // The rename keeps its pid; the added column takes a new one.
    assertThat(encoded.getVersions().get(1).getFields())
        .extracting(ProvenanceField::getPid).containsExactly(1, 2, 3);
    assertThat(encoded.getVersions().get(1).getFields().get(1).getNames())
        .containsExactly("full_name");
  }

  @Test
  void defaultsAreInTheirWireForm() {
    // The date default reaches the wire as RFC 3339, not as a LocalDate or Avro's day count.
    ProvenanceField since = encode(Arrays.asList(1001, 1002), null, false)
        .getVersions().get(1).getFields().get(2);
    assertThat(since.getDefaultValue()).isEqualTo("1970-01-02");
  }

  @Test
  void namesAndVersionsAreLeftOutWhenNotAskedForOrNotKnown() {
    SchemaProvenance encoded = encode(Arrays.asList(1001, 1002), null, false);
    assertThat(encoded.getVersions()).extracting(ProvenanceVersion::getVersion)
        .containsOnlyNulls();
    assertThat(encoded.getVersions().get(0).getFields())
        .extracting(ProvenanceField::getNames).containsOnlyNulls();
  }

  @Test
  void anEmptyProtobufDefaultSurvives() {
    ProtobufSchema proto = new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\nmessage Row { string name = 1; }\n");
    SchemaProvenance encoded = SchemaProvenanceEncoder.encode("s",
        ProvenanceComputer.report(
            Collections.singletonList(LogicalTypeConversion.toLogicalType(proto)),
            IdentityPolicy.PROTOBUF),
        Collections.singletonList(7), null, false);
    assertThat(encoded.getVersions().get(0).getFields().get(0).getDefaultValue()).isEqualTo("");
  }

  @Test
  void rejectsListsThatDoNotMatchTheVersions() {
    assertThatThrownBy(() -> encode(Collections.singletonList(1001), null, false))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static SchemaProvenance encode(
      java.util.List<Integer> schemaIds, java.util.List<Integer> versions, boolean verbose) {
    return SchemaProvenanceEncoder.encode("s",
        ProvenanceComputer.report(Arrays.asList(
            LogicalTypeConversion.toLogicalType(V1), LogicalTypeConversion.toLogicalType(V2)),
            IdentityPolicy.AVRO),
        schemaIds, versions, verbose);
  }
}
