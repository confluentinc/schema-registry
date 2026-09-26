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
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Provenance computed from real Avro schema documents, through the Avro reader.
 *
 * <p>Most tests in this package hand {@link ProvenanceComputer} a {@code LogicalType} built by
 * hand, which says nothing about whether an Avro document's aliases actually reach it. These pin
 * the whole path: {@code aliases} on a field or a record, read into {@code Schema.AVRO_ALIASES},
 * and matched.
 *
 * <p>They also pin the reader's shape, which is not uniform. A top-level record is <b>unwrapped</b>
 * — the root schema is an inline {@code STRUCT} and the record's name moves to
 * {@link LogicalType#getName()} — so a rename of the root record is invisible to provenance. A
 * <b>nested</b> record becomes a {@code NAMED_TYPE_REF}, so its name and aliases decide whether its
 * fields continue.
 */
class ProvenanceAvroSchemaTest {

  @Test
  void fieldAliasesReachProvenanceThroughTheReader() {
    // v1 {id, name} -> v2 renames name to full_name -> v3 reuses the retired name for a new
    // column. The rename keeps its pid; the reused name is a new column.
    Pids pids = compute(
        order("{'name':'id','type':'long'},"
            + "{'name':'name','type':'string'}"),
        order("{'name':'id','type':'long'},"
            + "{'name':'full_name','type':'string','aliases':['name']}"),
        order("{'name':'id','type':'long'},"
            + "{'name':'full_name','type':'string'},"
            + "{'name':'name','type':'string','default':''}"));

    assertThat(pids.at(2, 1)).isEqualTo(pids.at(0, 1));
    assertThat(pids.isNew(2, 2)).isTrue();
    // What a deserializer would build its projection from: the v1 writer's name column feeds the
    // reader's full_name, and the reader's name column has no source at all.
    assertThat(pids.shared(2, 0)).containsExactly(Arrays.asList(0), Arrays.asList(1));
  }

  @Test
  void nestedRecordAliasesReachProvenanceThroughTheReader() {
    // Avro resolves record aliases to full names at parse time, so the alias is the old fullname.
    // The nested record's use survives the rename, which is what keeps its fields.
    Pids pids = compute(
        order("{'name':'addr','type':{'type':'record','name':'Address',"
            + "'fields':[{'name':'city','type':'string'}]}}"),
        order("{'name':'addr','type':{'type':'record','name':'Location',"
            + "'aliases':['acme.Address'],'fields':[{'name':'city','type':'string'}]}}"));

    assertThat(pids.at(1, 0, 0)).isNotNull().isEqualTo(pids.at(0, 0, 0));
  }

  @Test
  void aDroppedAndReaddedFieldIsNewThroughTheReader() {
    Pids pids = compute(
        order("{'name':'id','type':'long'},{'name':'zip','type':'string'}"),
        order("{'name':'id','type':'long'}"),
        order("{'name':'id','type':'long'},{'name':'zip','type':'string'}"));

    assertThat(pids.at(1, 1)).isNull();
    assertThat(pids.isNew(2, 1)).isTrue();
    assertThat(pids.shared(2, 0)).containsExactly(Arrays.asList(0));
  }

  @Test
  void aTopLevelRecordRenameIsInvisibleBecauseTheReaderUnwrapsIt() {
    // Not an alias resolution: the root record simply is not a location, so its name never
    // matters. Worth pinning, because it is the opposite of the nested case above.
    Pids pids = compute(
        "{'type':'record','name':'Order','namespace':'acme','fields':["
            + "{'name':'id','type':'long'}]}",
        "{'type':'record','name':'Purchase','namespace':'acme','fields':["
            + "{'name':'id','type':'long'}]}");

    assertThat(pids.at(1, 0)).isEqualTo(pids.at(0, 0));
    assertThat(pids.version(0).keySet()).containsExactly(Collections.singletonList(0));
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static Pids compute(String... avroSchemas) {
    List<LogicalType> history = new ArrayList<>();
    for (String schema : avroSchemas) {
      history.add(AvroToLogicalTypeConverter.toLogicalType(
          new AvroSchema(schema.replace('\'', '"'))));
    }
    return Pids.of(history, IdentityPolicy.AVRO);
  }

  /** An {@code acme.Order} record wrapping the given field declarations. */
  private static String order(String fields) {
    return "{'type':'record','name':'Order','namespace':'acme','fields':[" + fields + "]}";
  }
}
