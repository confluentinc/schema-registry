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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Provenance computed from real Avro schema documents, through the Avro reader.
 *
 * <p>Every other test in this package hands {@link ProvenanceComputer} a {@code LogicalType} built
 * by hand, which says nothing about whether an Avro document's aliases actually reach it. These
 * pin the whole path: {@code aliases} on a field or a record, read into
 * {@code Schema.AVRO_ALIASES}, resolved as identity.
 *
 * <p>They also pin the reader's shape, which is not uniform. A top-level record is <b>unwrapped</b>
 * — the root schema is an inline {@code STRUCT} and the record's name moves to
 * {@link LogicalType#getName()} — so its fields sit at {@link PathKey#ofRoot()} and a rename of the
 * root record is invisible to provenance. A <b>nested</b> record becomes a {@code NAMED_TYPE_REF}
 * with an entry in {@code getNamedTypes()}, so its fields sit under
 * {@link PathKey#ofNamedType} and its aliases do carry identity.
 */
class ProvenanceAvroSchemaTest {

  @Test
  void fieldAliasesReachProvenanceThroughTheReader() {
    // v1 {id, name} -> v2 renames name to full_name -> v3 reuses the retired name for a new
    // column. The rename keeps its provenance; the reused name is a separate entity.
    ProvenanceResult result = compute(
        order("{'name':'id','type':'long'},"
            + "{'name':'name','type':'string'}"),
        order("{'name':'id','type':'long'},"
            + "{'name':'full_name','type':'string','aliases':['name']}"),
        order("{'name':'id','type':'long'},"
            + "{'name':'full_name','type':'string'},"
            + "{'name':'name','type':'string','default':''}"));

    PathKey id = PathKey.ofRoot().child(0);
    PathKey renamed = PathKey.ofRoot().child(1);
    PathKey reused = PathKey.ofRoot().child(2);

    assertThat(result.at(2, renamed)).isEqualTo(result.at(0, renamed));
    assertThat(result.at(2, reused)).isNotNull().isNotEqualTo(result.at(0, renamed));
    assertThat(result.at(2, reused).getPresenceStartVersion()).isEqualTo(2);

    // What a deserializer would build its projection from: the v1 writer's name column feeds the
    // reader's full_name, and the reader's name column has no source at all.
    assertThat(result.correspondence(2, 0))
        .containsOnly(entry(id, id), entry(renamed, renamed))
        .doesNotContainKey(reused);
  }

  @Test
  void nestedRecordAliasesReachProvenanceThroughTheReader() {
    // Avro resolves record aliases to full names at parse time, so the alias is the old fullname.
    // The nested record's identity survives the rename, which is what keeps its fields in scope.
    ProvenanceResult result = compute(
        order("{'name':'addr','type':{'type':'record','name':'Address',"
            + "'fields':[{'name':'city','type':'string'}]}}"),
        order("{'name':'addr','type':{'type':'record','name':'Location',"
            + "'aliases':['acme.Address'],'fields':[{'name':'city','type':'string'}]}}"));

    PathKey before = PathKey.ofNamedType("acme.Address").child(0);
    PathKey after = PathKey.ofNamedType("acme.Location").child(0);
    assertThat(result.at(1, after)).isNotNull().isEqualTo(result.at(0, before));
    assertThat(result.correspondence(0, 1)).containsEntry(before, after);
  }

  @Test
  void aDroppedAndReaddedFieldGetsANewIntervalThroughTheReader() {
    ProvenanceResult result = compute(
        order("{'name':'id','type':'long'},{'name':'zip','type':'string'}"),
        order("{'name':'id','type':'long'}"),
        order("{'name':'id','type':'long'},{'name':'zip','type':'string'}"));

    PathKey zip = PathKey.ofRoot().child(1);
    assertThat(result.at(1, zip)).isNull();
    assertThat(result.at(2, zip)).isNotNull().isNotEqualTo(result.at(0, zip));
    assertThat(result.at(2, zip).getPresenceStartVersion()).isEqualTo(2);
    assertThat(result.intersection(0, 2))
        .containsExactly(result.at(0, PathKey.ofRoot().child(0)));
  }

  @Test
  void aTopLevelRecordRenameIsInvisibleBecauseTheReaderUnwrapsIt() {
    // Not an alias resolution: the root record simply is not an entity, so its name never enters
    // any identity. Worth pinning, because it is the opposite of the nested case above.
    ProvenanceResult result = compute(
        "{'type':'record','name':'Order','namespace':'acme','fields':["
            + "{'name':'id','type':'long'}]}",
        "{'type':'record','name':'Purchase','namespace':'acme','fields':["
            + "{'name':'id','type':'long'}]}");

    PathKey id = PathKey.ofRoot().child(0);
    assertThat(result.at(1, id)).isEqualTo(result.at(0, id));
    assertThat(result.byPath(0).keySet()).containsExactly(id);
  }

  @Test
  void defaultsAreRekeyedOntoEveryInlinedPath() {
    // acme.Addr is used twice. The reader converts its body once, so it records city's default
    // under the FIRST use site only; a consumer that inlined the type needs it at both.
    LogicalType lt = AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(
        order("{'name':'a','type':{'type':'record','name':'Addr','fields':["
            + "{'name':'city','type':'string','default':'?'}]}},"
            + "{'name':'b','type':'acme.Addr'}").replace('\'', '"')));

    ProvenanceResult result = ProvenanceComputer.compute(
        Collections.singletonList(lt), IdentityPolicy.AVRO);
    Map<List<Integer>, Object> expanded = result.expandedDefaults(0);

    assertThat(expanded).containsEntry(Arrays.asList(0, 0), "?");
    assertThat(expanded).containsEntry(Arrays.asList(1, 0), "?");
  }

  @Test
  void aRootFieldDefaultIsKeyedByItsPosition() {
    LogicalType lt = AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(
        order("{'name':'id','type':'long'},{'name':'note','type':'string','default':'n/a'}")
            .replace('\'', '"')));

    ProvenanceResult result = ProvenanceComputer.compute(
        Collections.singletonList(lt), IdentityPolicy.AVRO);
    assertThat(result.expandedDefaults(0)).containsEntry(Collections.singletonList(1), "n/a");
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static ProvenanceResult compute(String... avroSchemas) {
    List<LogicalType> history = new ArrayList<>();
    for (String schema : avroSchemas) {
      history.add(AvroToLogicalTypeConverter.toLogicalType(
          new AvroSchema(schema.replace('\'', '"'))));
    }
    return ProvenanceComputer.compute(history, IdentityPolicy.AVRO);
  }

  /** An {@code acme.Order} record wrapping the given field declarations. */
  private static String order(String fields) {
    return "{'type':'record','name':'Order','namespace':'acme','fields':[" + fields + "]}";
  }

  private static Map.Entry<PathKey, PathKey> entry(PathKey from, PathKey to) {
    return new AbstractMap.SimpleEntry<>(from, to);
  }
}
