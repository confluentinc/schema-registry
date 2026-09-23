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

package io.confluent.kafka.serializers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

public class AvroProvenanceRenamerTest {

  @Test
  public void aPairedFieldTakesTheReaderNameAndAnUnpairedOneANameNothingMatches() {
    Schema writer = record("W", field("a", "\"int\""), field("b", "\"int\""));
    Schema reader = record("R", field("x", "\"int\""), field("b", "\"int\"", "0"));
    // a -> x keeps its pid; the reader's b is a new column that happens to share a name.
    AvroProvenanceRenamer.Renamed renamed = AvroProvenanceRenamer.rename(
        writer, reader, mapping(pids(p(1, 0), p(2, 1)), pids(p(1, 0), p(3, 1))));

    assertEquals("R", renamed.writer.getFullName());
    assertEquals("x", renamed.writer.getFields().get(0).name());
    assertTrue(renamed.writer.getFields().get(1).name().startsWith("__provenance_unmatched_"));
    assertEquals(Schema.create(Schema.Type.INT), renamed.writer.getFields().get(1).schema());
  }

  @Test
  public void theReadersAliasesDoNotMoveAFieldProvenancePlaced() {
    // v1 {name} read under v3 {full_name aliases [name], name}: provenance pairs v1's name with
    // full_name. Left in place, the alias would rename it again.
    Schema writer = record("R", field("name", "\"string\""));
    Schema reader = record("R",
        "{\"name\":\"full_name\",\"type\":\"string\",\"aliases\":[\"name\"]}",
        field("name", "\"string\"", "\"unknown\""));
    AvroProvenanceRenamer.Renamed renamed = AvroProvenanceRenamer.rename(
        writer, reader, mapping(pids(p(1, 0)), pids(p(1, 0), p(2, 1))));

    assertEquals("full_name", renamed.writer.getFields().get(0).name());
    assertTrue(renamed.reader.getField("full_name").aliases().isEmpty());
    assertEquals("full_name",
        Schema.applyAliases(renamed.writer, renamed.reader).getFields().get(0).name());
  }

  @Test
  public void aSharedRecordIsRenamedTheSameWayAtEverySite() {
    AvroProvenanceRenamer.Renamed renamed = AvroProvenanceRenamer.rename(
        shared("city"), shared("town"),
        mapping(pids(p(1, 0), p(2, 0, 0), p(3, 1), p(4, 1, 0)),
            pids(p(1, 0), p(2, 0, 0), p(3, 1), p(4, 1, 0))));

    Schema home = renamed.writer.getField("home").schema();
    assertEquals("town", home.getFields().get(0).name());
    assertEquals(home, renamed.writer.getField("work").schema());
  }

  @Test
  public void aSharedRecordNeedingTwoDifferentRenamesFallsBack() {
    // work.city has no counterpart, home.city does: one Address, two definitions.
    assertThrows(ProvenanceUnavailableException.class, () -> AvroProvenanceRenamer.rename(
        shared("city"), shared("town"),
        mapping(pids(p(1, 0), p(2, 0, 0), p(3, 1), p(4, 1, 0)),
            pids(p(1, 0), p(2, 0, 0), p(3, 1), p(5, 1, 0)))));
  }

  @Test
  public void aReaderFieldWithNothingToReadIsRejectedByName() {
    Schema writer = record("R", field("id", "\"int\""));
    Schema reader = record("R", field("id", "\"int\""), field("name", "\"string\""));
    AvroProvenanceRenamer.Renamed renamed = AvroProvenanceRenamer.rename(
        writer, reader, mapping(pids(p(1, 0)), pids(p(1, 0), p(2, 1))));

    SerializationException e = assertThrows(SerializationException.class,
        () -> AvroProvenanceRenamer.requireEveryFieldHasAValue(renamed));
    assertTrue(e.getMessage(), e.getMessage().contains("Field 'name'"));
  }

  @Test
  public void aReaderFieldWithADefaultPasses() {
    Schema writer = record("R", field("id", "\"int\""));
    Schema reader = record("R", field("id", "\"int\""), field("name", "\"string\"", "\"x\""));
    AvroProvenanceRenamer.requireEveryFieldHasAValue(AvroProvenanceRenamer.rename(
        writer, reader, mapping(pids(p(1, 0)), pids(p(1, 0), p(2, 1)))));
  }

  private static ProvenanceMapping mapping(List<ProvenanceField> writer,
      List<ProvenanceField> reader) {
    return ProvenanceMapping.join(new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, writer), new ProvenanceVersion(2, 2, reader))), 1, 2);
  }

  private static List<ProvenanceField> pids(ProvenanceField... fields) {
    return Arrays.asList(fields);
  }

  private static ProvenanceField p(int pid, Integer... path) {
    return new ProvenanceField(Arrays.asList(path), null, pid);
  }

  private static String field(String name, String type) {
    return "{\"name\":\"" + name + "\",\"type\":" + type + "}";
  }

  private static String field(String name, String type, String defaultValue) {
    return "{\"name\":\"" + name + "\",\"type\":" + type + ",\"default\":" + defaultValue + "}";
  }

  private static Schema record(String name, String... fields) {
    return new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"" + name
        + "\",\"fields\":[" + String.join(",", fields) + "]}");
  }

  // { home: Address {<field>}, work: Address }
  private static Schema shared(String field) {
    return record("R",
        "{\"name\":\"home\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":["
            + field(field, "\"string\"") + "]}}",
        field("work", "\"Address\""));
  }
}
