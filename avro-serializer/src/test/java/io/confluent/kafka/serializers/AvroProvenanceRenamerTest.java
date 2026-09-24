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
        writer, reader, mapping(pids(p(1, "a"), p(2, "b")), pids(p(1, "x"), p(3, "b"))));

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
        writer, reader, mapping(pids(p(1, "name")), pids(p(1, "full_name"), p(2, "name"))));

    assertEquals("full_name", renamed.writer.getFields().get(0).name());
    assertTrue(renamed.reader.getField("full_name").aliases().isEmpty());
    assertEquals("full_name",
        Schema.applyAliases(renamed.writer, renamed.reader).getFields().get(0).name());
  }

  @Test
  public void aSharedRecordIsRenamedTheSameWayAtEverySite() {
    AvroProvenanceRenamer.Renamed renamed = AvroProvenanceRenamer.rename(
        shared("city"), shared("town"),
        mapping(sites(1, 2, 3, 4, "city"), sites(1, 2, 3, 4, "town")));

    Schema home = renamed.writer.getField("home").schema();
    assertEquals("town", home.getFields().get(0).name());
    assertEquals(home, renamed.writer.getField("work").schema());
  }

  @Test
  public void aSharedRecordNeedingTwoDifferentRenamesIsCloned() {
    // work.city has no counterpart, home.city does: one Address, two definitions. Outside a union
    // the resolver ignores record names, so the second is a clone.
    AvroProvenanceRenamer.Renamed renamed = AvroProvenanceRenamer.rename(
        shared("city"), shared("town"),
        mapping(sites(1, 2, 3, 4, "city"), sites(1, 2, 3, 5, "town")));

    assertEquals("town", renamed.writer.getField("home").schema().getFields().get(0).name());
    assertTrue(renamed.writer.getField("work").schema().getFields().get(0).name()
        .startsWith("__provenance_unmatched_"));
  }

  @Test
  public void aRecordInsideAUnionNeedingTwoDifferentRenamesFallsBack() {
    // One reader record A at two union branches, paired differently: A cannot be cloned there.
    Schema a = record("A", field("x", "\"int\""));
    Schema writer = record("R", field("u", "[\"string\"," + a + "]"),
        field("v", "[\"string\",\"A\"]"));
    Schema reader = record("R", field("u", "[\"string\"," + a + "]"),
        field("v", "[\"string\",\"A\"]"));
    assertThrows(ProvenanceUnavailableException.class, () -> AvroProvenanceRenamer.rename(
        writer, reader, mapping(
            pids(p(1, "u"), p(2, "u", "string"), p(3, "u", "A"), p(4, "u", "A", "x"),
                p(5, "v"), p(6, "v", "string"), p(7, "v", "A"), p(8, "v", "A", "x")),
            pids(p(1, "u"), p(2, "u", "string"), p(3, "u", "A"), p(4, "u", "A", "x"),
                p(5, "v"), p(6, "v", "string"), p(7, "v", "A"), p(9, "v", "A", "x")))));
  }

  @Test
  public void aReaderFieldWithNothingToReadIsRejectedByName() {
    Schema writer = record("R", field("id", "\"int\""));
    Schema reader = record("R", field("id", "\"int\""), field("name", "\"string\""));
    AvroProvenanceRenamer.Renamed renamed = AvroProvenanceRenamer.rename(
        writer, reader, mapping(pids(p(1, "id")), pids(p(1, "id"), p(2, "name"))));

    SerializationException e = assertThrows(SerializationException.class,
        () -> AvroProvenanceRenamer.requireEveryFieldHasAValue(renamed));
    assertTrue(e.getMessage(), e.getMessage().contains("Field 'name'"));
  }

  @Test
  public void aReaderFieldWithADefaultPasses() {
    Schema writer = record("R", field("id", "\"int\""));
    Schema reader = record("R", field("id", "\"int\""), field("name", "\"string\"", "\"x\""));
    AvroProvenanceRenamer.requireEveryFieldHasAValue(AvroProvenanceRenamer.rename(
        writer, reader, mapping(pids(p(1, "id")), pids(p(1, "id"), p(2, "name")))));
  }

  @Test
  public void aLocationWithoutNamesFailsEveryRecord() {
    Schema schema = record("R", field("a", "\"int\""));
    assertThrows(SerializationException.class, () -> AvroProvenanceRenamer.rename(schema, schema,
        mapping(pids(p(1, "a")), pids(new ProvenanceField(Arrays.asList(1), null, 1)))));
  }

  @Test
  public void aLocationNotInTheSchemaFailsEveryRecord() {
    Schema schema = record("R", field("a", "\"int\""));
    SerializationException e = assertThrows(SerializationException.class,
        () -> AvroProvenanceRenamer.rename(schema, schema,
            mapping(pids(p(1, "a"), p(2, "ghost")), pids(p(1, "a")))));
    assertTrue(e.getMessage(), e.getMessage().contains("[ghost] of schema id 1"));
  }

  @Test
  public void aPairingAcrossParentsFailsEveryRecord() {
    // a.x and b.y share a pid, though a and b are different locations.
    Schema schema = record("R", field("a", record("A", field("x", "\"int\"")).toString()),
        field("b", record("B", field("y", "\"int\"")).toString()));
    SerializationException e = assertThrows(SerializationException.class,
        () -> AvroProvenanceRenamer.rename(schema, schema, mapping(
            pids(p(1, "a"), p(2, "a", "x"), p(3, "b"), p(4, "b", "y")),
            pids(p(1, "a"), p(4, "a", "x"), p(3, "b"), p(2, "b", "y")))));
    assertTrue(e.getMessage(), e.getMessage().contains("different parents"));
  }

  private static ProvenanceMapping mapping(List<ProvenanceField> writer,
      List<ProvenanceField> reader) {
    return ProvenanceMapping.join(new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, writer), new ProvenanceVersion(2, 2, reader))), 1, 2);
  }

  private static List<ProvenanceField> pids(ProvenanceField... fields) {
    return Arrays.asList(fields);
  }

  // The renamer reads only pids and names; the path just has to be unique.
  private static ProvenanceField p(int pid, String... names) {
    return new ProvenanceField(Arrays.asList(pid), Arrays.asList(names), pid);
  }

  // home and work, each an Address with one field.
  private static List<ProvenanceField> sites(int home, int homeField, int work, int workField,
      String field) {
    return pids(p(home, "home"), p(homeField, "home", field), p(work, "work"),
        p(workField, "work", field));
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
