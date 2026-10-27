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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

public class AvroProvenanceProjectionTest {

  @Test
  public void aPairedFieldTakesTheReaderNameAndAnUnpairedOneANameNothingMatches() {
    Schema writer = record("W", field("a", "\"int\""), field("b", "\"int\""));
    Schema reader = record("R", field("x", "\"int\""), field("b", "\"int\"", "0"));
    // a -> x keeps its pid; the reader's b is a new column that happens to share a name.
    AvroProvenanceProjection projection = AvroProvenanceProjection.of(
        writer, reader, mapping(pids(p(1, "a"), p(2, "b")), pids(p(1, "x"), p(3, "b"))));

    assertEquals("R", projection.writer.getFullName());
    assertEquals("x", projection.writer.getFields().get(0).name());
    assertTrue(projection.writer.getFields().get(1).name().startsWith("__provenance_unmatched_"));
    assertEquals(Schema.create(Schema.Type.INT), projection.writer.getFields().get(1).schema());
  }

  @Test
  public void aPairRenamingNothingReusesTheCallersTypes() {
    String in = record("In", field("x", "\"int\"")).toString();
    String e = "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}";
    String tags = "{\"type\":\"array\",\"items\":\"string\"}";
    Schema writer = record("R", field("a", "\"int\""), field("in", in), field("e", e),
        field("tags", tags));
    Schema reader = record("R", field("a", "\"int\""), field("in", in), field("e", e),
        field("tags", tags), field("b", "\"int\"", "0"));
    List<ProvenanceField> both = pids(p(1, "a"), p(2, "in"), p(3, "in", "x"), p(4, "e"),
        p(5, "tags"));
    List<ProvenanceField> added = new ArrayList<>(both);
    added.add(p(6, "b"));
    AvroProvenanceProjection projection =
        AvroProvenanceProjection.of(writer, reader, mapping(both, added));
    assertSame(writer, projection.writer);
    assertSame(reader, projection.reader);

    // A nullable field with no default is given one, so the reader is copied; the writer is not.
    Schema nullable = record("R", field("a", "\"int\""), field("n", "[\"null\",\"int\"]"));
    projection = AvroProvenanceProjection.of(record("R", field("a", "\"int\"")), nullable,
        mapping(pids(p(1, "a")), pids(p(1, "a"), p(2, "n"))));
    assertNotSame(nullable, projection.reader);
  }

  @Test
  public void aRenamedEnumOrFixedTakesTheReadersName() throws Exception {
    // The writer's own type is reused only where its name is kept: a renamed one would fail
    // every record, the reader's aliases being stripped.
    Schema writer = record("R",
        field("e", "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}"),
        field("f", "{\"type\":\"fixed\",\"name\":\"F\",\"size\":2}"));
    Schema reader = record("R", field("e", "{\"type\":\"enum\",\"name\":\"E2\","
        + "\"aliases\":[\"E\"],\"symbols\":[\"A\",\"B\"]}"),
        field("f", "{\"type\":\"fixed\",\"name\":\"F2\",\"aliases\":[\"F\"],\"size\":2}"));
    List<ProvenanceField> both = pids(p(1, "e"), p(2, "f"));
    AvroProvenanceProjection projection =
        AvroProvenanceProjection.of(writer, reader, mapping(both, both));
    assertEquals("E2", projection.writer.getField("e").schema().getFullName());
    assertEquals("F2", projection.writer.getField("f").schema().getFullName());

    GenericRecord read = decode(writer, projection, new GenericRecordBuilder(writer)
        .set("e", new GenericData.EnumSymbol(writer.getField("e").schema(), "B"))
        .set("f", new GenericData.Fixed(writer.getField("f").schema(), new byte[] {1, 2}))
        .build());
    assertEquals("B", read.get("e").toString());
    assertArrayEquals(new byte[] {1, 2}, ((GenericFixed) read.get("f")).bytes());
  }

  @Test
  public void theReadersAliasesDoNotMoveAFieldProvenancePlaced() {
    // v1 {name} read under v3 {full_name aliases [name], name}: provenance pairs v1's name with
    // full_name. Left in place, the alias would rename it again.
    Schema writer = record("R", field("name", "\"string\""));
    Schema reader = record("R",
        "{\"name\":\"full_name\",\"type\":\"string\",\"aliases\":[\"name\"]}",
        field("name", "\"string\"", "\"unknown\""));
    AvroProvenanceProjection projection = AvroProvenanceProjection.of(
        writer, reader, mapping(pids(p(1, "name")), pids(p(1, "full_name"), p(2, "name"))));

    assertEquals("full_name", projection.writer.getFields().get(0).name());
    assertNotSame(reader, projection.reader);
    assertTrue(projection.reader.getField("full_name").aliases().isEmpty());
    assertEquals("full_name",
        Schema.applyAliases(projection.writer, projection.reader).getFields().get(0).name());
  }

  @Test
  public void aSharedRecordIsRenamedTheSameWayAtEverySite() {
    AvroProvenanceProjection projection = AvroProvenanceProjection.of(
        shared("city", null), shared("town", null),
        mapping(sites(1, 2, 3, 4, "city"), sites(1, 2, 3, 4, "town")));

    Schema home = projection.writer.getField("home").schema();
    assertEquals("town", home.getFields().get(0).name());
    assertEquals(home, projection.writer.getField("work").schema());
  }

  @Test
  public void aSharedRecordNeedingTwoDifferentRenamesIsCloned() {
    // work.city has no counterpart, home.city does: one Address, two definitions. Outside a union
    // the resolver ignores record names, so the second is a clone.
    // work.town is new, so it needs a default for there to be anything to read.
    AvroProvenanceProjection projection = AvroProvenanceProjection.of(
        shared("city", null), shared("town", "\"\""),
        mapping(sites(1, 2, 3, 4, "city"), sites(1, 2, 3, 5, "town")));

    assertEquals("town", projection.writer.getField("home").schema().getFields().get(0).name());
    assertTrue(projection.writer.getField("work").schema().getFields().get(0).name()
        .startsWith("__provenance_unmatched_"));
  }

  @Test
  public void aRecordInsideAUnionNeedingTwoDifferentRenamesIsClonedAndMatchedByStructure()
      throws Exception {
    // One writer record A at two union branches, paired differently: v's A is cloned under a
    // throwaway namespace, and Avro matches a union branch by structure, preferring its short name.
    Schema writer = record("R", field("u", "[\"string\"," + record("A", field("x", "\"int\""))
        + "]"), field("v", "[\"string\",\"A\"]"));
    Schema reader = record("R", field("u", "[\"string\","
        + record("A", field("x", "\"int\"", "0")) + "]"), field("v", "[\"string\",\"A\"]"));
    AvroProvenanceProjection projection = AvroProvenanceProjection.of(writer, reader, mapping(
        pids(p(1, "u"), p(2, "u", "string"), p(3, "u", "A"), p(4, "u", "A", "x"),
            p(5, "v"), p(6, "v", "string"), p(7, "v", "A"), p(8, "v", "A", "x")),
        pids(p(1, "u"), p(2, "u", "string"), p(3, "u", "A"), p(4, "u", "A", "x"),
            p(5, "v"), p(6, "v", "string"), p(7, "v", "A"), p(9, "v", "A", "x"))));

    Schema a = writer.getField("u").schema().getTypes().get(1);
    GenericRecord read = decode(writer, projection, new GenericRecordBuilder(writer)
        .set("u", new GenericRecordBuilder(a).set("x", 5).build())
        .set("v", new GenericRecordBuilder(a).set("x", 9).build()).build());
    assertEquals(5, ((GenericRecord) read.get("u")).get("x"));
    // v's x is new: it takes the default rather than the writer's 9.
    assertEquals(0, ((GenericRecord) read.get("v")).get("x"));
  }

  @Test
  public void aCloneTheResolverWouldMatchToAnotherBranchFallsBack() {
    // Two reader branches share the short name A; Avro's structural match takes the last, n2.A,
    // not the n1.A the clone was renamed after.
    String n1 = "{\"type\":\"record\",\"name\":\"A\",\"namespace\":\"n1\",\"fields\":["
        + field("x", "\"int\"", "0") + "]}";
    String n2 = "{\"type\":\"record\",\"name\":\"A\",\"namespace\":\"n2\",\"fields\":["
        + field("x", "\"int\"", "0") + "]}";
    Schema writer = record("R", field("u", "[\"string\"," + n1 + "]"),
        field("v", "[\"string\",\"n1.A\"]"));
    Schema reader = record("R", field("u", "[\"string\"," + n1 + "]"),
        field("v", "[\"string\",\"n1.A\"," + n2 + "]"));
    assertThrows(ProvenanceUnavailableException.class, () -> AvroProvenanceProjection.of(
        writer, reader, mapping(
            pids(p(1, "u"), p(2, "u", "string"), p(3, "u", "n1.A"), p(4, "u", "n1.A", "x"),
                p(5, "v"), p(6, "v", "string"), p(7, "v", "n1.A"), p(8, "v", "n1.A", "x")),
            pids(p(1, "u"), p(2, "u", "string"), p(3, "u", "n1.A"), p(4, "u", "n1.A", "x"),
                p(5, "v"), p(6, "v", "string"), p(7, "v", "n1.A"), p(9, "v", "n1.A", "x"),
                p(10, "v", "n2.A"), p(11, "v", "n2.A", "x")))));
  }

  @Test
  public void aReaderFieldWithNothingToReadIsRejectedByName() {
    Schema writer = record("R", field("id", "\"int\""));
    Schema reader = record("R", field("id", "\"int\""), field("name", "\"string\""));

    SerializationException e = assertThrows(SerializationException.class,
        () -> AvroProvenanceProjection.of(
            writer, reader, mapping(pids(p(1, "id")), pids(p(1, "id"), p(2, "name")))));
    assertTrue(e.getMessage(), e.getMessage().contains("Field 'name'"));
  }

  @Test
  public void aReaderFieldWithADefaultPasses() {
    Schema writer = record("R", field("id", "\"int\""));
    Schema reader = record("R", field("id", "\"int\""), field("name", "\"string\"", "\"x\""));
    AvroProvenanceProjection projection = AvroProvenanceProjection.of(
        writer, reader, mapping(pids(p(1, "id")), pids(p(1, "id"), p(2, "name"))));
    // name has no writer source: the writer stays as it was, and the reader's default stands in.
    assertEquals(1, projection.writer.getFields().size());
    assertEquals("id", projection.writer.getFields().get(0).name());
    assertEquals("x", projection.reader.getField("name").defaultVal());
  }

  @Test
  public void aLocationWithoutNamesFailsEveryRecord() {
    Schema schema = record("R", field("a", "\"int\""));
    assertThrows(SerializationException.class, () -> AvroProvenanceProjection.of(schema, schema,
        mapping(pids(p(1, "a")),
            pids(new ProvenanceField(Arrays.asList(1), null, "SCALAR", 1)))));
  }

  @Test
  public void aLocationWithoutAKindFailsEveryRecord() {
    Schema schema = record("R", field("a", "\"int\""));
    SerializationException e = assertThrows(SerializationException.class,
        () -> AvroProvenanceProjection.of(schema, schema, mapping(pids(p(1, "a")),
            pids(new ProvenanceField(Arrays.asList(1), Arrays.asList("a"), null, 1)))));
    assertTrue(e.getMessage(), e.getMessage().contains("no kind for location"));
  }

  @Test
  public void aLocationNotInTheSchemaFailsEveryRecord() {
    Schema schema = record("R", field("a", "\"int\""));
    SerializationException e = assertThrows(SerializationException.class,
        () -> AvroProvenanceProjection.of(schema, schema,
            mapping(pids(p(1, "a"), p(2, "ghost")), pids(p(1, "a")))));
    assertTrue(e.getMessage(), e.getMessage().contains("[ghost] of schema id 1"));
  }

  @Test
  public void aPairingAcrossParentsFailsEveryRecord() {
    // a.x and b.y share a pid, though a and b are different locations.
    Schema schema = record("R", field("a", record("A", field("x", "\"int\"")).toString()),
        field("b", record("B", field("y", "\"int\"")).toString()));
    SerializationException e = assertThrows(SerializationException.class,
        () -> AvroProvenanceProjection.of(schema, schema, mapping(
            pids(p(1, "a"), p(2, "a", "x"), p(3, "b"), p(4, "b", "y")),
            pids(p(1, "a"), p(4, "a", "x"), p(3, "b"), p(2, "b", "y")))));
    assertTrue(e.getMessage(), e.getMessage().contains("different parents"));
  }

  @Test
  public void aMatchedTypesAliasCannotRenameTheWriterAgain() throws Exception {
    // A2 aliases A, whose name a new type took. Avro applies a reader's aliases to the writer
    // before resolving, so left in place the alias would rename the writer's A to A2 as well.
    String union = "[\"null\",{\"type\":\"record\",\"name\":\"A2\",\"aliases\":[\"A\"],"
        + "\"fields\":[" + field("x", "\"int\"") + "]},{\"type\":\"record\",\"name\":\"A\","
        + "\"fields\":[" + field("q", "\"string\"") + "]}]";
    Schema writer = record("R", field("u", union));
    Schema reader = record("R", field("u", union), field("extra", "\"int\"", "0"));
    List<ProvenanceField> located = pids(p(1, "u"), p(2, "u", "A2"), p(3, "u", "A2", "x"),
        p(4, "u", "A"), p(5, "u", "A", "q"));
    List<ProvenanceField> readerLocated = new ArrayList<>(located);
    readerLocated.add(p(6, "extra"));
    AvroProvenanceProjection projection =
        AvroProvenanceProjection.of(writer, reader, mapping(located, readerLocated));

    assertTrue(projection.reader.getField("u").schema().getTypes().get(1).getAliases().isEmpty());
    Schema a2 = writer.getField("u").schema().getTypes().get(1);
    GenericRecord value = new GenericRecordBuilder(writer)
        .set("u", new GenericRecordBuilder(a2).set("x", 7).build()).build();
    GenericRecord read = decode(writer, projection, value);
    assertEquals(7, ((GenericRecord) read.get("u")).get("x"));
  }

  @Test
  public void bytesAndFixedDefaultsSurviveTheReaderCopy() throws Exception {
    // Avro hands a bytes or fixed default back as a byte[], which it cannot write as a default.
    Schema writer = record("R", field("a", "\"int\""));
    Schema reader = record("R", field("a", "\"int\""),
        field("d", "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":5,"
            + "\"scale\":2}", "\"\\u0001\""),
        field("f", "{\"type\":\"fixed\",\"name\":\"F\",\"size\":2}", "\"ab\""));
    AvroProvenanceProjection projection = AvroProvenanceProjection.of(writer, reader,
        mapping(pids(p(1, "a")), pids(p(1, "a"), p(2, "d"), p(3, "f"))));

    GenericRecord read = decode(writer, projection,
        new GenericRecordBuilder(writer).set("a", 1).build());
    assertEquals(ByteBuffer.wrap(new byte[] {1}), read.get("d"));
    assertArrayEquals("ab".getBytes(StandardCharsets.ISO_8859_1),
        ((GenericFixed) read.get("f")).bytes());
  }

  @Test
  public void anInvalidDefaultNoRecordNeedsDoesNotFailTheRead() throws Exception {
    // The registry parses without validating defaults, and native Avro fails one only when used.
    Schema writer = record("R", field("a", "\"int\""), field("d", "\"int\""));
    Schema reader = new Schema.Parser().setValidateDefaults(false).parse("{\"type\":\"record\","
        + "\"name\":\"R\",\"fields\":[" + field("a", "\"int\"") + ","
        + field("d", "\"int\"", "\"x\"") + "]}");
    AvroProvenanceProjection projection = AvroProvenanceProjection.of(writer, reader,
        mapping(pids(p(1, "a"), p(2, "d")), pids(p(1, "a"), p(2, "d"))));

    GenericRecord read = decode(writer, projection,
        new GenericRecordBuilder(writer).set("a", 1).set("d", 2).build());
    assertEquals(2, read.get("d"));
  }

  // Written under writer, read through the renamed pair, as the deserializer reads it.
  private static GenericRecord decode(Schema writer, AvroProvenanceProjection projection,
      GenericRecord value) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(writer).write(value, encoder);
    encoder.flush();
    return new GenericDatumReader<GenericRecord>(projection.writer, projection.reader)
        .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
  }

  private static ProvenanceMapping mapping(List<ProvenanceField> writer,
      List<ProvenanceField> reader) {
    return ProvenanceMapping.join(new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, "STRUCT", writer),
        new ProvenanceVersion(2, 2, "STRUCT", reader))), 1, 2);
  }

  private static List<ProvenanceField> pids(ProvenanceField... fields) {
    return Arrays.asList(fields);
  }

  // The renamer reads only pids and names; the path just has to be unique.
  private static ProvenanceField p(int pid, String... names) {
    return new ProvenanceField(Arrays.asList(pid), Arrays.asList(names), "SCALAR", pid);
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
  private static Schema shared(String field, String defaultValue) {
    return record("R",
        "{\"name\":\"home\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":["
            + (defaultValue != null ? field(field, "\"string\"", defaultValue)
                : field(field, "\"string\"")) + "]}}",
        field("work", "\"Address\""));
  }
}
