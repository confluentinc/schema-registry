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

package io.confluent.kafka.serializers.provenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Avro read with {@code provenance.algorithm}: type changes the resolver handles must read exactly as
 * without provenance, and only what history decides — a rename chained through an interior
 * version, a column dropped and re-added — may differ.
 */
class AvroProvenanceDeserializerTest {

  private static final String TOPIC = "promotion";
  private static final String SUBJECT = TOPIC + "-value";

  private ProvenanceMockSchemaRegistryClient client;
  private KafkaAvroSerializer serializer;

  @BeforeEach
  void init() {
    client = new ProvenanceMockSchemaRegistryClient();
    serializer = new KafkaAvroSerializer(client, config(null));
  }

  // --- Type changes: identical both ways -------------------------------------------------------

  @Test
  void intPromotesToLong() throws Exception {
    assertEquals(7L, sameBothWays(field("int"), field("long"), 7).get("f"));
  }

  @Test
  void intPromotesToFloat() throws Exception {
    assertEquals(7f, sameBothWays(field("int"), field("float"), 7).get("f"));
  }

  @Test
  void intPromotesToDouble() throws Exception {
    assertEquals(7d, sameBothWays(field("int"), field("double"), 7).get("f"));
  }

  @Test
  void longPromotesToFloat() throws Exception {
    assertEquals(7f, sameBothWays(field("long"), field("float"), 7L).get("f"));
  }

  @Test
  void longPromotesToDouble() throws Exception {
    assertEquals(7d, sameBothWays(field("long"), field("double"), 7L).get("f"));
  }

  @Test
  void floatPromotesToDouble() throws Exception {
    assertEquals(1.5d, sameBothWays(field("float"), field("double"), 1.5f).get("f"));
  }

  @Test
  void stringPromotesToBytes() throws Exception {
    assertEquals(ByteBuffer.wrap("ada".getBytes(StandardCharsets.UTF_8)),
        sameBothWays(field("string"), field("bytes"), "ada").get("f"));
  }

  @Test
  void bytesPromotesToString() throws Exception {
    assertEquals("ada", sameBothWays(field("bytes"), field("string"),
        ByteBuffer.wrap("ada".getBytes(StandardCharsets.UTF_8))).get("f").toString());
  }

  @Test
  void aRenamedColumnIsPromotedToo() throws Exception {
    Schema reader = record("{\"name\":\"m\",\"type\":\"long\",\"aliases\":[\"f\"],\"default\":0}");
    assertEquals(7L, sameBothWays(field("int"), reader, 7).get("m"));
  }

  @Test
  void anArrayElementIsPromoted() throws Exception {
    assertEquals(Arrays.asList(1L, 2L), sameBothWays(
        field("{\"type\":\"array\",\"items\":\"int\"}"),
        field("{\"type\":\"array\",\"items\":\"long\"}"), Arrays.asList(1, 2)).get("f"));
  }

  @Test
  void aMapValueIsPromoted() throws Exception {
    Map<?, ?> map = (Map<?, ?>) sameBothWays(field("{\"type\":\"map\",\"values\":\"int\"}"),
        field("{\"type\":\"map\",\"values\":\"long\"}"), Collections.singletonMap("k", 1))
        .get("f");
    assertEquals(1L, map.values().iterator().next());
  }

  @Test
  void aNullableBranchIsPromoted() throws Exception {
    assertEquals(7L,
        sameBothWays(field("[\"null\",\"int\"]"), field("[\"null\",\"long\"]"), 7).get("f"));
  }

  @Test
  void anUnknownEnumSymbolTakesTheReadersEnumDefault() throws Exception {
    Schema writer = enumField("[\"A\",\"B\",\"C\"]", null);
    Object c = new GenericData.EnumSymbol(writer.getField("f").schema(), "C");
    assertEquals("A",
        sameBothWays(writer, enumField("[\"A\",\"B\"]", "A"), c).get("f").toString());
  }

  @Test
  void anUnknownEnumSymbolWithNoDefaultFailsBothWays() throws Exception {
    Schema writer = enumField("[\"A\",\"B\",\"C\"]", null);
    failsBothWays(writer, enumField("[\"A\",\"B\"]", null),
        new GenericData.EnumSymbol(writer.getField("f").schema(), "C"));
  }

  @Test
  void aValueWidenedIntoAUnionIsANewColumn() throws Exception {
    // A scalar becoming a union changes kind, a drop and an add: Avro alone reads the value
    // into the int branch, provenance does not, and with no default the record fails.
    assertNewColumn(field("int"), field("[\"int\",\"string\"]"), 7, null);
    assertNewColumn(field("int"), defaulted("[\"int\",\"string\"]", "0"), 7, 0);
  }

  @Test
  void aUnionNarrowedToTheBranchItHoldsIsANewColumn() throws Exception {
    assertNewColumn(field("[\"int\",\"string\"]"), field("int"), 7, null);
    assertNewColumn(field("[\"int\",\"string\"]"), defaulted("int", "0"), 7, 0);
  }

  @Test
  void aUnionNarrowedPastTheBranchItHoldsFailsBothWays() throws Exception {
    failsBothWays(field("[\"int\",\"string\"]"), field("int"), "ada");
  }

  @Test
  void aNullReadIntoARequiredFieldFailsBothWays() throws Exception {
    failsBothWays(field("[\"null\",\"int\"]"), field("int"), null);
  }

  // --- History: where provenance and the resolver part company ----------------------------------

  @Test
  void aRenameChainedThroughAnInteriorVersionIsFollowed() throws Exception {
    // name -> full_name (aliases name) -> display_name (aliases full_name): the v3 reader has no
    // alias for "name", so a v1 record connects to it only through v2.
    Schema v1 = record(idField(), string("name"));
    Schema v2 = record(idField(),
        "{\"name\":\"full_name\",\"type\":\"string\",\"aliases\":[\"name\"]}");
    Schema v3 = record(idField(), "{\"name\":\"display_name\",\"type\":\"string\","
        + "\"aliases\":[\"full_name\"],\"default\":\"?\"}");
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("name", "ada"));
    client.register(SUBJECT, new AvroSchema(v2));
    client.register(SUBJECT, new AvroSchema(v3));

    assertEquals("ada", read(v3, bytes, "v1").get("display_name").toString());
    assertEquals("?", read(v3, bytes, null).get("display_name").toString());
  }

  @Test
  void aColumnDroppedAndReAddedAcrossAnInteriorVersionIsANewColumn() throws Exception {
    Schema v1 = record(idField(), string("name"));
    Schema v2 = record(idField());
    Schema v3 = record(idField(), "{\"name\":\"name\",\"type\":\"string\",\"default\":\"new\"}");
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("name", "ada"));
    client.register(SUBJECT, new AvroSchema(v2));
    client.register(SUBJECT, new AvroSchema(v3));

    assertEquals("new", read(v3, bytes, "v1").get("name").toString());
    assertEquals("ada", read(v3, bytes, null).get("name").toString());
  }

  @Test
  void aRecordNamingItsSchemaByGuidAloneIsReadByProvenance() throws Exception {
    Schema v1 = record(idField(), string("name"));
    Schema v2 = record(idField());
    Schema v3 = record(idField(), "{\"name\":\"name\",\"type\":\"string\",\"default\":\"new\"}");
    client.register(SUBJECT, new AvroSchema(v1));
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = new KafkaAvroSerializer(client, byGuid(config(null))).serialize(
        TOPIC, headers, new GenericRecordBuilder(v1).set("id", 7).set("name", "ada").build());
    client.register(SUBJECT, new AvroSchema(v2));
    client.register(SUBJECT, new AvroSchema(v3));

    GenericRecord on = (GenericRecord) new KafkaAvroDeserializer(client, config("v1"))
        .deserializeWithSchema(TOPIC, headers, bytes, v3).getValue();
    assertEquals("new", on.get("name").toString());
  }

  @Test
  void aReaderWithRulesMergedOnIsFoundByItsStructure() throws Exception {
    // Flink merges the writer's metadata and rules onto its pinned reader; the result is no
    // registered version, but it has the structure of one, which is all provenance needs.
    Schema v1 = record(idField(), string("name"));
    Schema v2 = record(idField());
    Schema v3 = record(idField(), "{\"name\":\"name\",\"type\":\"string\",\"default\":\"new\"}");
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("name", "ada"));
    client.register(SUBJECT, new AvroSchema(v2));
    client.register(SUBJECT, new AvroSchema(v3));
    AvroSchema merged = (AvroSchema) new AvroSchema(v3).copy(
        new Metadata(null, Collections.singletonMap("owner", "writer"), null), null);

    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config("v1"));
    GenericRecord on = (GenericRecord) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, writer -> merged).getValue();
    assertEquals("new", on.get("name").toString());
  }

  @Test
  void aReaderFieldWithNothingToReadFailsByName() throws Exception {
    Schema writer = record(idField());
    Schema reader = record(idField(), string("name"));
    byte[] bytes = write(writer, new GenericRecordBuilder(writer).set("id", 7));
    client.register(SUBJECT, new AvroSchema(reader));

    Exception e = assertThrows(Exception.class, () -> read(reader, bytes, "v1"));
    StringWriter trace = new StringWriter();
    e.printStackTrace(new PrintWriter(trace));
    assertTrue(trace.toString().contains("Field 'name'"), trace.toString());
  }

  @Test
  void aWriterUnderTheReadersOwnSchemaReadsAsWritten() throws Exception {
    Schema schema = record(idField(), string("name"));
    byte[] bytes = write(schema, new GenericRecordBuilder(schema).set("id", 7).set("name", "ada"));
    assertEquals("ada", read(schema, bytes, "v1").get("name").toString());
  }

  @Test
  void aReaderMatchesAVersionImportingWhatItImports() throws Exception {
    // A reader matched by structure imports the same schemas as the version, spelled otherwise:
    // its references in another order, one as the latest version, one through another subject.
    ProvenanceMockSchemaRegistryClient client = new ProvenanceMockSchemaRegistryClient();
    String dep = "{\"type\":\"record\",\"name\":\"Dep\",\"namespace\":\"d\","
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
    String oth = "{\"type\":\"record\",\"name\":\"Oth\",\"namespace\":\"e\","
        + "\"fields\":[{\"name\":\"q\",\"type\":\"int\"}]}";
    client.register("dep", new AvroSchema(dep));
    client.register("dep-alias", new AvroSchema(dep));
    client.register("oth", new AvroSchema(oth));
    Map<String, String> resolved = new HashMap<>();
    resolved.put("dep", dep);
    resolved.put("oth", oth);
    String main = "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"m\",\"doc\":\"%s\","
        + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}%s,{\"name\":\"d\",\"type\":\"d.Dep\"},"
        + "{\"name\":\"o\",\"type\":\"e.Oth\"}]}";
    String note = ",{\"name\":\"note\",\"type\":\"string\",\"default\":\"new\"}";
    List<SchemaReference> refs = Arrays.asList(new SchemaReference("dep", "dep", 1),
        new SchemaReference("oth", "oth", 1));
    AvroSchema v1 = new AvroSchema(String.format(main, "v1", note), refs, resolved, null);
    int id1 = client.register(SUBJECT, v1);
    client.register(SUBJECT, new AvroSchema(String.format(main, "v2", ""), refs, resolved, null));
    client.register(SUBJECT, new AvroSchema(String.format(main, "v3", note), refs, resolved, null));
    GenericRecord value = new GenericData.Record(v1.rawSchema());
    value.put("id", 7);
    value.put("note", "old");
    GenericRecord d = new GenericData.Record(v1.rawSchema().getField("d").schema());
    d.put("x", 1);
    value.put("d", d);
    GenericRecord o = new GenericData.Record(v1.rawSchema().getField("o").schema());
    o.put("q", 2);
    value.put("o", o);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0);
    out.write(ByteBuffer.allocate(4).putInt(id1).array());
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<>(v1.rawSchema()).write(value, encoder);
    encoder.flush();
    byte[] bytes = out.toByteArray();
    // Metadata of its own keeps the reader from matching any version exactly.
    Metadata metadata = new Metadata(null, Collections.singletonMap("k", "v"), null);
    for (List<SchemaReference> spelled : Arrays.asList(
        Arrays.asList(refs.get(1), refs.get(0)),
        Arrays.asList(new SchemaReference("dep", "dep", -1), refs.get(1)),
        Arrays.asList(new SchemaReference("dep", "dep-alias", 1), refs.get(1)))) {
      AvroSchema reader = (AvroSchema) new AvroSchema(String.format(main, "v3", note), spelled,
          resolved, null).copy(metadata, null);
      GenericRecord read = (GenericRecord) new KafkaAvroDeserializer(client, config("v1"))
          .deserializeWithSchema(TOPIC, new RecordHeaders(), bytes, writer -> reader).getValue();
      assertEquals("new", read.get("note").toString(), spelled.toString());
    }
  }

  @Test
  void aReaderLookupTheRegistryRejectsFallsBack() throws Exception {
    // The reader's reference names a version its subject lacks: the registry's 40402 answers the
    // lookup, not the provenance request, so the pair falls back rather than failing every record.
    ProvenanceMockSchemaRegistryClient client = new ProvenanceMockSchemaRegistryClient();
    String dep = "{\"type\":\"record\",\"name\":\"Dep\",\"namespace\":\"d\","
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
    client.register("dep", new AvroSchema(dep));
    String main = "{\"type\":\"record\",\"name\":\"R\",\"doc\":\"%s\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"d\",\"type\":\"d.Dep\"}]}";
    Map<String, String> resolved = Collections.singletonMap("dep", dep);
    List<SchemaReference> refs = Collections.singletonList(new SchemaReference("dep", "dep", 1));
    AvroSchema v1 = new AvroSchema(String.format(main, "v1"), refs, resolved, null);
    int id1 = client.register(SUBJECT, v1);
    client.register(SUBJECT, new AvroSchema(String.format(main, "v2"), refs, resolved, null));
    AvroSchema reader = (AvroSchema) new AvroSchema(String.format(main, "v2"),
        Collections.singletonList(new SchemaReference("dep", "dep", 9)), resolved, null)
        .copy(new Metadata(null, Collections.singletonMap("k", "v"), null), null);
    GenericRecord value = new GenericData.Record(v1.rawSchema());
    value.put("id", 7);
    GenericRecord d = new GenericData.Record(v1.rawSchema().getField("d").schema());
    d.put("x", 1);
    value.put("d", d);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0);
    out.write(ByteBuffer.allocate(4).putInt(id1).array());
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<>(v1.rawSchema()).write(value, encoder);
    encoder.flush();

    GenericRecord read = (GenericRecord) new KafkaAvroDeserializer(client, config("v1"))
        .deserializeWithSchema(TOPIC, new RecordHeaders(), out.toByteArray(), writer -> reader)
        .getValue();
    assertEquals(7, read.get("id"));
  }

  @Test
  void aFieldWithNoValueFailsOnlyTheRecordsReachingIt() throws Exception {
    // A gains b, with no default, under a nullable union: a record whose u is null never reads A;
    // one holding an A fails, as Avro fails it.
    String a1 = "{\"type\":\"record\",\"name\":\"A\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"}]}";
    String a2 = "{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"b\",\"type\":\"int\"}]}";
    Schema v1 = record(idField(), "{\"name\":\"u\",\"type\":[\"null\"," + a1 + "]}");
    Schema v2 = record(idField(), "{\"name\":\"u\",\"type\":[\"null\"," + a2 + "]}");
    byte[] none = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("u", null));
    Schema inner = v1.getField("u").schema().getTypes().get(1);
    byte[] some = write(v1, new GenericRecordBuilder(v1).set("id", 8)
        .set("u", new GenericRecordBuilder(inner).set("a", 5).build()));
    client.register(SUBJECT, new AvroSchema(v2));

    assertEquals(7, read(v2, none, "v1").get("id"));
    Exception e = assertThrows(Exception.class, () -> read(v2, some, "v1"));
    StringWriter trace = new StringWriter();
    e.printStackTrace(new PrintWriter(trace));
    assertTrue(trace.toString().contains("missing required field b"), trace.toString());
  }

  @Test
  void fieldsSwappedByAliasesAreReadByTheirAliases() throws Exception {
    // a and b swap names by aliases: each value follows its field both ways, where Avro alone
    // reads the older data by the aliases only, and the newer by name, into the wrong types.
    Schema v1 = record("{\"name\":\"a\",\"type\":\"int\"}", string("b"));
    Schema v2 = record("{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\"]}",
        "{\"name\":\"a\",\"type\":\"string\",\"aliases\":[\"b\"]}");
    byte[] older = write(v1, new GenericRecordBuilder(v1).set("a", 5).set("b", "s"));
    byte[] newer = write(v2, new GenericRecordBuilder(v2).set("b", 6).set("a", "t"));

    GenericRecord forward = read(v2, older, "v1");
    assertEquals(5, forward.get("b"));
    assertEquals("s", forward.get("a").toString());
    GenericRecord backward = read(v1, newer, "v1");
    assertEquals(6, backward.get("a"));
    assertEquals("t", backward.get("b").toString());
  }

  @Test
  void typesSwappedByAliasesInOneUnionAreReadByTheirAliases() throws Exception {
    // A and B swap names by aliases inside one union. Data written as v2's B, read under v1,
    // belongs in v1's A, which B continues; Avro alone matches the branch by name and drops x.
    String body = "{\"name\":\"u\",\"type\":[\"int\","
        + "{\"type\":\"record\",\"name\":\"%s\",%s\"fields\":"
        + "[{\"name\":\"x\",\"type\":\"int\",\"default\":-1}]},"
        + "{\"type\":\"record\",\"name\":\"%s\",%s\"fields\":"
        + "[{\"name\":\"y\",\"type\":\"int\",\"default\":-1}]}]}";
    Schema v1 = record(String.format(body, "A", "", "B", ""));
    Schema v2 = record(String.format(body, "B", "\"aliases\":[\"io.confluent.A\"],", "A",
        "\"aliases\":[\"io.confluent.B\"],"));
    client.register(SUBJECT, new AvroSchema(v1));
    Schema b2 = v2.getField("u").schema().getTypes().get(1);
    byte[] bytes = write(v2, new GenericRecordBuilder(v2)
        .set("u", new GenericRecordBuilder(b2).set("x", 5).build()));

    GenericRecord off = (GenericRecord) read(v1, bytes, null).get("u");
    assertEquals("B", off.getSchema().getName());
    GenericRecord on = (GenericRecord) read(v1, bytes, "v1").get("u");
    assertEquals("A", on.getSchema().getName());
    assertEquals(5, on.get("x"));
  }

  @Test
  void aReaderListingAliasesInAnotherOrderIsIdentifiedByStructure() throws Exception {
    // The reader is v3 but for a doc and the order of b's aliases: found by structure, n is the
    // field re-added at v3, so the writer's n is no value of it.
    Schema v1 = record(idField(), "{\"name\":\"a\",\"type\":\"int\"}", string("n"));
    Schema v2 = record(idField(), "{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\",\"z\"]}");
    String v3 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"namespace\":\"io.confluent\",%s"
        + "\"fields\":[" + idField() + ",{\"name\":\"b\",\"type\":\"int\",\"aliases\":[%s]},"
        + "{\"name\":\"n\",\"type\":\"string\",\"default\":\"dflt\"}]}";
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("a", 5).set("n", "old"));
    client.register(SUBJECT, new AvroSchema(v2));
    client.register(SUBJECT, new AvroSchema(String.format(v3, "", "\"a\",\"z\"")));
    Schema reader = new Schema.Parser().parse(String.format(v3, "\"doc\":\"d\",", "\"z\",\"a\""));

    GenericRecord read = read(reader, bytes, "v1");
    assertEquals(5, read.get("b"));
    assertEquals("dflt", read.get("n").toString());
  }

  // --- Helpers -----------------------------------------------------------------------------------

  private GenericRecord sameBothWays(Schema writer, Schema reader, Object value) throws Exception {
    byte[] bytes = write(writer, new GenericRecordBuilder(writer).set("f", value));
    client.register(SUBJECT, new AvroSchema(reader));
    GenericRecord on = read(reader, bytes, "v1");
    GenericRecord off = read(reader, bytes, null);
    assertNotNull(on);
    // Compared by value: the projected record's schema is the reader less some aliases.
    assertEquals(off.toString(), on.toString());
    return on;
  }

  /**
   * Native reading keeps {@code value}; with provenance the reader's field is new, so it reads
   * {@code expected}, its default, or fails where it has none.
   */
  private void assertNewColumn(Schema writer, Schema reader, Object value, Object expected)
      throws Exception {
    byte[] bytes = write(writer, new GenericRecordBuilder(writer).set("f", value));
    client.register(SUBJECT, new AvroSchema(reader));
    assertEquals(value, read(reader, bytes, null).get("f"));
    if (expected == null) {
      assertThrows(Exception.class, () -> read(reader, bytes, "v1"));
    } else {
      assertEquals(expected, read(reader, bytes, "v1").get("f"));
    }
  }

  private void failsBothWays(Schema writer, Schema reader, Object value) throws Exception {
    byte[] bytes = write(writer, new GenericRecordBuilder(writer).set("f", value));
    client.register(SUBJECT, new AvroSchema(reader));
    assertThrows(Exception.class, () -> read(reader, bytes, null), "without provenance");
    assertThrows(Exception.class, () -> read(reader, bytes, "v1"), "with provenance");
  }

  @Test
  void aSubjectWhoseLatestVersionIsAnotherFormatStillFindsTheReadersVersion() throws Exception {
    // This client parses Avro alone, as a deserializer's does: the JSON v4 is skipped unparsed.
    int[] jsonId = {-1};
    client = new ProvenanceMockSchemaRegistryClient() {
      @Override
      public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
          throws IOException, RestClientException {
        if (id == jsonId[0]) {
          throw new IllegalArgumentException("Invalid schema type JSON");
        }
        return super.getSchemaBySubjectAndId(subject, id);
      }
    };
    serializer = new KafkaAvroSerializer(client, config(null));
    Schema v1 = record(idField(), string("x"));
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("x", "old"));
    client.register(SUBJECT, new AvroSchema(record(idField())));
    client.register(SUBJECT, new AvroSchema(record(idField(),
        "{\"name\":\"x\",\"type\":\"string\",\"default\":\"DEF\"}")));
    jsonId[0] = client.register(SUBJECT, new JsonSchema("{\"type\":\"object\"}"));
    // v3 but for a doc, so found by structure rather than by its text.
    Schema reader = record(idField(),
        "{\"name\":\"x\",\"type\":\"string\",\"default\":\"DEF\",\"doc\":\"again\"}");

    assertEquals("DEF", read(reader, bytes, "v1").get("x").toString());
  }

  @Test
  void aFailureFetchingAVersionFailsTheLookupRatherThanSettlingOnAnOlderOne() throws Exception {
    // v3 cannot be fetched now, as when a token endpoint is down: the reader, v3 but for a doc,
    // is not taken for v1, the writer's own version, where x would keep its old value.
    int[] v3 = {-1};
    client = new ProvenanceMockSchemaRegistryClient() {
      @Override
      public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
          throws IOException, RestClientException {
        if (id == v3[0]) {
          throw new IllegalStateException("token endpoint unreachable");
        }
        return super.getSchemaBySubjectAndId(subject, id);
      }
    };
    serializer = new KafkaAvroSerializer(client, config(null));
    Schema v1 = record(idField(), string("x"));
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("x", "old"));
    client.register(SUBJECT, new AvroSchema(record(idField())));
    v3[0] = client.register(SUBJECT, new AvroSchema(record(idField(),
        "{\"name\":\"x\",\"type\":\"string\",\"default\":\"DEF\"}")));
    Schema reader = record(idField(),
        "{\"name\":\"x\",\"type\":\"string\",\"default\":\"DEF\",\"doc\":\"again\"}");

    assertThrows(SerializationException.class, () -> read(reader, bytes, "v1"));
  }

  @Test
  void aPinnedReaderOfASoftDeletedVersionIsThatVersion() throws Exception {
    // v1 is soft-deleted; a reader in v1's exact text is still v1, not v3, which re-adds x.
    String x = "{\"name\":\"x\",\"type\":\"string\",\"default\":\"%s\"}";
    Schema v1 = record(idField(), String.format(x, "V1D"));
    client.register(SUBJECT, new AvroSchema(v1));
    client.register(SUBJECT, new AvroSchema(record(idField())));
    Schema v3 = record(idField(), String.format(x, "DEF"));
    byte[] bytes = write(v3, new GenericRecordBuilder(v3).set("id", 7).set("x", "new"));
    client.deleteSchemaVersion(SUBJECT, "1");

    assertEquals("V1D", read(v1, bytes, "v1").get("x").toString());
  }

  @Test
  void aLatestWithMetadataReaderOfAnOlderVersionIsThatVersion() throws Exception {
    // The reader is v1, carrying its version: found by its content, as the registry finds it, it
    // is v1, where x is another entity than v3's.
    String x = "{\"name\":\"x\",\"type\":\"string\",\"default\":\"%s\"}";
    client.register(SUBJECT, new AvroSchema(record(idField(), String.format(x, "A"))).copy(
        new Metadata(null, Collections.singletonMap("tag", "m1"), null), null));
    client.register(SUBJECT, new AvroSchema(record(idField())));
    Schema v3 = record(idField(), String.format(x, "B"));
    byte[] bytes = write(v3, new GenericRecordBuilder(v3).set("id", 7).set("x", "new"));
    Map<String, Object> config = config("v1");
    config.put("use.latest.with.metadata", "tag=m1");
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config);

    assertEquals("A", ((GenericRecord) deserializer.deserialize(TOPIC, bytes)).get("x").toString());
  }

  @Test
  void aKeyIsReadByProvenanceUnderItsKeySubject() throws Exception {
    // The key subject's history: x, re-added in v3, is new there.
    String keySubject = TOPIC + "-key";
    Schema v1 = record(idField(), string("x"));
    client.register(keySubject, new AvroSchema(v1));
    KafkaAvroSerializer keys = new KafkaAvroSerializer(client);
    keys.configure(config(null), true);
    byte[] bytes = keys.serialize(TOPIC,
        new GenericRecordBuilder(v1).set("id", 7).set("x", "old").build());
    client.register(keySubject, new AvroSchema(record(idField())));
    Schema v3 = record(idField(), "{\"name\":\"x\",\"type\":\"string\",\"default\":\"DEF\"}");
    client.register(keySubject, new AvroSchema(v3));
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client);
    deserializer.configure(config("v1"), true);

    GenericRecord read = (GenericRecord) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, v3).getValue();
    assertEquals(7, read.get("id"));
    assertEquals("DEF", read.get("x").toString());
  }

  @Test
  void anAuthFailureReachesTheConsumerAsASchemaFetchsWould() throws Exception {
    // Not a record to skip: the deserializer leaves the authentication or authorization failure.
    for (int status : new int[] {401, 403}) {
      client = new ProvenanceMockSchemaRegistryClient() {
        @Override
        public SchemaProvenance getProvenanceById(String subject, int fromId, int toId,
            boolean includeInterior, boolean includeMultipleMessages, String algorithm)
            throws RestClientException {
          throw new RestClientException("refused", status, status * 100 + 1);
        }
      };
      serializer = new KafkaAvroSerializer(client, config(null));
      byte[] bytes = write(record(idField(), string("note")),
          new GenericRecordBuilder(record(idField(), string("note"))).set("id", 1)
              .set("note", "old"));
      Schema reader = record(idField(), string("memo"));
      client.register(SUBJECT, new AvroSchema(reader));

      Class<? extends RuntimeException> expected =
          status == 401 ? AuthenticationException.class : AuthorizationException.class;
      assertThrows(expected, () -> read(reader, bytes, "v1"));
    }
  }

  @Test
  void aReaderDifferingOnlyInDocsIsMatchedByStructure() throws Exception {
    // No version has its docs, but it has v3's structure: note, re-added there, is new.
    Schema v1 = record(idField(), string("note"));
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("note", "ada"));
    client.register(SUBJECT, new AvroSchema(record(idField())));
    client.register(SUBJECT, new AvroSchema(record(idField(),
        "{\"name\":\"note\",\"type\":\"string\",\"default\":\"new\"}")));
    Schema reader = record(idField(),
        "{\"name\":\"note\",\"type\":\"string\",\"default\":\"new\",\"doc\":\"again\"}");

    assertEquals("new", read(reader, bytes, "v1").get("note").toString());
  }

  @Test
  void aReaderDeclaringFieldsSymbolsOrBranchesInAnotherOrderIsMatchedByStructure()
      throws Exception {
    // Avro finds each by name, so each reader is still v3: note, re-added there, is new.
    String e = "{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[%s]}}";
    String u = "{\"name\":\"u\",\"type\":[%s]}";
    String ab = String.format(e, "\"A\",\"B\"");
    String intString = String.format(u, "\"int\",\"string\"");
    String note = "{\"name\":\"note\",\"type\":\"string\",\"default\":\"new\"}";
    Schema v1 = record(idField(), ab, intString, string("note"));
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7)
        .set("e", new GenericData.EnumSymbol(v1.getField("e").schema(), "B")).set("u", 3)
        .set("note", "old"));
    client.register(SUBJECT, new AvroSchema(record(idField(), ab, intString)));
    client.register(SUBJECT, new AvroSchema(record(idField(), ab, intString, note)));

    for (Schema reader : new Schema[] {
        record(note, intString, ab, idField()),
        record(idField(), String.format(e, "\"B\",\"A\""), intString, note),
        record(idField(), ab, String.format(u, "\"string\",\"int\""), note)}) {
      GenericRecord read = read(reader, bytes, "v1");
      assertEquals("new", read.get("note").toString(), reader.toString());
      assertEquals(3, read.get("u"), reader.toString());
      assertEquals("B", read.get("e").toString(), reader.toString());
    }
  }

  @Test
  void aReaderStandingForTheWritersOwnVersionReadsWithoutItsAliases() throws Exception {
    // f7's alias names f5, a field of its own: Avro would rename the writer's f5 onto f7. v2 only
    // reorders v1, so a reader in v1's text, reordered, is v2, the writer's version.
    String f7 = "{\"name\":\"f7\",\"type\":\"int\",\"aliases\":[\"f5\"]}";
    String f5 = "{\"name\":\"f5\",\"type\":\"float\"}";
    client.register(SUBJECT, new AvroSchema(record(idField(), f7, f5)));
    Schema v2 = record(f5, f7, idField());
    byte[] bytes = write(v2,
        new GenericRecordBuilder(v2).set("id", 7).set("f7", 1).set("f5", 3.5f));

    for (Schema reader : new Schema[] {v2, record(idField(), f5, f7)}) {
      GenericRecord read = read(reader, bytes, "v1");
      assertEquals(1, read.get("f7"), reader.toString());
      assertEquals(3.5f, read.get("f5"), reader.toString());
    }
  }

  @Test
  void withTheLatestVersionARecordOfTheLatestStillReads() throws Exception {
    // The latest, carrying its version, is looked up as the reader: the client keeps what it
    // registered, so a record of the latest still finds its version.
    Schema v1 = record(idField(), string("x"));
    byte[] old = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("x", "old"));
    client.register(SUBJECT, new AvroSchema(record(idField())));
    Schema v3 = record(idField(), "{\"name\":\"x\",\"type\":\"string\",\"default\":\"DEF\"}");
    byte[] latest = write(v3, new GenericRecordBuilder(v3).set("id", 8).set("x", "new"));
    Map<String, Object> config = config("v1");
    config.put("use.latest.version", true);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config);

    assertEquals("DEF", ((GenericRecord) deserializer.deserialize(TOPIC, old)).get("x").toString());
    assertEquals("new",
        ((GenericRecord) deserializer.deserialize(TOPIC, latest)).get("x").toString());
  }

  @Test
  void aReadRuleSeesTheReAddedColumnsDefault() throws Exception {
    // Domain rules run on the projected record: a re-added column holds its default, not the
    // dropped column's value, by the time a rule reads it.
    Schema v1 = record(idField(), string("name"));
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("name", "old"));
    client.register(SUBJECT, new AvroSchema(record(idField())));
    Rule bang = new Rule("bang", null, RuleKind.TRANSFORM, RuleMode.READ, "CEL_FIELD", null, null,
        "name == 'name' ; value + '!'", null, null, false);
    client.register(SUBJECT, new AvroSchema(
        record(idField(), "{\"name\":\"name\",\"type\":\"string\",\"default\":\"new\"}"))
        .copy(null, new RuleSet(null, Collections.singletonList(bang))));
    Map<String, Object> config = config("v1");
    config.put("use.latest.version", true);

    GenericRecord read = (GenericRecord) new KafkaAvroDeserializer(client, config)
        .deserialize(TOPIC, bytes);
    assertEquals(7, read.get("id"));
    assertEquals("new!", read.get("name").toString());
  }

  @Test
  void aReaderSchemaPassedWithEveryRecordIsNotRehashedForEach() throws Exception {
    // One reader schema passed with every record, as a caller holding it does: wrapped anew for
    // each, the provenance cache hashed the whole schema per record (8,000 union branches here).
    StringBuilder branches = new StringBuilder();
    for (int i = 0; i < 8000; i++) {
      branches.append(i > 0 ? "," : "").append("{\"type\":\"record\",\"name\":\"B").append(i)
          .append("\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}");
    }
    String union = "{\"name\":\"u\",\"type\":[" + branches + "]}";
    Schema v1 = record(union);
    Schema v2 = record(union, "{\"name\":\"w\",\"type\":\"int\",\"default\":-1}");
    GenericData.Record u = new GenericData.Record(v1.getField("u").schema().getTypes().get(0));
    u.put("x", 7);
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("u", u));
    client.register(SUBJECT, new AvroSchema(v2));
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config("v1"));
    deserializer.deserializeWithSchema(TOPIC, new RecordHeaders(), bytes, v2);

    GenericRecord read = assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
      GenericRecord last = null;
      for (int i = 0; i < 20000; i++) {
        last = (GenericRecord) deserializer.deserializeWithSchema(
            TOPIC, new RecordHeaders(), bytes, v2).getValue();
      }
      return last;
    });
    assertEquals(7, ((GenericRecord) read.get("u")).get("x"));
    assertEquals(-1, read.get("w"));
  }

  private byte[] write(Schema writer, GenericRecordBuilder record) throws Exception {
    client.register(SUBJECT, new AvroSchema(writer));
    return serializer.serialize(TOPIC, record.build());
  }

  private GenericRecord read(Schema reader, byte[] bytes, String provenance) {
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config(provenance));
    return (GenericRecord) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, reader).getValue();
  }

  private static Map<String, Object> config(String provenance) {
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", "bogus");
    config.put("auto.register.schemas", false);
    config.put("use.latest.version", false);
    if (provenance != null) {
      config.put("provenance.algorithm", provenance);
    }
    return config;
  }

  // The record then carries its schema's GUID, in a header, and no id.
  private static Map<String, Object> byGuid(Map<String, Object> config) {
    config.put("value.schema.id.serializer", HeaderSchemaIdSerializer.class.getName());
    return config;
  }

  private static String idField() {
    return "{\"name\":\"id\",\"type\":\"int\"}";
  }

  private static String string(String name) {
    return "{\"name\":\"" + name + "\",\"type\":\"string\"}";
  }

  private static Schema field(String type) {
    String json = type.startsWith("{") || type.startsWith("[") ? type : "\"" + type + "\"";
    return record("{\"name\":\"f\",\"type\":" + json + "}");
  }

  private static Schema defaulted(String type, String defaultValue) {
    String json = type.startsWith("[") ? type : "\"" + type + "\"";
    return record("{\"name\":\"f\",\"type\":" + json + ",\"default\":" + defaultValue + "}");
  }

  private static Schema enumField(String symbols, String defaultSymbol) {
    return field("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":" + symbols
        + (defaultSymbol == null ? "" : ",\"default\":\"" + defaultSymbol + "\"") + "}");
  }

  private static Schema record(String... fields) {
    return new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"MyRecord\","
        + "\"namespace\":\"io.confluent\",\"fields\":[" + String.join(",", fields) + "]}");
  }
}
