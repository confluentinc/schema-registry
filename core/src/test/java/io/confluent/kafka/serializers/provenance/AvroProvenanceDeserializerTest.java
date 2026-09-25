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

package io.confluent.kafka.serializers.provenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
    // A leaf becoming a union changes category, a drop and an add: Avro alone reads the value
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
