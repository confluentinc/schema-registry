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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import io.confluent.kafka.serializers.provenance.ReaderSchema;
import io.confluent.kafka.serializers.test.Readded;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Deserialization strictness in Avro: a reader location whose provenance id is new never receives
 * the writer's value — a record branch with none fails the records containing it, and a union
 * choice the resolver would make against provenance falls back.
 */
class AvroProvenanceStrictnessTest {

  private static final String TOPIC = "strict";
  private static final String SUBJECT = TOPIC + "-value";
  private static final String A = "{\"type\":\"record\",\"name\":\"A\",\"fields\":"
      + "[{\"name\":\"x\",\"type\":\"int\",\"default\":0}]}";

  private ProvenanceMockSchemaRegistryClient client;

  @BeforeEach
  void init() {
    client = new ProvenanceMockSchemaRegistryClient();
  }

  @Test
  void aReAddedRecordBranchFailsOnlyTheRecordsContainingIt() throws Exception {
    Schema v1 = record("[\"string\"," + A + "]", "v1");
    register(v1, record("[\"string\",\"int\"]", "v2"));
    Schema v3 = record("[\"string\"," + A + "]", "v3");
    register(v3);
    Schema a = v1.getField("f").schema().getTypes().get(1);

    byte[] branch = write(v1, new GenericRecordBuilder(a).set("x", 5).build());
    // Named in the writer's terms, not by the renamer's sink.
    Exception e = assertThrows(SerializationException.class, () -> read(v3, branch, "v1"));
    assertEquals("The record holds a value of A at writer location [f], a union branch "
        + "provenance pairs with none of the reader's. There is no value to read.",
        e.getCause().getMessage());
    assertEquals(5, ((GenericRecord) read(v3, branch, null).get("f")).get("x"));

    assertEquals("s", read(v3, write(v1, "s"), "v1").get("f").toString());
  }

  @Test
  void aReAddedBranchOfARootUnionIsNamedAtTheRoot() throws Exception {
    String b = "{\"type\":\"record\",\"name\":\"B\",%s\"fields\":"
        + "[{\"name\":\"y\",\"type\":\"string\"}]}";
    Schema v1 = new Schema.Parser().parse("[\"null\"," + A + "," + String.format(b, "") + "]");
    Schema v3 = new Schema.Parser().parse(
        "[\"null\"," + A + "," + String.format(b, "\"doc\":\"v3\",") + "]");
    register(v1, new Schema.Parser().parse("[\"null\"," + A + ",\"int\"]"), v3);
    GenericRecord value = new GenericRecordBuilder(v1.getTypes().get(2)).set("y", "old").build();

    Exception e = assertThrows(SerializationException.class, () ->
        new KafkaAvroDeserializer(client, config("v1"))
            .deserializeWithSchema(TOPIC, new RecordHeaders(), framed(v1, value), v3));
    assertEquals("The record holds a value of B at the writer's root, a union branch "
        + "provenance pairs with none of the reader's. There is no value to read.",
        e.getCause().getMessage());
  }

  @Test
  void aReAddedFixedBranchIsNamedInTheWritersTerms() throws Exception {
    // A fixed or enum branch matches no reader branch at all; Avro's error names the throwaway.
    String f1 = "{\"type\":\"fixed\",\"name\":\"F1\",\"size\":4}";
    Schema v1 = record("[\"null\"," + f1 + ",\"string\"]", "v1");
    register(v1, record("[\"null\",\"int\",\"string\"]", "v2"));
    Schema v3 = record("[\"null\"," + f1 + ",\"string\"]", "v3");
    register(v3);
    Schema fixed = v1.getField("f").schema().getTypes().get(1);
    byte[] bytes = write(v1, new GenericData.Fixed(fixed, new byte[4]));

    Exception e = assertThrows(SerializationException.class, () -> read(v3, bytes, "v1"));
    assertEquals("The record holds a value of F1 at writer location [f], a union branch "
        + "provenance pairs with none of the reader's. There is no value to read.",
        e.getCause().getMessage());
  }

  @Test
  void aRootRenamedIntoAnotherNamespaceKeepsItsNestedTypes() throws Exception {
    // The nested N and the branch A inherit the root's namespace, so move with it, unaliased.
    String fields = "[{\"name\":\"o\",\"type\":{\"type\":\"record\",\"name\":\"N\","
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"default\":0}]}},"
        + "{\"name\":\"u\",\"type\":[\"null\",\"string\"," + A + "]}]}";
    Schema v1 = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"a.b\",\"fields\":" + fields);
    Schema v2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R2\","
        + "\"namespace\":\"a.c\",\"aliases\":[\"a.b.R\"],\"fields\":" + fields);
    register(v1, v2);
    GenericRecord value = new GenericRecordBuilder(v1)
        .set("o", new GenericRecordBuilder(v1.getField("o").schema()).set("x", 42).build())
        .set("u", new GenericRecordBuilder(v1.getField("u").schema().getTypes().get(2))
            .set("x", 5).build())
        .build();

    GenericRecord read = (GenericRecord) new KafkaAvroDeserializer(client, config("v1"))
        .deserializeWithSchema(TOPIC, new RecordHeaders(), framed(v1, value), v2).getValue();
    assertEquals(42, ((GenericRecord) read.get("o")).get("x"));
    assertEquals(5, ((GenericRecord) read.get("u")).get("x"));
  }

  @Test
  void aRootKeepsItsFieldsWhenATypeNamedInsideItGoes() throws Exception {
    // As a reflect schema does, E is named inside R; dropping it must leave n its value.
    Schema v1 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"q\","
        + "\"fields\":[{\"name\":\"n\",\"type\":\"string\"},{\"name\":\"e\",\"type\":{\"type\":"
        + "\"enum\",\"name\":\"E\",\"namespace\":\"q.R\",\"symbols\":[\"X\",\"Y\"]}}]}");
    Schema v2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"q\","
        + "\"doc\":\"v2\",\"fields\":[{\"name\":\"n\",\"type\":\"string\"}]}");
    client.register(SUBJECT, new AvroSchema(v1));
    client.register(SUBJECT, new AvroSchema(v2));
    GenericRecord value = new GenericData.Record(v1);
    value.put("n", "kept");
    value.put("e", new GenericData.EnumSymbol(v1.getField("e").schema(), "Y"));

    GenericRecord read = (GenericRecord) new KafkaAvroDeserializer(client, config("v1"))
        .deserializeWithSchema(TOPIC, new RecordHeaders(), framed(v1, value), v2).getValue();
    assertEquals("kept", read.get("n").toString());
  }

  @Test
  void aFixedBranchMovedWithItsRootsNamespaceReads() throws Exception {
    String fields = "[{\"name\":\"u\",\"type\":[\"null\",\"string\",{\"type\":\"fixed\","
        + "\"name\":\"F\",\"size\":4}]}]}";
    Schema v1 = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"a.b\",\"fields\":" + fields);
    Schema v2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R2\","
        + "\"namespace\":\"a.c\",\"aliases\":[\"a.b.R\"],\"fields\":" + fields);
    client.register(SUBJECT, new AvroSchema(v1));
    client.register(SUBJECT, new AvroSchema(v2));
    GenericRecord value = new GenericData.Record(v1);
    value.put("u", new GenericData.Fixed(v1.getField("u").schema().getTypes().get(2),
        new byte[] {1, 2, 3, 4}));

    GenericRecord read = (GenericRecord) new KafkaAvroDeserializer(client, config("v1"))
        .deserializeWithSchema(TOPIC, new RecordHeaders(), framed(v1, value), v2).getValue();
    assertEquals(4, ((GenericData.Fixed) read.get("u")).bytes()[3]);
  }

  @Test
  void aReAddedPrimitiveBranchFallsBack() throws Exception {
    Schema v1 = record("[\"int\",\"string\"]", "v1");
    Schema v3 = record("[\"int\",\"string\"]", "v3");
    register(v1, record("[\"string\",\"boolean\"]", "v2"), v3);
    assertThrows(ProvenanceUnavailableException.class, () -> rename(v1, v3));
  }

  @Test
  void aValueWidenedIntoANewBranchFallsBack() throws Exception {
    Schema v1 = record("\"int\"", "v1");
    Schema v2 = record("[\"int\",\"string\"]", "v2");
    register(v1, v2);
    assertThrows(ProvenanceUnavailableException.class, () -> rename(v1, v2));
  }

  @Test
  void aBranchPromotedUnambiguouslyReadsItsValue() throws Exception {
    Schema v1 = record("[\"null\",\"int\",\"string\"]", "v1");
    Schema v2 = record("[\"null\",\"long\",\"string\"]", "v2");
    register(v1, v2);
    assertEquals(7L, read(v2, write(v1, 7), "v1").get("f"));
  }

  @Test
  void aBranchPromotedTwoWaysFallsBack() throws Exception {
    Schema v1 = record("[\"int\",\"string\"]", "v1");
    Schema v2 = record("[\"long\",\"float\",\"string\"]", "v2");
    register(v1, v2);
    assertThrows(ProvenanceUnavailableException.class, () -> rename(v1, v2));
  }

  @Test
  void aConnectMapEntryValueFollowsItsProvenance() throws Exception {
    String entry = "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"MapEntry\","
        + "\"namespace\":\"io.confluent.connect.avro\",\"fields\":[{\"name\":\"key\","
        + "\"type\":\"int\"},{\"name\":\"value\",\"type\":{\"type\":\"record\",\"name\":\"V\","
        + "\"fields\":[%s]}}]}}";
    Schema v1 = record(String.format(entry, "{\"name\":\"x\",\"type\":\"int\"}"), "v1");
    Schema v2 = record(String.format(entry,
        "{\"name\":\"y\",\"type\":\"int\",\"aliases\":[\"x\"]}"), "v2");
    register(v1, v2);
    Schema mapEntry = v1.getField("f").schema().getElementType();
    Schema value = mapEntry.getField("value").schema();
    GenericRecord pair = new GenericRecordBuilder(mapEntry).set("key", 1)
        .set("value", new GenericRecordBuilder(value).set("x", 5).build()).build();

    Object read = read(v2, write(v1, new GenericData.Array<>(
        v1.getField("f").schema(), Arrays.asList(pair))), "v1").get("f");
    assertEquals(5, ((GenericRecord) ((GenericRecord) ((java.util.List<?>) read).get(0))
        .get("value")).get("y"));
  }

  @Test
  void aNewTypesAliasCannotMoveAPlacedBranch() throws Exception {
    // v2's new field v holds Q, aliased P. Avro applies a type alias across the whole writer, so
    // left on the reader it would rename u's P to Q, and u's value would match O by structure.
    String p = "{\"type\":\"record\",\"name\":\"P\",\"fields\":"
        + "[{\"name\":\"x\",\"type\":\"int\"}]}";
    Schema v1 = record("[\"null\",\"string\"," + p + "]", "v1");
    Schema v2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"f\",\"type\":[\"null\",\"string\",{\"type\":\"record\",\"name\":\"O\","
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"default\":0}]}," + p + "]},"
        + "{\"name\":\"v\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Q\","
        + "\"aliases\":[\"P\"],\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"default\":0}]}],"
        + "\"default\":null}]}");
    register(v2);
    Schema pType = v1.getField("f").schema().getTypes().get(2);
    GenericRecord read = read(v2, write(v1, new GenericRecordBuilder(pType).set("x", 5).build()),
        "v1");
    assertEquals("P", ((GenericRecord) read.get("f")).getSchema().getName());
  }

  @Test
  void aReaderFieldNamedLikeAThrowawayGetsNoOldValue() throws Exception {
    // b is dropped and re-added; its writer field is renamed so nothing matches it, and that name
    // must not be one the reader happens to use.
    Schema v1 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}");
    Schema v2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"}]}");
    Schema v3 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"__provenance_unmatched_1\",\"type\":\"int\",\"default\":0},"
        + "{\"name\":\"b\",\"type\":\"int\",\"default\":0}]}");
    client.register(SUBJECT, new AvroSchema(v1));
    byte[] bytes = new KafkaAvroSerializer(client, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("a", 1).set("b", 2).build());
    register(v2, v3);

    GenericRecord read = read(v3, bytes, "v1");
    assertEquals(0, read.get("__provenance_unmatched_1"));
    assertEquals(0, read.get("b"));
  }

  @Test
  void aRecordReadThroughARenameCarriesTheReadersOwnSchemas() throws Exception {
    // The renamer reads through a copy of the reader, aliases stripped and sinks added; a record
    // carrying the copy would not be the registered reader, and could not be serialized again.
    Schema v1 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"f\",\"type\":[\"string\",{\"type\":\"record\",\"name\":\"A\","
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"default\":0}]}]},"
        + "{\"name\":\"home\",\"type\":{\"type\":\"record\",\"name\":\"Home\",\"fields\":["
        + "{\"name\":\"city\",\"type\":\"string\"}]}}]}");
    Schema v2 = new Schema.Parser().parse(v1.toString().replace("\"A\"", "\"B\""));
    Schema v3 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"full_name\",\"type\":\"string\",\"aliases\":[\"name\"]},"
        + "{\"name\":\"f\",\"type\":[\"string\",{\"type\":\"record\",\"name\":\"A\","
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"default\":0}]}]},"
        + "{\"name\":\"home\",\"type\":{\"type\":\"record\",\"name\":\"House\","
        + "\"aliases\":[\"Home\"],\"fields\":[{\"name\":\"town\",\"type\":\"string\","
        + "\"aliases\":[\"city\"]}]}}]}");
    client.register(SUBJECT, new AvroSchema(v1));
    GenericRecord home = new GenericRecordBuilder(v1.getField("home").schema())
        .set("city", "c").build();
    byte[] bytes = new KafkaAvroSerializer(client, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("name", "ada").set("f", "s").set("home", home).build());
    register(v2, v3);

    GenericRecord read = read(v3, bytes, "v1");
    assertEquals(v3.toString(), read.getSchema().toString());
    assertEquals(v3.getField("home").schema().toString(),
        ((GenericRecord) read.get("home")).getSchema().toString());
    // And so it can be written again under the registered reader.
    new KafkaAvroSerializer(client, config(null)).serialize(TOPIC, read);
  }

  @Test
  void aSpecificReaderWithoutATypeGetsNoOldValue() throws Exception {
    // The reader comes from the generated class, found only when the datum reader is built;
    // provenance must see it too, rather than skip the read.
    Schema v1 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Readded\","
        + "\"namespace\":\"io.confluent.kafka.serializers.test\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"note\",\"type\":\"string\"}]}");
    Schema v2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Readded\","
        + "\"namespace\":\"io.confluent.kafka.serializers.test\",\"doc\":\"v2\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"}]}");
    client.register(SUBJECT, new AvroSchema(v1));
    byte[] bytes = new KafkaAvroSerializer(client, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("name", "ada").set("note", "old").build());
    register(v2, Readded.getClassSchema());

    assertEquals("old", readSpecific(bytes, null).getNote().toString());
    assertEquals("new", readSpecific(bytes, "v1").getNote().toString());
  }

  private Readded readSpecific(byte[] bytes, String provenance) {
    Map<String, Object> config = config(provenance);
    config.put("specific.avro.reader", true);
    return (Readded) new KafkaAvroDeserializer(client, config).deserialize(TOPIC, bytes);
  }

  @Test
  void aSuppliedReaderIdChoosesTheVersionAStructuralMatchWouldNot() throws Exception {
    // v1 and v3 are structurally equal; note was dropped at v2, so it is a new column in v3.
    Schema v1 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},"
        + "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\"}]}");
    Schema v2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"}]}");
    int v1Id = client.register(SUBJECT, new AvroSchema(v1));
    client.register(SUBJECT, new AvroSchema(v2));
    client.register(SUBJECT, withMetadata(v1, "v3"));
    byte[] bytes = new KafkaAvroSerializer(client, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("id", 7).set("note", "ada").build());
    // Flink's reader: its pinned v1 with other metadata merged on, registered nowhere.
    AvroSchema merged = withMetadata(v1, "merged");
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config("v1"));

    GenericRecord byStructure = (GenericRecord) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, writer -> merged, false).getValue();
    GenericRecord byId = (GenericRecord) deserializer.deserializeWithReaderSchema(
        TOPIC, new RecordHeaders(), bytes, writer -> ReaderSchema.of(merged, v1Id), false)
        .getValue();
    assertEquals("", byStructure.get("note").toString());
    assertEquals("ada", byId.get("note").toString());
  }

  // -------------------------------------------------------------------------------------------

  private static AvroSchema withMetadata(Schema schema, String value) {
    return new AvroSchema(schema).copy(
        new Metadata(null, Collections.singletonMap("version", value), null), null);
  }

  private void register(Schema... versions) throws Exception {
    for (Schema version : versions) {
      client.register(SUBJECT, new AvroSchema(version));
    }
  }

  private AvroProvenanceRenamer.Renamed rename(Schema writer, Schema reader) throws Exception {
    int writerId = client.getId(SUBJECT, new AvroSchema(writer));
    int readerId = client.getId(SUBJECT, new AvroSchema(reader));
    return AvroProvenanceRenamer.rename(writer, reader, ProvenanceMapping.join(
        client.getProvenanceById(SUBJECT, writerId, readerId, false, false, null),
        writerId, readerId));
  }

  private byte[] write(Schema writer, Object value) throws Exception {
    client.register(SUBJECT, new AvroSchema(writer));
    return new KafkaAvroSerializer(client, config(null))
        .serialize(TOPIC, new GenericRecordBuilder(writer).set("f", value).build());
  }

  // The serializer takes a record's own schema, not a root union: framed by hand.
  private byte[] framed(Schema writer, Object value) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0);
    out.write(ByteBuffer.allocate(4).putInt(client.getId(SUBJECT, new AvroSchema(writer))).array());
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<>(writer).write(value, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  private GenericRecord read(Schema reader, byte[] bytes, String provenance) {
    return (GenericRecord) new KafkaAvroDeserializer(client, config(provenance))
        .deserializeWithSchema(TOPIC, new RecordHeaders(), bytes, reader).getValue();
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

  private static Schema record(String fieldType, String doc) {
    return new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\",\"doc\":\"" + doc
        + "\",\"fields\":[{\"name\":\"f\",\"type\":" + fieldType + "}]}");
  }
}
