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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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
    assertThrows(Exception.class, () -> read(v3, branch, "v1"));
    assertEquals(5, ((GenericRecord) read(v3, branch, null).get("f")).get("x"));

    assertEquals("s", read(v3, write(v1, "s"), "v1").get("f").toString());
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
