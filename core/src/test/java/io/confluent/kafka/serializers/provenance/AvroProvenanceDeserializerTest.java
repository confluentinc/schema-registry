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
import static org.junit.jupiter.api.Assertions.assertNull;
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
import io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth.exceptions.SchemaRegistryOauthTokenRetrieverException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.provenance.strategy.StablePidProvenanceStrategy;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.lang.ref.WeakReference;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.FutureTask;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
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
  void aReaderPinnedByVersionKeepsItsPinThroughAReconfigureDuringTheRead() throws Exception {
    // v3 re-adds note: the reader, v1 with a rule merged onto it, is pinned to v1, so v3's note
    // is new to it; unpinned, it would be found by structure as v3 and read v3's note.
    Gated gated = new Gated();
    Schema v1 = record(idField(), "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\"}");
    gated.register(SUBJECT, new AvroSchema(v1));
    gated.register(SUBJECT, new AvroSchema(record(idField())));
    Schema v3 = record(idField(), "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\","
        + "\"doc\":\"re-added\"}");
    gated.register(SUBJECT, new AvroSchema(v3));
    byte[] bytes = new KafkaAvroSerializer(gated, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v3).set("id", 7).set("note", "v3's").build());
    AvroSchema reader = new AvroSchema(v1).copy(null, new RuleSet(null, Collections.singletonList(
        new Rule("r", null, RuleKind.CONDITION, RuleMode.READ, "CEL", null, null, "true", null,
            null, false))));
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(gated, config("v1"));
    gated.armed = true;
    // A FutureTask bounds the wait for the read and rethrows its own failure.
    FutureTask<Object> read = new FutureTask<>(() -> deserializer.deserializeWithReaderSchema(
        TOPIC, new RecordHeaders(), bytes, w -> ReaderSchema.of(reader, SUBJECT, 1), false)
        .getValue());
    new Thread(read).start();
    assertTrue(gated.entered.await(10, TimeUnit.SECONDS), "the read never reached the gate");
    // The reconfigure lands while the read is fetching its writer.
    deserializer.configure(config("v1"), false);
    gated.release.countDown();
    assertEquals("", ((GenericRecord) read.get(10, TimeUnit.SECONDS)).get("note").toString());
  }

  @Test
  void aTransientFailureOfTheClientsOwnLookupIsNotCached() throws Exception {
    // The reader's exact lookup fails once, unchecked: that record fails, and the next reads.
    Gated gated = new Gated();
    Schema v1 = record(idField());
    gated.register(SUBJECT, new AvroSchema(v1));
    AvroSchema v2 = new AvroSchema(record(idField(), "{\"name\":\"n\",\"type\":\"int\","
        + "\"default\":0}"));
    gated.register(SUBJECT, v2);
    byte[] bytes = new KafkaAvroSerializer(gated, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("id", 7).build());
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(gated, config("v1"));
    gated.nextVersionLookupFailure = new UncheckedIOException(new IOException("connection reset"));
    assertThrows(SerializationException.class, () -> deserializer.deserializeWithSchema(TOPIC,
        new RecordHeaders(), bytes, w -> v2));
    GenericRecord read = (GenericRecord) deserializer.deserializeWithSchema(TOPIC,
        new RecordHeaders(), bytes, w -> v2).getValue();
    assertEquals(7, read.get("id"));
  }

  @Test
  void anOauthTokenNotHadNowFailsTheRecordUncached() throws Exception {
    // As a schema fetch failing for its token: that record fails, and the next reads.
    Gated gated = new Gated();
    Schema v1 = record(idField());
    gated.register(SUBJECT, new AvroSchema(v1));
    AvroSchema v2 = new AvroSchema(record(idField(), "{\"name\":\"n\",\"type\":\"int\","
        + "\"default\":0}"));
    gated.register(SUBJECT, v2);
    byte[] bytes = new KafkaAvroSerializer(gated, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("id", 7).build());
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(gated, config("v1"));
    gated.nextVersionLookupFailure = oauthTokenNotHad();
    assertThrows(SerializationException.class, () -> deserializer.deserializeWithSchema(TOPIC,
        new RecordHeaders(), bytes, w -> v2));
    GenericRecord read = (GenericRecord) deserializer.deserializeWithSchema(TOPIC,
        new RecordHeaders(), bytes, w -> v2).getValue();
    assertEquals(7, read.get("id"));
  }

  @Test
  void anOauthTokenNotHadNowForTheProvenanceRequestFailsTheRecordUncached() throws Exception {
    Gated gated = new Gated();
    byte[] bytes = idOnlyThenDefaultedN(gated);
    gated.nextProvenanceFailure = oauthTokenNotHad();
    assertSecondRecordReads(gated, bytes);
  }

  @Test
  void aRawKafkaExceptionFromTheClientsOwnLookupFailsTheRecordUncached() throws Exception {
    // As the client's token retrievers throw on an identity provider's 4xx, and its SSL factory
    // on a keystore it cannot load: that record fails, and the next reads.
    Gated gated = new Gated();
    byte[] bytes = idOnlyThenDefaultedN(gated);
    gated.nextVersionLookupFailure = rawKafkaException();
    assertSecondRecordReads(gated, bytes);
  }

  @Test
  void aRawKafkaExceptionFromTheProvenanceRequestFailsTheRecordUncached() throws Exception {
    Gated gated = new Gated();
    byte[] bytes = idOnlyThenDefaultedN(gated);
    gated.nextProvenanceFailure = rawKafkaException();
    assertSecondRecordReads(gated, bytes);
  }

  @Test
  void aServerWithoutTheProvenanceEndpointReadsNatively() throws Exception {
    // An older server answers 404: the pair reads as without provenance, asked once, not per record.
    Gated gated = new Gated();
    Schema v1 = record(idField(), string("note"));
    Schema v3 = record(idField(), "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\"}");
    gated.register(SUBJECT, new AvroSchema(v1));
    gated.register(SUBJECT, new AvroSchema(record(idField())));
    gated.register(SUBJECT, new AvroSchema(v3));
    byte[] bytes = new KafkaAvroSerializer(gated, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("id", 7).set("note", "old").build());
    gated.provenanceAnswer = new RestClientException("HTTP 404 Not Found", 404, 404);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(gated, config("v1"));
    try (Warnings warnings = new Warnings(ProvenanceProjector.class)) {
      for (int i = 0; i < 2; i++) {
        assertEquals("old", ((GenericRecord) deserializer.deserializeWithSchema(TOPIC,
            new RecordHeaders(), bytes, v3).getValue()).get("note").toString());
      }
      assertEquals(1, warnings.messages.size(), warnings.messages.toString());
      assertTrue(warnings.messages.get(0).startsWith("No provenance for "),
          warnings.messages.get(0));
    }
    assertEquals(1, gated.provenanceCalls);
  }

  @Test
  void aReaderDifferingOnlyInDocsBuildsRecordsUnderItsOwnSchema() throws Exception {
    // Avro's equality ignores docs: a datum reader shared by the two built r5's records as r3's.
    String note = "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\",\"doc\":\"%s\"}";
    Schema v1 = record(idField(), string("note"));
    Schema r3 = record(idField(), String.format(note, "r3"));
    Schema r5 = record(idField(), String.format(note, "r5"));
    ProvenanceMockSchemaRegistryClient client = new ProvenanceMockSchemaRegistryClient();
    client.register(SUBJECT, new AvroSchema(v1));
    client.register(SUBJECT, new AvroSchema(record(idField())));
    client.register(SUBJECT, new AvroSchema(r3));
    client.register(SUBJECT, new AvroSchema(r5));
    byte[] bytes = new KafkaAvroSerializer(client, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("id", 7).set("note", "old").build());
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config("v1"));
    for (Schema reader : Arrays.asList(r3, r5)) {
      GenericRecord read = (GenericRecord) deserializer.deserializeWithSchema(TOPIC,
          new RecordHeaders(), bytes, reader).getValue();
      assertEquals("", read.get("note").toString());
      assertEquals(reader.getField("note").doc(), read.getSchema().getField("note").doc());
    }
  }

  @Test
  void aTypeRenamedOntoOneKeptElsewhereInAUnionStillReadsByProvenance() throws Exception {
    // T is kept at p and A renamed onto it at q. Matched as copies are, q's T is no clone the
    // resolver would read into o.T: the pair reads by provenance, and the re-added k is new.
    // An error type too: Avro's equality and resolver ignore that it is one.
    for (String kind : Arrays.asList("record", "error")) {
      String t = "\"name\":\"T\",\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"default\":0}]}";
      String other = "{\"type\":\"" + kind + "\",\"namespace\":\"o\"," + t;
      String head = "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"n\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"int\"},";
      String rest = "{\"name\":\"p\",\"type\":[\"null\",{\"type\":\"" + kind
          + "\",\"aliases\":[\"A\"]," + t + "],\"default\":null},"
          + "{\"name\":\"q\",\"type\":[\"null\",\"string\",\"T\"," + other + "],\"default\":null}]}";
      Schema v1 = new Schema.Parser().parse(head + "{\"name\":\"k\",\"type\":\"int\"},"
          + "{\"name\":\"p\",\"type\":[\"null\",{\"type\":\"" + kind + "\"," + t
          + "],\"default\":null},"
          + "{\"name\":\"q\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"A\",\"fields\":"
          + "[{\"name\":\"x\",\"type\":\"int\"}]},\"string\"," + other + "],\"default\":null}]}");
      Schema v3 = new Schema.Parser().parse(
          head + "{\"name\":\"k\",\"type\":\"int\",\"default\":0}," + rest);
      ProvenanceMockSchemaRegistryClient client = new ProvenanceMockSchemaRegistryClient();
      client.updateCompatibility(SUBJECT, "NONE");
      client.register(SUBJECT, new AvroSchema(v1));
      client.register(SUBJECT, new AvroSchema(new Schema.Parser().parse(head + rest)));
      client.register(SUBJECT, new AvroSchema(v3));
      Schema a = v1.getField("q").schema().getTypes().get(1);
      byte[] bytes = new KafkaAvroSerializer(client, config(null)).serialize(TOPIC,
          new GenericRecordBuilder(v1).set("id", 1).set("k", 5).set("p", null)
              .set("q", new GenericRecordBuilder(a).set("x", 7).build()).build());
      GenericRecord read = (GenericRecord) new KafkaAvroDeserializer(client, config("v1"))
          .deserializeWithSchema(TOPIC, new RecordHeaders(), bytes, v3).getValue();
      assertEquals(0, read.get("k"), kind);
      assertEquals(7, ((GenericRecord) read.get("q")).get("x"), kind);
    }
  }

  @Test
  void aReconfigureReachesTheDatumReaderOfAPairReadBefore() throws Exception {
    // The projection and the datum reader it holds belong to the configuration they were built
    // under: turning the logical-type converters on reads a decimal as a BigDecimal.
    String amount = "{\"name\":\"amount\",\"type\":{\"type\":\"bytes\","
        + "\"logicalType\":\"decimal\",\"precision\":5,\"scale\":2}}";
    Schema v1 = record(idField(), amount);
    Schema v2 = record(idField(), amount,
        "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\"}");
    ProvenanceMockSchemaRegistryClient client = new ProvenanceMockSchemaRegistryClient();
    client.register(SUBJECT, new AvroSchema(v1));
    client.register(SUBJECT, new AvroSchema(v2));
    byte[] bytes = new KafkaAvroSerializer(client, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("id", 7)
            .set("amount", ByteBuffer.wrap(new BigDecimal("1.25").unscaledValue().toByteArray()))
            .build());
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config("v1"));
    assertTrue(((GenericRecord) deserializer.deserializeWithSchema(TOPIC, new RecordHeaders(),
        bytes, v2).getValue()).get("amount") instanceof ByteBuffer);
    Map<String, Object> converting = config("v1");
    converting.put("avro.use.logical.type.converters", true);
    deserializer.configure(converting, false);
    assertEquals(new BigDecimal("1.25"), ((GenericRecord) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, v2).getValue()).get("amount"));
  }

  @Test
  void aRecordNameStrategySubjectIsReadByProvenance() throws Exception {
    // The subject is the record's full name; note, re-added, takes its default.
    Schema v1 = record(idField(), string("note"));
    Schema v3 = record(idField(), "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\"}");
    String subject = v1.getFullName();
    client.register(subject, new AvroSchema(v1));
    client.register(subject, new AvroSchema(record(idField())));
    client.register(subject, new AvroSchema(v3));
    Map<String, Object> config = config(null);
    config.put("value.subject.name.strategy", RecordNameStrategy.class.getName());
    byte[] bytes = new KafkaAvroSerializer(client, config).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("id", 7).set("note", "old").build());
    config.put("provenance.algorithm", "v1");
    GenericRecord read = (GenericRecord) new KafkaAvroDeserializer(client, config)
        .deserializeWithSchema(TOPIC, new RecordHeaders(), bytes, v3).getValue();
    assertEquals("", read.get("note").toString());
  }

  // v1 {id}, v2 {id, n = 0}: a v1 record, read with v2 by provenance.
  private static byte[] idOnlyThenDefaultedN(Gated gated) throws Exception {
    Schema v1 = record(idField());
    gated.register(SUBJECT, new AvroSchema(v1));
    gated.register(SUBJECT, new AvroSchema(record(idField(),
        "{\"name\":\"n\",\"type\":\"int\",\"default\":0}")));
    return new KafkaAvroSerializer(gated, config(null)).serialize(TOPIC,
        new GenericRecordBuilder(v1).set("id", 7).build());
  }

  // The first record fails, as the failure set for it; the next reads, as nothing was cached.
  private static void assertSecondRecordReads(Gated gated, byte[] bytes) throws Exception {
    Schema v2 = record(idField(), "{\"name\":\"n\",\"type\":\"int\",\"default\":0}");
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(gated, config("v1"));
    assertThrows(SerializationException.class, () -> deserializer.deserializeWithSchema(TOPIC,
        new RecordHeaders(), bytes, v2));
    GenericRecord read = (GenericRecord) deserializer.deserializeWithSchema(TOPIC,
        new RecordHeaders(), bytes, v2).getValue();
    assertEquals(7, read.get("id"));
  }

  private static RuntimeException oauthTokenNotHad() {
    return new SchemaRegistryOauthTokenRetrieverException(
        "Failed to Retrieve OAuth Token for Schema Registry", new RuntimeException("idp"));
  }

  private static RuntimeException rawKafkaException() {
    return new KafkaException(new IOException("The response code 401 was encountered"));
  }

  @Test
  void aStablePidStrategyReadsByTheColumnIdsItIsGiven() throws Exception {
    // note dropped and re-added: a new column id reads the default, as the registry's pids do;
    // ids claiming note continued would read the old value, so the ids decide.
    Schema v1 = record(idField(), string("note"));
    Schema v3 = record(idField(), "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\"}");
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("note", "old"));
    client.register(SUBJECT, new AvroSchema(record(idField())));
    client.register(SUBJECT, new AvroSchema(v3));

    assertEquals("", readByColumnIds(v3, bytes, 3).get("note").toString());
    assertEquals("old", readByColumnIds(v3, bytes, 2).get("note").toString());
  }

  @Test
  void aNullableFieldReAddedWithoutDefaultReadsNull() throws Exception {
    // f is new to the reader and declares no default, but its union lets it be null: it reads
    // null, as a column added later reads null for older rows, rather than failing the record.
    Schema v1 = record(idField(), "{\"name\":\"f\",\"type\":\"int\"}");
    Schema v2 = record(idField());
    Schema v3 = record(idField(), "{\"name\":\"f\",\"type\":[\"null\",\"int\"]}");
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7).set("f", 11));
    client.register(SUBJECT, new AvroSchema(v2));
    client.register(SUBJECT, new AvroSchema(v3));

    GenericRecord read = read(v3, bytes, "v1");
    assertEquals(7, read.get("id"));
    assertNull(read.get("f"));
  }

  @Test
  void aFieldWidenedIntoANullableUnionWithoutDefaultReadsNull() throws Exception {
    // A kind change, so a new column: null where the union lists null first, as a default must;
    // with null elsewhere there is still nothing to read.
    Schema writer = field("int");
    byte[] bytes = write(writer, new GenericRecordBuilder(writer).set("f", 7));
    Schema nullFirst = field("[\"null\",\"int\",\"string\"]");
    client.register(SUBJECT, new AvroSchema(nullFirst));
    assertEquals(7, read(nullFirst, bytes, null).get("f"));
    assertNull(read(nullFirst, bytes, "v1").get("f"));

    Schema nullLast = field("[\"int\",\"string\",\"null\"]");
    client.register(SUBJECT, new AvroSchema(nullLast));
    assertThrows(Exception.class, () -> read(nullLast, bytes, "v1"));
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

    SerializationException e =
        assertThrows(SerializationException.class, () -> read(reader, bytes, "v1"));
    assertTrue(causes(e).contains("token endpoint unreachable"), causes(e));
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

  @Test
  void aReaderSchemaParsedForOneRecordIsNotKeptOnceDropped() throws Exception {
    // A caller parsing its reader anew for each record: the wrapper kept per schema must not
    // keep the schema itself, as equal readers share one cached outcome anyway.
    Schema v1 = record(idField());
    String v2 = record(idField(), "{\"name\":\"w\",\"type\":\"int\",\"default\":-1}").toString();
    byte[] bytes = write(v1, new GenericRecordBuilder(v1).set("id", 7));
    client.register(SUBJECT, new AvroSchema(v2));
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config("v1"));
    deserializer.deserializeWithSchema(TOPIC, new RecordHeaders(), bytes,
        new Schema.Parser().parse(v2));

    WeakReference<Schema> once = readOnce(deserializer, bytes, v2);
    // A full collection clears a weak reference it finds unreachable, so no wait is needed.
    for (int i = 0; i < 20 && once.get() != null; i++) {
      System.gc();
    }
    assertNull(once.get());
  }

  private static WeakReference<Schema> readOnce(KafkaAvroDeserializer deserializer, byte[] bytes,
      String reader) {
    Schema schema = new Schema.Parser().parse(reader);
    GenericRecord read = (GenericRecord) deserializer.deserializeWithSchema(
        TOPIC, new RecordHeaders(), bytes, schema).getValue();
    assertEquals(-1, read.get("w"));
    return new WeakReference<>(schema);
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

  // v1 [id, note], v2 [id], v3 [id, note], with v3's note under column id noteId.
  private GenericRecord readByColumnIds(Schema reader, byte[] bytes, int noteId) {
    Map<Integer, Map<List<Integer>, Integer>> pids = new HashMap<>();
    pids.put(1, ColumnIds.of(1, 2));
    pids.put(2, ColumnIds.of(1));
    pids.put(3, ColumnIds.of(1, noteId));
    Map<String, Object> config = config("v1");
    config.put("provenance.strategy", ColumnIds.class);
    config.put(ColumnIds.PIDS, pids);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config);
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

  // Collects what a class logs at WARN while open.
  private static final class Warnings extends AbstractAppender implements AutoCloseable {

    final List<String> messages = new CopyOnWriteArrayList<>();
    private final Logger logger;

    Warnings(Class<?> type) {
      super("warnings-" + type.getSimpleName(), null, null, true, Property.EMPTY_ARRAY);
      logger = (Logger) LogManager.getLogger(type);
      start();
      logger.addAppender(this);
    }

    @Override
    public void append(LogEvent event) {
      if (event.getLevel() == Level.WARN) {
        messages.add(event.getMessage().getFormattedMessage());
      }
    }

    @Override
    public void close() {
      logger.removeAppender(this);
      stop();
    }
  }

  // Blocks the next writer fetch once armed, until released; fails the next version lookup or
  // provenance request with what it is given, once; answers every provenance request with an
  // error, as a server without the endpoint does, once given one.
  private static final class Gated extends ProvenanceMockSchemaRegistryClient {

    volatile boolean armed;
    volatile RuntimeException nextVersionLookupFailure;
    volatile RuntimeException nextProvenanceFailure;
    volatile RestClientException provenanceAnswer;
    volatile int provenanceCalls;
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    @Override
    public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
        throws IOException, RestClientException {
      if (armed) {
        armed = false;
        entered.countDown();
        try {
          release.await();
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      }
      return super.getSchemaBySubjectAndId(subject, id);
    }

    @Override
    public int getVersion(String subject, ParsedSchema schema)
        throws IOException, RestClientException {
      RuntimeException failure = nextVersionLookupFailure;
      if (failure != null) {
        nextVersionLookupFailure = null;
        throw failure;
      }
      return super.getVersion(subject, schema);
    }

    @Override
    public SchemaProvenance getProvenanceById(String subject, int fromId, int toId,
        boolean includeInterior, boolean includeMultipleMessages, String algorithm)
        throws IOException, RestClientException {
      provenanceCalls++;
      RuntimeException failure = nextProvenanceFailure;
      if (failure != null) {
        nextProvenanceFailure = null;
        throw failure;
      }
      if (provenanceAnswer != null) {
        throw provenanceAnswer;
      }
      return super.getProvenanceById(
          subject, fromId, toId, includeInterior, includeMultipleMessages, algorithm);
    }
  }

  /** Column ids handed over through the deserializer's configs, as a Metastore's might be. */
  public static final class ColumnIds extends StablePidProvenanceStrategy {

    static final String PIDS = "test.column.ids";

    private Map<Integer, Map<List<Integer>, Integer>> pids;

    // Top-level paths [0], [1], ... to the given column ids.
    static Map<List<Integer>, Integer> of(int... columnIds) {
      Map<List<Integer>, Integer> byPath = new HashMap<>();
      for (int i = 0; i < columnIds.length; i++) {
        byPath.put(Collections.singletonList(i), columnIds[i]);
      }
      return byPath;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
      super.configure(configs);
      pids = (Map<Integer, Map<List<Integer>, Integer>>) configs.get(PIDS);
    }

    @Override
    protected Map<List<Integer>, Integer> pids(String subject, int version,
        boolean includeMultipleMessages) {
      return pids.get(version);
    }
  }

  // Every message in the cause chain, so a test can name the failure it expects.
  private static String causes(Throwable e) {
    StringBuilder chain = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      chain.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append('\n');
    }
    return chain.toString();
  }

}
