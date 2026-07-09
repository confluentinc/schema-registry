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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

/**
 * Serde-level scenarios demonstrating how the Confluent {@code __key_schema_id} header relates (or
 * fails to relate) to the raw key {@code byte[]}, when the key schema id is transported in the
 * record header via {@link HeaderSchemaIdSerializer} instead of the payload prefix.
 *
 * <p>Motivation: header-aware state stores (KIP-1271) preserve a record's headers alongside its
 * value, so a would-be versioned store row could end up holding several versions, each carrying its
 * own copy of the producing record's {@code __key_schema_id}. The row itself is identified by the
 * key {@code byte[]}. These tests establish, purely at the serializer/deserializer level (no state
 * store is involved — the header-aware versioned store does not exist yet), exactly what can happen
 * to that relationship. Each test's assertions are the demonstrated behavior.
 *
 * <p>Key fact under test: in header mode the key {@code byte[]} is the raw encoded data with no
 * schema-id bytes, so the schema id does NOT participate in row identity.
 */
public class HeaderSchemaIdRowKeyScenariosTest extends KafkaAvroSerializerTest {

  // Two same-shaped but differently-named key schemas. A single-string field encodes identically
  // regardless of the field/record name, so equal string values produce byte-identical payloads
  // while the schemas (and thus their registered GUIDs) differ.
  private static final Schema SCHEMA_A = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"RowKeyA\",\"namespace\":\"io.confluent.test\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");
  private static final Schema SCHEMA_B = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"RowKeyB\",\"namespace\":\"io.confluent.test\","
          + "\"fields\":[{\"name\":\"label\",\"type\":\"string\"}]}");
  // An unrelated numeric-keyed schema, used to model a header id that describes a different key.
  private static final Schema SCHEMA_C = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"OtherKey\",\"namespace\":\"io.confluent.test\","
          + "\"fields\":[{\"name\":\"n\",\"type\":\"long\"}]}");
  // A key schema and a byte-affecting evolution of it (adds a field with a default).
  private static final Schema SCHEMA_V1 = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"EvolvingKeyV1\",\"namespace\":\"io.confluent.test\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");
  private static final Schema SCHEMA_V2 = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"EvolvingKeyV2\",\"namespace\":\"io.confluent.test\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"},"
          + "{\"name\":\"region\",\"type\":\"string\",\"default\":\"US\"}]}");

  // Each schema is registered under its own topic/subject to keep auto-registration free of
  // subject-compatibility constraints; header-mode deserialization resolves the schema by the GUID
  // in the header, so the topic is irrelevant on the read path.
  private static final String TOPIC_A = "topic-a";
  private static final String TOPIC_B = "topic-b";
  private static final String TOPIC_C = "topic-c";
  private static final String TOPIC_V1 = "topic-v1";
  private static final String TOPIC_V2 = "topic-v2";

  private KafkaAvroSerializer headerModeKeySerializer() {
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
    serializer.configure(config, /* isKey= */ true);
    return serializer;
  }

  private KafkaAvroDeserializer keyDeserializer() {
    // Default DualSchemaIdDeserializer: reads the __key_schema_id header first, else the prefix.
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistry);
    deserializer.configure(config, /* isKey= */ true);
    return deserializer;
  }

  private static GenericRecord record(Schema schema, String field, Object value) {
    GenericRecord r = new GenericData.Record(schema);
    r.put(field, value);
    return r;
  }

  private static byte[] keySchemaIdHeader(RecordHeaders headers) {
    return headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER).value();
  }

  /**
   * Regime 1 happy path: the same key serialized twice yields the same row key bytes AND the same
   * {@code __key_schema_id}. When a single, fixed key serde is used, the invariant holds trivially.
   */
  @Test
  public void sameSchema_sameRowKey_sameHeaderId() {
    KafkaAvroSerializer serializer = headerModeKeySerializer();
    GenericRecord key = record(SCHEMA_A, "id", "row-1");

    RecordHeaders headers1 = new RecordHeaders();
    byte[] rowKey1 = serializer.serialize(TOPIC_A, headers1, key);
    RecordHeaders headers2 = new RecordHeaders();
    byte[] rowKey2 = serializer.serialize(TOPIC_A, headers2, key);

    assertArrayEquals("same key -> same row key bytes", rowKey1, rowKey2);
    assertArrayEquals("same key -> same __key_schema_id",
        keySchemaIdHeader(headers1), keySchemaIdHeader(headers2));
  }

  /**
   * Header mode collapses rows: two DIFFERENT key schemas whose values encode to identical bytes
   * produce the same row key bytes (so they would land in the same row) while carrying DIFFERENT,
   * non-equal {@code __key_schema_id} GUIDs. The schema id plays no part in row identity here.
   */
  @Test
  public void differentSchemaIds_sameRowKey_headerModeCollapsesRows() {
    KafkaAvroSerializer serializer = headerModeKeySerializer();

    RecordHeaders headersA = new RecordHeaders();
    byte[] rowKeyA = serializer.serialize(TOPIC_A, headersA, record(SCHEMA_A, "id", "row-1"));
    RecordHeaders headersB = new RecordHeaders();
    byte[] rowKeyB = serializer.serialize(TOPIC_B, headersB, record(SCHEMA_B, "label", "row-1"));

    assertArrayEquals("distinct schemas, identical row key bytes", rowKeyA, rowKeyB);
    assertFalse("but the stored key schema ids differ",
        Arrays.equals(keySchemaIdHeader(headersA), keySchemaIdHeader(headersB)));
  }

  /**
   * Semantic divergence: given one shared row key byte[], deserializing it with one version's header
   * vs another's yields DIFFERENT logical keys. So "which stored header you trust" changes the
   * reconstructed key — you cannot pick an arbitrary version's {@code __key_schema_id}.
   */
  @Test
  public void sameRowKey_differentHeaderId_decodesToDifferentKey() {
    KafkaAvroSerializer serializer = headerModeKeySerializer();
    KafkaAvroDeserializer deserializer = keyDeserializer();

    RecordHeaders headersA = new RecordHeaders();
    byte[] rowKey = serializer.serialize(TOPIC_A, headersA, record(SCHEMA_A, "id", "row-1"));
    RecordHeaders headersB = new RecordHeaders();
    serializer.serialize(TOPIC_B, headersB, record(SCHEMA_B, "label", "row-1"));

    Object decodedWithA = deserializer.deserialize(TOPIC_A, headersA, rowKey);
    Object decodedWithB = deserializer.deserialize(TOPIC_A, headersB, rowKey);

    assertNotEquals("same bytes, different header id -> different decoded key",
        decodedWithA, decodedWithB);
  }

  /**
   * The changelog/restore hazard: a stored {@code __key_schema_id} need not describe the row key at
   * all. Here the row key bytes encode a string key, but the header carries the id of an unrelated
   * numeric schema (as happens after re-keying / Processor-API puts, where the stored header comes
   * from the source record, not the store key). Decoding silently produces the wrong record instead
   * of failing.
   */
  @Test
  public void storedHeaderIdUnrelatedToRowKey_decodesToWrongRecord() {
    KafkaAvroSerializer serializer = headerModeKeySerializer();
    KafkaAvroDeserializer deserializer = keyDeserializer();

    GenericRecord realKey = record(SCHEMA_A, "id", "row-1");
    RecordHeaders realHeaders = new RecordHeaders();
    byte[] rowKey = serializer.serialize(TOPIC_A, realHeaders, realKey);

    // A header describing a completely different (numeric) key, e.g. carried over from a source
    // record after re-keying.
    RecordHeaders unrelatedHeaders = new RecordHeaders();
    serializer.serialize(TOPIC_C, unrelatedHeaders, record(SCHEMA_C, "n", 5L));

    Object decoded = deserializer.deserialize(TOPIC_A, unrelatedHeaders, rowKey);

    assertNotNull(decoded);
    assertEquals("the unrelated header id decodes the row key under the wrong schema",
        "OtherKey", ((GenericRecord) decoded).getSchema().getName());
    assertNotEquals("the real key is not recovered", realKey, decoded);
  }

  /**
   * Duplicate headers, last wins: {@link HeaderSchemaIdSerializer} appends and the default
   * DualSchemaIdDeserializer reads {@code lastHeader}. If two {@code __key_schema_id} headers are
   * present (e.g. the store key serde adds one to a record that already carries the source record's
   * header), the LAST one silently decides the schema.
   */
  @Test
  public void duplicateKeySchemaIdHeaders_lastWins() {
    KafkaAvroSerializer serializer = headerModeKeySerializer();
    KafkaAvroDeserializer deserializer = keyDeserializer();

    RecordHeaders headersA = new RecordHeaders();
    byte[] rowKey = serializer.serialize(TOPIC_A, headersA, record(SCHEMA_A, "id", "row-1"));
    GenericRecord keyB = record(SCHEMA_B, "label", "row-1");
    RecordHeaders headersB = new RecordHeaders();
    serializer.serialize(TOPIC_B, headersB, keyB);

    RecordHeaders duplicate = new RecordHeaders();
    duplicate.add(SchemaId.KEY_SCHEMA_ID_HEADER, keySchemaIdHeader(headersA));
    duplicate.add(SchemaId.KEY_SCHEMA_ID_HEADER, keySchemaIdHeader(headersB));

    Object decoded = deserializer.deserialize(TOPIC_A, duplicate, rowKey);

    assertEquals("the last __key_schema_id header (B) is the one used", keyB, decoded);
  }

  /**
   * Regime 1 boundary: a byte-affecting key schema evolution (adding a field with a default) changes
   * the encoded key bytes for the "same" logical key, so the versions self-separate into different
   * rows rather than mixing schema ids within one row.
   */
  @Test
  public void keySchemaEvolutionChangesBytes_selfSeparatesRows() {
    KafkaAvroSerializer serializer = headerModeKeySerializer();

    byte[] rowKeyV1 =
        serializer.serialize(TOPIC_V1, new RecordHeaders(), record(SCHEMA_V1, "id", "row-1"));
    GenericRecord keyV2 = new GenericData.Record(SCHEMA_V2);
    keyV2.put("id", "row-1");
    keyV2.put("region", "US");
    byte[] rowKeyV2 = serializer.serialize(TOPIC_V2, new RecordHeaders(), keyV2);

    assertFalse("adding a field changes the encoded key bytes -> a different row",
        Arrays.equals(rowKeyV1, rowKeyV2));
  }
}
