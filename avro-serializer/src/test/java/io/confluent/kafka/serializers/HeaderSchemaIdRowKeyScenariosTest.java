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
   * Happy path: the same key serialized twice yields the same row key bytes AND the same
   * {@code __key_schema_id}. When a single, fixed key serde is used, the invariant holds trivially.
   *
   * <p>How this arises in practice: the normal, healthy case. A materialized source table (or any
   * store) is fed by records whose keys all use one registered schema and one fixed key serde. The
   * same logical key always encodes to the same bytes and stamps the same GUID, so the stored
   * {@code __key_schema_id} genuinely describes the row. The remaining tests show how this breaks once
   * more than one schema, an unrelated id, or a byte-affecting change enters the picture.
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
   * Two DIFFERENT key schemas whose values encode to identical bytes produce the SAME row key
   * byte[] while carrying DIFFERENT {@code __key_schema_id} GUIDs. Because in header mode the schema
   * id lives in the header and not in the bytes, it plays no part in row identity, which leads to
   * three linked consequences demonstrated here in order:
   * <ol>
   *   <li><b>Row collapse:</b> identical row key bytes, different GUIDs &rarr; the two distinct keys
   *       would land in the same store row.</li>
   *   <li><b>Ambiguous decode:</b> that one shared byte[] deserializes to a DIFFERENT logical key
   *       depending on which stored {@code __key_schema_id} you trust &mdash; there is no single
   *       correct reconstruction.</li>
   *   <li><b>Duplicate headers, last wins:</b> if both GUIDs are present as two
   *       {@code __key_schema_id} headers, the default {@code DualSchemaIdDeserializer} silently uses
   *       the LAST one ({@code lastHeader}).</li>
   * </ol>
   *
   * <p>How this arises in practice: two producers (or two topics merged/joined into one store) use
   * different key schemas that are logically the same shape &mdash; e.g. teams pick different record
   * names/namespaces for an equal key, or a schema is renamed &mdash; so equal key values encode to
   * byte-identical payloads under different registered GUIDs. Both records land in one store keyed by
   * those bytes, each version keeping its own {@code __key_schema_id}. The duplicate-header case
   * arises when a record that already carries a source {@code __key_schema_id} is re-serialized by a
   * store key serde that appends another (KIP-1271 preserves the original headers).
   */
  @Test
  public void twoSchemasSameRowKeyBytes_collapseAmbiguousDecodeLastWins() {
    KafkaAvroSerializer serializer = headerModeKeySerializer();
    KafkaAvroDeserializer deserializer = keyDeserializer();

    GenericRecord keyA = record(SCHEMA_A, "id", "row-1");
    GenericRecord keyB = record(SCHEMA_B, "label", "row-1");

    RecordHeaders headersA = new RecordHeaders();
    byte[] rowKeyA = serializer.serialize(TOPIC_A, headersA, keyA);
    RecordHeaders headersB = new RecordHeaders();
    byte[] rowKeyB = serializer.serialize(TOPIC_B, headersB, keyB);

    // (1) Row collapse: identical bytes, but different (non-equal) stored key schema ids.
    assertArrayEquals("distinct schemas, identical row key bytes", rowKeyA, rowKeyB);
    assertFalse("but the stored key schema ids differ",
        Arrays.equals(keySchemaIdHeader(headersA), keySchemaIdHeader(headersB)));

    // (2) Ambiguous decode: the one shared byte[] decodes to a different logical key per header.
    Object decodedWithA = deserializer.deserialize(TOPIC_A, headersA, rowKeyA);
    Object decodedWithB = deserializer.deserialize(TOPIC_A, headersB, rowKeyA);
    assertEquals("header A reconstructs key A", keyA, decodedWithA);
    assertEquals("header B reconstructs key B", keyB, decodedWithB);
    assertNotEquals("same bytes, different header id -> different decoded key",
        decodedWithA, decodedWithB);

    // (3) Duplicate __key_schema_id headers: the last one silently wins.
    RecordHeaders duplicate = new RecordHeaders();
    duplicate.add(SchemaId.KEY_SCHEMA_ID_HEADER, keySchemaIdHeader(headersA));
    duplicate.add(SchemaId.KEY_SCHEMA_ID_HEADER, keySchemaIdHeader(headersB));
    assertEquals("the last __key_schema_id header (B) is the one used",
        keyB, deserializer.deserialize(TOPIC_A, duplicate, rowKeyA));
  }

  /**
   * A stored {@code __key_schema_id} need not describe the row key at all. Here the row key bytes
   * encode a string key, but the header carries the id of an unrelated numeric schema. Decoding
   * silently produces the WRONG record instead of failing (the string's leading length byte happens
   * to read as a valid {@code long}).
   *
   * <p>How this arises in practice (Processor API only): a custom processor stores under a key
   * derived from the value (e.g. a secondary index keyed by {@code CustomerKey} while the input
   * records are keyed by {@code OrderKey}), then reads keys back via a key-returning op
   * ({@code all}/{@code range}/{@code fetch}) or a punctuator. At read time
   * {@code deserializeKey(bytes)} uses the in-flight record's {@code __key_schema_id} (an
   * {@code OrderKey} id), or empty headers under a punctuator — neither describes the stored
   * {@code CustomerKey} bytes, so the key decodes under the wrong schema.
   *
   * <p>Scope — this does NOT occur in the DSL, where every store is keyed by {@code record.key()}
   * (so the in-flight id always matches the stored key's schema), and does NOT occur on the
   * versioned store, which never deserializes stored keys (its {@code get}/{@code put}/{@code delete}
   * and version queries all take the key as an input). It bites only plain key-value / windowed /
   * session stores that return keys via iteration. The versioned angle is a purely hypothetical
   * caution for a future header-aware versioned store that reconstructs keys from stored headers.
   * (In this serde-level test the unrelated id belongs to a numeric schema, and the string key's
   * leading length byte happens to read as a valid {@code long}, so decoding silently succeeds.)
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
   * A byte-affecting key schema evolution (adding a field with a default) changes the encoded key
   * bytes for the "same" logical key, so the versions self-separate into different rows rather than
   * sharing one. This is the inverse of the collapse case: one logical key &rarr; two byte[].
   *
   * <p>How this arises in practice: someone evolves the KEY schema by adding a field. During a
   * rolling upgrade, or with producers pinned to different schema versions, the same logical entity
   * ({@code id="row-1"}) is emitted under both v1 and v2 and encodes to different bytes. In a keyed
   * store or compacted topic the "same" entity then occupies two rows and {@code get} of one version
   * misses the other. Evolving key schemas is explicitly discouraged for exactly this reason, but it
   * is a common operational mistake. (Unlike the other tests, this one does not depend on where the
   * schema id is placed &mdash; the payload bytes themselves change.)
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
