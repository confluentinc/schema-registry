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

package io.confluent.kafka.schemaregistry.avro;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

public class NameAwareGenericDataTest {

  @Test
  public void sameSchemaInstance_preservesValuesWithPositionalGet() throws Exception {
    Schema schema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"b\",\"type\":\"int\"},"
        + "{\"name\":\"c\",\"type\":\"int\"}]}");
    CountingRecord record = new CountingRecord(schema);
    record.put("a", 1);
    record.put("b", 2);
    record.put("c", 3);
    record.resetCounts();

    GenericRecord result = roundTrip(schema, record);

    assertEquals(Integer.valueOf(1), result.get("a"));
    assertEquals(Integer.valueOf(2), result.get("b"));
    assertEquals(Integer.valueOf(3), result.get("c"));
    assertEquals(3, record.indexGets);
    assertEquals(0, record.nameGets);
  }

  @Test
  public void reorderedFields_preservesValuesWithPositionalGet() throws Exception {
    Schema writerSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"b\",\"type\":\"int\"},"
        + "{\"name\":\"c\",\"type\":\"int\"}]}");
    Schema recordSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"b\",\"type\":\"int\"},"
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"c\",\"type\":\"int\"}]}");
    CountingRecord record = new CountingRecord(recordSchema);
    record.put("b", 2);
    record.put("a", 1);
    record.put("c", 3);
    record.resetCounts();

    GenericRecord first = roundTrip(writerSchema, record);
    assertEquals(Integer.valueOf(1), first.get("a"));
    assertEquals(Integer.valueOf(2), first.get("b"));
    assertEquals(Integer.valueOf(3), first.get("c"));
    assertEquals(3, record.indexGets);
    assertEquals(0, record.nameGets);

    record.resetCounts();
    GenericRecord second = roundTrip(writerSchema, record);
    assertEquals(Integer.valueOf(1), second.get("a"));
    assertEquals(Integer.valueOf(2), second.get("b"));
    assertEquals(Integer.valueOf(3), second.get("c"));
    assertEquals(3, record.indexGets);
    assertEquals(0, record.nameGets);
  }

  @Test
  public void nestedReorderedFields_roundTripsByName() throws Exception {
    Schema writerSchema = parse("{\"type\":\"record\",\"name\":\"Outer\",\"fields\":[{"
        + "\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"b\",\"type\":\"int\"},"
        + "{\"name\":\"c\",\"type\":\"int\"}]}}]}");
    Schema recordSchema = parse("{\"type\":\"record\",\"name\":\"Outer\",\"fields\":[{"
        + "\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
        + "{\"name\":\"b\",\"type\":\"int\"},"
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"c\",\"type\":\"int\"}]}}]}");
    GenericRecord inner = new GenericData.Record(recordSchema.getField("inner").schema());
    inner.put("b", 2);
    inner.put("a", 1);
    inner.put("c", 3);
    GenericRecord outer = new GenericData.Record(recordSchema);
    outer.put("inner", inner);

    GenericRecord result = (GenericRecord) roundTrip(writerSchema, outer).get("inner");

    assertEquals(Integer.valueOf(1), result.get("a"));
    assertEquals(Integer.valueOf(2), result.get("b"));
    assertEquals(Integer.valueOf(3), result.get("c"));
  }

  @Test
  public void aliasAtDifferentIndex_followsAlias() throws Exception {
    Schema recordSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"b\",\"type\":\"int\"}]}");
    Schema writerSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"b_new\",\"type\":\"int\",\"aliases\":[\"b\"]},"
        + "{\"name\":\"a\",\"type\":\"int\"}]}");
    CountingRecord record = new CountingRecord(recordSchema);
    record.put("a", 1);
    record.put("b", 2);
    record.resetCounts();

    GenericRecord result = roundTrip(writerSchema, record);

    assertEquals(Integer.valueOf(2), result.get("b_new"));
    assertEquals(Integer.valueOf(1), result.get("a"));
    assertEquals(2, record.indexGets);
    assertEquals(0, record.nameGets);
  }

  @Test
  public void directGetAndSet_alignedUsesPositionReorderedUsesName() {
    GenericData data = AvroSchemaUtils.getGenericData();
    Schema alignedSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"b\",\"type\":\"int\"},"
        + "{\"name\":\"c\",\"type\":\"int\"}]}");
    CountingRecord aligned = new CountingRecord(alignedSchema);
    aligned.put("a", 1);
    aligned.put("b", 2);
    aligned.put("c", 3);
    aligned.resetCounts();

    assertEquals(Integer.valueOf(1), data.getField(aligned, "a", 0));
    assertEquals(1, aligned.indexGets);
    assertEquals(0, aligned.nameGets);
    data.setField(aligned, "b", 1, 8);
    assertEquals(1, aligned.indexPuts);
    assertEquals(0, aligned.namePuts);
    assertEquals(Integer.valueOf(8), aligned.get(1));

    Schema reorderedSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"b\",\"type\":\"int\"},"
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"c\",\"type\":\"int\"}]}");
    CountingRecord reordered = new CountingRecord(reorderedSchema);
    reordered.put("b", 2);
    reordered.put("a", 1);
    reordered.put("c", 3);
    reordered.resetCounts();

    assertEquals(Integer.valueOf(1), data.getField(reordered, "a", 0));
    assertEquals(0, reordered.indexGets);
    assertEquals(1, reordered.nameGets);
    data.setField(reordered, "a", 0, 9);
    assertEquals(0, reordered.indexPuts);
    assertEquals(1, reordered.namePuts);
    assertEquals(Integer.valueOf(9), reordered.get("a"));
    assertEquals(Integer.valueOf(2), reordered.get("b"));
  }

  @Test
  public void missingWriterField_usesPositionalFallback() throws Exception {
    Schema recordSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"b\",\"type\":\"int\"}]}");
    Schema writerSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"missing\",\"type\":\"int\"}]}");
    CountingRecord record = new CountingRecord(recordSchema);
    record.put("a", 1);
    record.put("b", 2);
    record.resetCounts();

    GenericRecord result = roundTrip(writerSchema, record);

    assertEquals(Integer.valueOf(1), result.get("a"));
    assertEquals(Integer.valueOf(2), result.get("missing"));
    assertEquals(2, record.indexGets);
    assertEquals(0, record.nameGets);

    GenericData data = AvroSchemaUtils.getGenericData();
    record.resetCounts();
    assertEquals(Integer.valueOf(1), data.getField(record, "missing", 0));
    assertEquals(1, record.indexGets);
    assertEquals(0, record.nameGets);
    data.setField(record, "missing", 1, 8);
    assertEquals(1, record.indexPuts);
    assertEquals(0, record.namePuts);
    assertEquals(Integer.valueOf(8), record.get(1));
    assertEquals(Integer.valueOf(1), record.get(0));
  }

  @Test
  public void recordFieldAliasIsNotFollowed() throws Exception {
    // Avro applies reader aliases to writer names, not the other way around.
    Schema recordSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"b_alias\"]}]}");
    Schema writerSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"b_alias\",\"type\":\"int\"},"
        + "{\"name\":\"a\",\"type\":\"int\"}]}");
    CountingRecord record = new CountingRecord(recordSchema);
    record.put("a", 1);
    record.put("b", 2);

    GenericRecord result = roundTrip(writerSchema, record);

    // "a" matches by name; "b_alias" has no match and falls back to position 0.
    assertEquals(Integer.valueOf(1), result.get("b_alias"));
    assertEquals(Integer.valueOf(1), result.get("a"));
  }

  @Test
  public void canonicalNameWinsOverWriterFieldAlias() throws Exception {
    Schema recordSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},"
        + "{\"name\":\"b\",\"type\":\"int\"}]}");
    Schema writerSchema = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\"]},"
        + "{\"name\":\"a\",\"type\":\"int\"}]}");
    CountingRecord record = new CountingRecord(recordSchema);
    record.put("a", 1);
    record.put("b", 2);

    GenericRecord result = roundTrip(writerSchema, record);

    assertEquals(Integer.valueOf(2), result.get("b"));
    assertEquals(Integer.valueOf(1), result.get("a"));
  }

  private static GenericRecord roundTrip(Schema writerSchema, GenericRecord record)
      throws IOException {
    GenericData data = AvroSchemaUtils.getGenericData();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(writerSchema, data).write(record, encoder);
    encoder.flush();
    return new GenericDatumReader<GenericRecord>(writerSchema, writerSchema, data)
        .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
  }

  private static Schema parse(String json) {
    return new Schema.Parser().parse(json);
  }

  private static final class CountingRecord extends GenericData.Record {
    private int indexGets;
    private int nameGets;
    private int indexPuts;
    private int namePuts;

    private CountingRecord(Schema schema) {
      super(schema);
    }

    private void resetCounts() {
      indexGets = 0;
      nameGets = 0;
      indexPuts = 0;
      namePuts = 0;
    }

    @Override
    public Object get(int i) {
      indexGets++;
      return super.get(i);
    }

    @Override
    public Object get(String key) {
      nameGets++;
      return super.get(key);
    }

    @Override
    public void put(int i, Object v) {
      indexPuts++;
      super.put(i, v);
    }

    @Override
    public void put(String key, Object value) {
      namePuts++;
      super.put(key, value);
    }
  }
}
