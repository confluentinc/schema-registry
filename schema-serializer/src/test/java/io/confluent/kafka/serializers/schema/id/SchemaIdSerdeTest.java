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

package io.confluent.kafka.serializers.schema.id;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.confluent.kafka.serializers.schema.id.SchemaId.SchemaMessageIndexes;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.Test;

public class SchemaIdSerdeTest {

  @Test
  public void testHeaderSchemaIdSerializerSkipsDuplicateHeader() {
    HeaderSchemaIdSerializer serializer = new HeaderSchemaIdSerializer();
    Headers headers = new RecordHeaders();
    byte[] payload = new byte[]{1, 2, 3};

    UUID guid = UUID.randomUUID();
    SchemaId schemaId = new SchemaId("AVRO", null, guid);

    // First serialization should add the header
    byte[] result1 = serializer.serialize("topic", false, headers, payload, schemaId);
    assertArrayEquals(payload, result1);
    assertEquals(1, countHeaders(headers, SchemaId.VALUE_SCHEMA_ID_HEADER));

    // Second serialization with the same schemaId should not add a duplicate header
    byte[] result2 = serializer.serialize("topic", false, headers, payload, schemaId);
    assertArrayEquals(payload, result2);
    assertEquals(1, countHeaders(headers, SchemaId.VALUE_SCHEMA_ID_HEADER));
  }

  @Test
  public void testHeaderSchemaIdSerializerAddsDifferentHeader() {
    HeaderSchemaIdSerializer serializer = new HeaderSchemaIdSerializer();
    Headers headers = new RecordHeaders();
    byte[] payload = new byte[]{1, 2, 3};

    UUID guid1 = UUID.randomUUID();
    UUID guid2 = UUID.randomUUID();
    SchemaId schemaId1 = new SchemaId("AVRO", null, guid1);
    SchemaId schemaId2 = new SchemaId("AVRO", null, guid2);

    // First serialization should add the header
    serializer.serialize("topic", false, headers, payload, schemaId1);
    assertEquals(1, countHeaders(headers, SchemaId.VALUE_SCHEMA_ID_HEADER));

    // Second serialization with a different schemaId should add another header
    serializer.serialize("topic", false, headers, payload, schemaId2);
    assertEquals(2, countHeaders(headers, SchemaId.VALUE_SCHEMA_ID_HEADER));
  }

  @Test
  public void testHeaderSchemaIdSerializerKeyAndValueHeaders() {
    HeaderSchemaIdSerializer serializer = new HeaderSchemaIdSerializer();
    Headers headers = new RecordHeaders();
    byte[] payload = new byte[]{1, 2, 3};

    UUID guid = UUID.randomUUID();
    SchemaId schemaId = new SchemaId("AVRO", null, guid);

    // Serialize as key
    serializer.serialize("topic", true, headers, payload, schemaId);
    assertEquals(1, countHeaders(headers, SchemaId.KEY_SCHEMA_ID_HEADER));
    assertEquals(0, countHeaders(headers, SchemaId.VALUE_SCHEMA_ID_HEADER));

    // Serialize as value
    serializer.serialize("topic", false, headers, payload, schemaId);
    assertEquals(1, countHeaders(headers, SchemaId.KEY_SCHEMA_ID_HEADER));
    assertEquals(1, countHeaders(headers, SchemaId.VALUE_SCHEMA_ID_HEADER));

    // Serialize same key again - should not add duplicate
    serializer.serialize("topic", true, headers, payload, schemaId);
    assertEquals(1, countHeaders(headers, SchemaId.KEY_SCHEMA_ID_HEADER));
  }

  @Test
  public void testMessageIndexesRoundTrip() {
    List<Integer> indexes = Arrays.asList(1, 0, 3);
    byte[] bytes = new SchemaMessageIndexes(indexes).toByteArray();
    assertEquals(indexes, SchemaMessageIndexes.readFrom(bytes).indexes());
    assertEquals(Arrays.asList(0), SchemaMessageIndexes.readFrom(new byte[]{0}).indexes());
  }

  @Test(expected = SerializationException.class)
  public void testMessageIndexesCountExceedsRemaining() {
    SchemaMessageIndexes.readFrom(varints(100, 1, 2));
  }

  @Test(expected = SerializationException.class)
  public void testMessageIndexesNegativeCount() {
    SchemaMessageIndexes.readFrom(varints(-1, 1));
  }

  private static byte[] varints(int... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 5);
    for (int value : values) {
      ByteUtils.writeVarint(value, buffer);
    }
    return Arrays.copyOf(buffer.array(), buffer.position());
  }

  private int countHeaders(Headers headers, String key) {
    int count = 0;
    for (org.apache.kafka.common.header.Header header : headers) {
      if (header.key().equals(key)) {
        count++;
      }
    }
    return count;
  }
}
