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

package io.confluent.kafka.schemaregistry.storage.garbagecollection.serde;

import io.confluent.protobuf.events.catalog.v1.MetadataChange;
import io.confluent.protobuf.events.catalog.v1.MetadataEvent;
import io.confluent.protobuf.events.catalog.v1.OpType;
import io.confluent.protobuf.events.catalog.v1.TopicMetadata;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class MetadataChangeDeserializerTest {

  private static final String SOURCE_LKC = "lkc-1";
  private static final String TOPIC_1 = "topic-1";

  private MetadataChangeDeserializer deserializer;

  @Before
  public void setUp() {
    deserializer = new MetadataChangeDeserializer();
  }

  @Test
  public void testDeserialize_Protobuf() {
    MetadataChange.Builder builder = MetadataChange.newBuilder();
    builder.setSource(SOURCE_LKC);
    builder.setOp(OpType.DELETE);
    TopicMetadata topicMeta = TopicMetadata.newBuilder()
        .setTopicId(TOPIC_1)
        .setTopicName(TOPIC_1)
        .build();
    builder.addEvents(MetadataEvent.newBuilder().setTopicMetadata(topicMeta).build());
    MetadataChange expected = builder.build();

    byte[] protobufBytes = expected.toByteArray();
    MetadataChange result = deserializer.deserialize(protobufBytes, "protobuf");

    assertEquals(expected, result);
  }

  @Test
  public void testDeserialize_Json() {
    String json = "{\n"
        + "  \"source\" : \"" + SOURCE_LKC + "\",\n"
        + "  \"op\" : \"DELETE\",\n"
        + "  \"events\" : [ {\n"
        + "    \"topicMetadata\" : {\n"
        + "      \"topicId\" : \"" + TOPIC_1 + "\",\n"
        + "      \"topicName\" : \"" + TOPIC_1 + "\"\n"
        + "    }\n"
        + "  } ]\n"
        + "}";
    byte[] data = json.getBytes(StandardCharsets.UTF_8);

    MetadataChange result = deserializer.deserialize(data, "json");

    assertNotNull(result);
    assertEquals(SOURCE_LKC, result.getSource());
    assertEquals(OpType.DELETE, result.getOp());
    assertEquals(1, result.getEventsCount());
    assertEquals(TOPIC_1, result.getEvents(0).getTopicMetadata().getTopicId());
    assertEquals(TOPIC_1, result.getEvents(0).getTopicMetadata().getTopicName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeserialize_UnsupportedFormat() {
    byte[] data = "{}".getBytes(StandardCharsets.UTF_8);
    deserializer.deserialize(data, "xml");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeserialize_InvalidData() {
    byte[] invalidProtobuf = new byte[]{0x00, (byte) 0xFF, 0x12, 0x34};
    deserializer.deserialize(invalidProtobuf, "protobuf");
  }
}
