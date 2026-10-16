/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.streams.serde.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class KafkaProtobufSerdeTest {

  private static final String ANY_TOPIC = "any-topic";

  private static final String recordSchemaString = "syntax = \"proto3\";\n"
      + "\n"
      + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
      + "option java_outer_classname = \"TestMessageProtos\";\n"
      + "\n"
      + "import \"google/protobuf/descriptor.proto\";\n"
      + "\n"
      + "message TestMessage {\n"
      + "    string test_string = 1 [json_name = \"test_str\"];\n"
      + "    bool test_bool = 2;\n"
      + "    bytes test_bytes = 3;\n"
      + "    double test_double = 4;\n"
      + "    float test_float = 5;\n"
      + "    fixed32 test_fixed32 = 6;\n"
      + "    fixed64 test_fixed64 = 7;\n"
      + "    int32 test_int32 = 8;\n"
      + "    int64 test_int64 = 9;\n"
      + "    sfixed32 test_sfixed32 = 10;\n"
      + "    sfixed64 test_sfixed64 = 11;\n"
      + "    sint32 test_sint32 = 12;\n"
      + "    sint64 test_sint64 = 13;\n"
      + "    uint32 test_uint32 = 14;\n"
      + "    uint64 test_uint64 = 15;\n"
      + "}\n";

  private static final ProtobufSchema recordSchema = new ProtobufSchema(recordSchemaString);

  private DynamicMessage createDynamicMessage() {
    DynamicMessage.Builder builder = recordSchema.newMessageBuilder();
    Descriptors.Descriptor desc = builder.getDescriptorForType();
    Descriptors.FieldDescriptor fd = desc.findFieldByName("test_string");
    builder.setField(fd, "string");
    fd = desc.findFieldByName("test_bool");
    builder.setField(fd, true);
    fd = desc.findFieldByName("test_bytes");
    builder.setField(fd, ByteString.copyFromUtf8("hello"));
    fd = desc.findFieldByName("test_double");
    builder.setField(fd, 800.25);
    fd = desc.findFieldByName("test_float");
    builder.setField(fd, 23.4f);
    fd = desc.findFieldByName("test_fixed32");
    builder.setField(fd, 32);
    fd = desc.findFieldByName("test_fixed64");
    builder.setField(fd, 64L);
    fd = desc.findFieldByName("test_int32");
    builder.setField(fd, 32);
    fd = desc.findFieldByName("test_int64");
    builder.setField(fd, 64L);
    fd = desc.findFieldByName("test_sfixed32");
    builder.setField(fd, 32);
    fd = desc.findFieldByName("test_sfixed64");
    builder.setField(fd, 64L);
    fd = desc.findFieldByName("test_sint32");
    builder.setField(fd, 32);
    fd = desc.findFieldByName("test_sint64");
    builder.setField(fd, 64L);
    fd = desc.findFieldByName("test_uint32");
    builder.setField(fd, 32);
    fd = desc.findFieldByName("test_uint64");
    builder.setField(fd, 64L);
    return builder.build();
  }

  private static KafkaProtobufSerde<Message> createConfiguredSerdeForRecordValues() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(
        ImmutableList.of(new ProtobufSchemaProvider()));
    KafkaProtobufSerde<Message> serde = new KafkaProtobufSerde<>(schemaRegistryClient);
    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake");
    serde.configure(serdeConfig, false);
    return serde;
  }

  @Test
  public void shouldRoundTripRecords() {
    // Given
    KafkaProtobufSerde<Message> serde = createConfiguredSerdeForRecordValues();
    DynamicMessage record = createDynamicMessage();

    // When
    Message roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC, serde.serializer().serialize(ANY_TOPIC, record));

    // Then
    assertThat(roundtrippedRecord, equalTo(record));

    // Cleanup
    serde.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenInstantiatedWithNullSchemaRegistryClient() {
    new KafkaProtobufSerde<>((SchemaRegistryClient) null);
  }
}