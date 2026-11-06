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

package io.confluent.kafka.serializers.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * One deserializer reading the same Protobuf body in two contexts, whose imports differ.
 */
public class KafkaProtobufDeserializerContextTest {

  public static class TopicContextNameStrategy implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return topic;
    }
  }

  // Each record decodes with its own context's import, even once the same body was read in the
  // other context.
  @Test
  public void testSameBodyInTwoContextsDecodesWithEachContextsDependency() throws Exception {
    MockSchemaRegistryClient client =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    String depA = "syntax = \"proto3\";\npackage dep;\nmessage Y { int32 f = 1; }\n";
    String depB = "syntax = \"proto3\";\npackage dep;\nmessage Y { string g = 1; }\n";
    String body = "syntax = \"proto3\";\npackage top;\nimport \"dep.proto\";\n"
        + "message X { dep.Y a = 1; }\n";
    List<SchemaReference> refs =
        Collections.singletonList(new SchemaReference("dep.proto", "dep", 1));
    client.register(":.a:dep", new ProtobufSchema(depA));
    client.register(":.b:dep", new ProtobufSchema(depB));
    ProtobufSchema xa = new ProtobufSchema(body, refs,
        Collections.singletonMap("dep.proto", depA), null, null);
    ProtobufSchema xb = new ProtobufSchema(body, refs,
        Collections.singletonMap("dep.proto", depB), null, null);
    int idA = client.register(":.a:a-value", xa);
    int idB = client.register(":.b:b-value", xb);

    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config.put(AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        TopicContextNameStrategy.class.getName());
    config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
    config.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
    KafkaProtobufDeserializer<DynamicMessage> deserializer =
        new KafkaProtobufDeserializer<>(client, config);

    byte[] bytesA = wire(idA, message(xa, "f", 5));
    byte[] bytesB = wire(idB, message(xb, "g", "hello"));
    DynamicMessage readA = deserializer.deserialize("a", bytesA);
    DynamicMessage readB = deserializer.deserialize("b", bytesB);
    assertEquals(5, field(readA, "f"));
    assertEquals("hello", field(readB, "g"));
  }

  // Records read with their schema's version share one copy of it, so its descriptors are built
  // once rather than for every record.
  @Test
  public void testRecordsReadWithTheirVersionShareOneSchemaCopy() throws Exception {
    MockSchemaRegistryClient client =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    ProtobufSchema x = new ProtobufSchema("syntax = \"proto3\";\nmessage X { int32 f = 1; }\n");
    int id = client.register("t-value", x);
    KafkaProtobufDeserializer<DynamicMessage> deserializer = new KafkaProtobufDeserializer<>(
        client, Collections.singletonMap(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus"));
    Descriptor descriptor = x.toDescriptor();
    byte[] bytes = wire(id, DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName("f"), 1).build());

    ProtobufSchemaAndValue first =
        deserializer.deserializeWithSchemaAndVersion("t", false, null, bytes);
    ProtobufSchemaAndValue second =
        deserializer.deserializeWithSchemaAndVersion("t", false, null, bytes);
    assertEquals(Integer.valueOf(1), first.getSchema().version());
    assertSame(first.getSchema(), second.getSchema());
  }

  private static DynamicMessage message(ProtobufSchema schema, String name, Object value) {
    Descriptor x = schema.toDescriptor();
    FieldDescriptor a = x.findFieldByName("a");
    Descriptor y = a.getMessageType();
    DynamicMessage inner = DynamicMessage.newBuilder(y)
        .setField(y.findFieldByName(name), value).build();
    return DynamicMessage.newBuilder(x).setField(a, inner).build();
  }

  // The wire format: magic byte, schema id, message index 0, then the message.
  private static byte[] wire(int id, DynamicMessage m) {
    byte[] payload = m.toByteArray();
    ByteBuffer b = ByteBuffer.allocate(6 + payload.length);
    b.put((byte) 0).putInt(id).put((byte) 0).put(payload);
    return b.array();
  }

  private static Object field(DynamicMessage x, String name) {
    DynamicMessage inner =
        (DynamicMessage) x.getField(x.getDescriptorForType().findFieldByName("a"));
    FieldDescriptor fd = inner.getDescriptorForType().findFieldByName(name);
    return fd == null ? "no field " + name : inner.getField(fd);
  }
}
