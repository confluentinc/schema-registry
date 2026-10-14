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

package io.confluent.kafka.serializers.protobuf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.UnknownFieldSet;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/**
 * A kept oneof member the reader's own parse could split, under a message the writer names
 * otherwise: the writer may declare the moved member there.
 */
class ProtoProvenanceSplitTest {

  @Test
  void aKeptMemberSplitInsideARenamedNestedMessageIsMerged() throws Exception {
    // R.X renamed R.Y, its fields kept: the writer's X declares u, re-added in Y with a new id,
    // so a record k {a: 1} u: 9 | k {b: 2} must merge k as the projected parse does.
    String in = "\nmessage In { int32 a = 1; int32 b = 2; }\n";
    ProtobufSchema v1 = new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage R {\n"
        + "  int32 id = 1;\n  message X { oneof o { In k = 2; int32 u = 4; } }\n  X x = 2;\n}" + in);
    ProtobufSchema v2 = new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage R {\n"
        + "  int32 id = 1;\n  message X { oneof o { In k = 2; } }\n  X x = 2;\n}" + in);
    ProtobufSchema v3 = new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage R {\n"
        + "  int32 id = 1;\n  message Y { oneof o { In k = 2; int32 u = 4; } }\n  Y x = 2;\n}" + in);
    SchemaProvenance provenance = ProvenanceHistory.compute("s", Arrays.asList(
        new SchemaMetadata(1, 1, "PROTOBUF", Collections.emptyList(), ""),
        new SchemaMetadata(2, 2, "PROTOBUF", Collections.emptyList(), ""),
        new SchemaMetadata(3, 3, "PROTOBUF", Collections.emptyList(), "")),
        ProvenanceHistory.held(Arrays.<ParsedSchema>asList(v1, v2, v3)), false);
    ProtoProvenanceProjection projection = ProtoProvenanceProjection.of(v3, v1,
        ProvenanceMapping.join(provenance, 1, 3), false);
    byte[] x = cat(bytes(2, varint(1, 1)), varint(4, 9), bytes(2, varint(2, 2)));
    byte[] record = bytes(2, x);
    Message read = AbstractKafkaProtobufDeserializer.parseProjected(projection, v3,
        ByteBuffer.wrap(record), 0, record.length);
    assertEquals("x { k { a: 1 b: 2 } }", TextFormat.shortDebugString(read));
  }

  private static byte[] varint(int number, long value) {
    return UnknownFieldSet.newBuilder().addField(number,
        UnknownFieldSet.Field.newBuilder().addVarint(value).build()).build().toByteArray();
  }

  private static byte[] bytes(int number, byte[] value) {
    return UnknownFieldSet.newBuilder().addField(number, UnknownFieldSet.Field.newBuilder()
        .addLengthDelimited(ByteString.copyFrom(value)).build()).build().toByteArray();
  }

  private static byte[] cat(byte[]... parts) {
    ByteString all = ByteString.EMPTY;
    for (byte[] part : parts) {
      all = all.concat(ByteString.copyFrom(part));
    }
    return all.toByteArray();
  }
}
