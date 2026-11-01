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

package io.confluent.kafka.schemaregistry.type.logical.protobuf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.cel.CelValidator;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesParserFactory;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesSchemaVisitor;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * CHECKs written as Protobuf and run on real messages. A proto3 scalar has no presence, so its
 * has() is false for a zero; a NOT NULL column holding one must not read as NULL.
 */
class ProtoCheckPresenceTest {

  @Test
  void aNotNullZeroIsNotNull() {
    String ddl = "STRUCT T (a INT NOT NULL, s STRING NOT NULL, t BOOLEAN NOT NULL, "
        + "r STRUCT(x INT NOT NULL), items STRUCT(x INT NOT NULL) NOT NULL ARRAY NOT NULL, "
        + "CONSTRAINT c7 CHECK (COALESCE((r).x, 7) = 0), "
        + "CONSTRAINT c8 CHECK (EVERY(items, e, COALESCE(e.x, 7) = 0)), "
        + "CONSTRAINT c1 CHECK (COALESCE(a, 7) = 0), "
        + "CONSTRAINT c2 CHECK (GREATEST(a, -3) = 0), "
        + "CONSTRAINT c3 CHECK (COALESCE(s, 'x') = ''), "
        + "CONSTRAINT c4 CHECK (COALESCE(t, TRUE) = FALSE), "
        + "CONSTRAINT c5 CHECK (LEAST(r.x, 1) = 0), "
        + "CONSTRAINT c6 CHECK (r.x IS NOT NULL));";
    LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
    visitor.visit(LogicalTypesParserFactory.parse(ddl));
    ProtobufSchema schema = LogicalTypeToProtoConverter.fromLogicalType(
        visitor.toLogicalType(), "T");
    // Every scalar left at its zero value, r present with x = 0, and one item with x = 0.
    Descriptor descriptor = schema.toDescriptor();
    FieldDescriptor r = descriptor.findFieldByName("r");
    FieldDescriptor items = descriptor.findFieldByName("items");
    DynamicMessage message = DynamicMessage.newBuilder(descriptor)
        .setField(r, DynamicMessage.newBuilder(r.getMessageType()).build())
        .addRepeatedField(items, DynamicMessage.newBuilder(items.getMessageType()).build())
        .build();
    assertEquals(List.of(), schema.validateMessage(new CelValidator(), message));
  }

  @Test
  void anUnsetCaseResultIsNull() {
    // An unset nullable field reads as its zero; a CASE gives it as NULL, as has() says.
    String ddl = "STRUCT T (t BOOLEAN NOT NULL, ni INT, "
        + "CONSTRAINT c1 CHECK (CASE WHEN t THEN ni ELSE ni END IS NULL), "
        + "CONSTRAINT c2 CHECK (CASE WHEN t THEN 1.5 ELSE ni END IS NULL));";
    LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
    visitor.visit(LogicalTypesParserFactory.parse(ddl));
    ProtobufSchema schema = LogicalTypeToProtoConverter.fromLogicalType(
        visitor.toLogicalType(), "T");
    DynamicMessage message = DynamicMessage.newBuilder(schema.toDescriptor()).build();
    assertEquals(List.of(), schema.validateMessage(new CelValidator(), message));
  }
}
