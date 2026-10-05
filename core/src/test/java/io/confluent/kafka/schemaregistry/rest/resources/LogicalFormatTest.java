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

package io.confluent.kafka.schemaregistry.rest.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code format=logical} conversion helper. No live cluster is needed: both
 * directions are pure conversions over a mocked {@link SchemaRegistry}.
 */
class LogicalFormatTest {

  private static final String STRUCT_DDL = "STRUCT Widget (id INT NOT NULL, name VARCHAR NOT NULL)";

  // -- isLogical --------------------------------------------------------------------------------

  @Test
  void isLogicalIsCaseInsensitive() {
    assertTrue(LogicalFormat.isLogical("logical"));
    assertTrue(LogicalFormat.isLogical("LOGICAL"));
    assertTrue(LogicalFormat.isLogical("Logical"));
  }

  @Test
  void isLogicalRejectsEverythingElse() {
    assertEquals(false, LogicalFormat.isLogical("resolved"));
    assertEquals(false, LogicalFormat.isLogical(""));
    assertEquals(false, LogicalFormat.isLogical(null));
  }

  // -- convertToLogicalDdl: happy path, one per schemaType -------------------------------------

  @Test
  void convertToLogicalDdlFromAvro() throws Exception {
    String avroSchemaString =
        "{\"type\":\"record\",\"name\":\"Row\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"},"
            + "{\"name\":\"name\",\"type\":\"string\"}]}";
    Schema schema = schemaEntityFor("AVRO", avroSchemaString);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.parseSchema(schema, false, false))
        .thenReturn(new AvroSchema(avroSchemaString));

    String ddl = LogicalFormat.convertToLogical(schemaRegistry, schema);

    assertTrue(ddl.contains("id"));
    assertTrue(ddl.contains("name"));
  }

  @Test
  void convertToLogicalDdlFromJson() throws Exception {
    String jsonSchemaString =
        "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
            + "\"type\":\"object\",\"properties\":{"
            + "\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"}},"
            + "\"required\":[\"id\",\"name\"]}";
    Schema schema = schemaEntityFor("JSON", jsonSchemaString);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.parseSchema(schema, false, false))
        .thenReturn(new JsonSchema(jsonSchemaString));

    String ddl = LogicalFormat.convertToLogical(schemaRegistry, schema);

    assertTrue(ddl.contains("id"));
    assertTrue(ddl.contains("name"));
  }

  @Test
  void convertToLogicalDdlFromProtobuf() throws Exception {
    String protoSchemaString =
        "syntax = \"proto3\";\n"
            + "message Widget {\n"
            + "  int32 id = 1;\n"
            + "  string name = 2;\n"
            + "}\n";
    Schema schema = schemaEntityFor("PROTOBUF", protoSchemaString);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.parseSchema(schema, false, false))
        .thenReturn(new ProtobufSchema(protoSchemaString));

    String ddl = LogicalFormat.convertToLogical(schemaRegistry, schema);

    assertTrue(ddl.contains("id"));
    assertTrue(ddl.contains("name"));
  }

  @Test
  void convertToLogicalDdlCollapsesASingletonOneofToItsMemberType() throws Exception {
    // format=logical must keep the canonical V2 reading even though LogicalPolicyChecker.check()
    // pins V1 for compatibility (see LogicalPolicyCheckerTest -- V1 keeps a singleton oneOf as a
    // UNION). A caller reading this schema's logical-type representation should see the same
    // scalar column a real V2 reader would, not a synthesized single-branch union.
    String jsonSchemaString =
        "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
            + "\"type\":\"object\",\"properties\":{"
            + "\"u\":{\"oneOf\":[{\"type\":\"integer\"}]}}}";
    Schema schema = schemaEntityFor("JSON", jsonSchemaString);
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.parseSchema(schema, false, false))
        .thenReturn(new JsonSchema(jsonSchemaString));

    String ddl = LogicalFormat.convertToLogical(schemaRegistry, schema);

    assertTrue(ddl.contains("u"));
    assertFalse(ddl.contains("UNION"), ddl);
  }

  @Test
  void convertToLogicalDdlRejectsUnsupportedSchemaType() throws Exception {
    Schema schema = schemaEntityFor("XML", "<not-a-real-schema/>");
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.parseSchema(any(Schema.class), anyBoolean(), anyBoolean()))
        .thenThrow(new InvalidSchemaException("unsupported"));

    assertThrows(InvalidSchemaException.class, () ->
        LogicalFormat.convertToLogical(schemaRegistry, schema));
  }

  // -- helpers ------------------------------------------------------------------------------

  private static Schema schemaEntityFor(String schemaType, String schemaString) {
    return new Schema(
        "widgets-value", 1, 1, null, schemaType, Collections.emptyList(), null, null,
        schemaString);
  }
}
