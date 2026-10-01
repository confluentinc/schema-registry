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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/**
 * Confirms the {@code format=logical} wiring inside {@link SubjectVersionsResource} itself --
 * that {@code getSchemaByVersion} dispatches to {@link LogicalFormat} rather than the
 * ordinary {@code formattedString} path, and only when asked to. {@link LogicalFormatTest}
 * covers the conversion logic itself; this covers the call site.
 */
class SubjectVersionsResourceLogicalFormatTest {

  private static final String AVRO_SCHEMA_STRING =
      "{\"type\":\"record\",\"name\":\"Widget\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"},"
          + "{\"name\":\"name\",\"type\":\"string\"}]}";

  @Test
  void getSchemaByVersionConvertsToLogicalDdlWhenRequested() throws Exception {
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.tenant()).thenReturn(QualifiedSubject.DEFAULT_TENANT);
    Schema schema = schemaEntity();
    when(schemaRegistry.getUsingContexts("widgets-value", 1, false)).thenReturn(schema);
    when(schemaRegistry.parseSchema(schema, false, false))
        .thenReturn(new AvroSchema(AVRO_SCHEMA_STRING));

    SubjectVersionsResource resource = new SubjectVersionsResource(schemaRegistry);
    Schema result = resource.getSchemaByVersion(
        "widgets-value", "1", "logical", "", false, null);

    assertTrue(result.getSchema().contains("id"));
    assertTrue(result.getSchema().contains("name"));
    // Not the raw Avro string any more -- it went through the DDL converter.
    assertTrue(!result.getSchema().contains("\"type\":\"record\""));
  }

  @Test
  void getSchemaByVersionLeavesSchemaAloneWithNoFormat() throws Exception {
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.tenant()).thenReturn(QualifiedSubject.DEFAULT_TENANT);
    Schema schema = schemaEntity();
    when(schemaRegistry.getUsingContexts("widgets-value", 1, false)).thenReturn(schema);

    SubjectVersionsResource resource = new SubjectVersionsResource(schemaRegistry);
    Schema result = resource.getSchemaByVersion(
        "widgets-value", "1", "", "", false, null);

    assertEquals(AVRO_SCHEMA_STRING, result.getSchema());
    // format=logical's conversion path must not run at all when format is empty.
    verify(schemaRegistry, never()).parseSchema(eq(schema), anyBoolean(), anyBoolean());
  }

  @Test
  void getSchemaByVersionStillHonorsExistingFormatValues() throws Exception {
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.tenant()).thenReturn(QualifiedSubject.DEFAULT_TENANT);
    Schema schema = schemaEntity();
    when(schemaRegistry.getUsingContexts("widgets-value", 1, false)).thenReturn(schema);
    ParsedSchema parsedSchema = new AvroSchema(AVRO_SCHEMA_STRING);
    when(schemaRegistry.parseSchema(schema, false, false)).thenReturn(parsedSchema);

    SubjectVersionsResource resource = new SubjectVersionsResource(schemaRegistry);
    Schema result = resource.getSchemaByVersion(
        "widgets-value", "1", "resolved", "", false, null);

    // Existing format values still go through the ordinary formattedString path, unaffected by
    // the new branch -- "resolved" isn't a recognized Avro format value, so it falls back to
    // canonicalString(), same as before this change.
    assertEquals(parsedSchema.canonicalString(), result.getSchema());
    verify(schemaRegistry, times(1)).parseSchema(schema, false, false);
  }

  private static Schema schemaEntity() {
    return new Schema(
        "widgets-value", 1, 1, null, "AVRO", Collections.emptyList(), null, null,
        AVRO_SCHEMA_STRING);
  }
}
