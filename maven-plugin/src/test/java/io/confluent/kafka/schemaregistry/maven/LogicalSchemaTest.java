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

package io.confluent.kafka.schemaregistry.maven;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import org.junit.Test;

public class LogicalSchemaTest {

  private static final String SUBJECT = "TestSubject-value";
  private static final String DDL = "TYPE STRUCT<name STRING, age INT>";

  @Test
  public void recognizesDdl() {
    assertTrue(LogicalSchema.isLogical(DDL));
    assertTrue(LogicalSchema.isLogical("STRUCT User (name STRING, age INT); TYPE User"));
  }

  @Test
  public void rejectsNonDdl() {
    assertFalse(LogicalSchema.isLogical("["));
    assertFalse(LogicalSchema.isLogical("{\"type\": \"string\"}"));
  }

  @Test
  public void rejectsEmptyDdl() {
    assertFalse(LogicalSchema.isLogical(""));
    assertFalse(LogicalSchema.isLogical("   \n "));
    assertFalse(LogicalSchema.isLogical(null));
  }

  @Test
  public void validatesAsAvro() {
    LogicalSchema.validate(SUBJECT, AvroSchema.TYPE, DDL, false);
  }

  @Test
  public void validatesAsJson() {
    LogicalSchema.validate(SUBJECT, JsonSchema.TYPE, DDL, false);
  }

  @Test
  public void validatesAsProtobuf() {
    LogicalSchema.validate(SUBJECT, ProtobufSchema.TYPE, DDL, false);
  }

  @Test
  public void validatesNamedTypes() {
    LogicalSchema.validate(
        SUBJECT, AvroSchema.TYPE, "STRUCT User (name STRING, age INT); TYPE User", false);
  }

  @Test(expected = ValidationException.class)
  public void validateRejectsUnsupportedSchemaType() {
    LogicalSchema.validate(SUBJECT, "FOO", DDL, false);
  }

  @Test(expected = ValidationException.class)
  public void validateRejectsMalformedDdl() {
    LogicalSchema.validate(SUBJECT, AvroSchema.TYPE, "TYPE STRUCT<name", false);
  }

  @Test
  public void validateRejectsExternalImports() {
    try {
      LogicalSchema.validate(
          SUBJECT,
          AvroSchema.TYPE,
          "USING TYPE Ext FOR REF 'http://example.com/ext.json'; TYPE STRUCT<f Ext>",
          false);
      throw new AssertionError("expected validation to fail on external imports");
    } catch (ValidationException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("external imports"));
    }
  }
}
