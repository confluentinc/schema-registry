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

package io.confluent.kafka.schemaregistry.type.logical;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import java.util.Locale;
import java.util.Map;

/**
 * A schema provider that also accepts schemas written as logical types DDL, converting them to
 * the native format of the provider it wraps.
 *
 * <p>Native-first, the same rule the registry applies: the body is handed to the wrapped provider
 * first, and only a body that provider rejects is tried as DDL. So a native schema always wins,
 * and a body that is neither reports the native error rather than a misleading DDL one.
 *
 * <p>Note that this converts locally. A caller that wants the registry to do the conversion --
 * so that what it stores is decided server-side -- must send the DDL itself rather than the
 * schema this returns.
 */
public class LogicalSchemaProvider implements SchemaProvider {

  private final SchemaProvider delegate;

  public LogicalSchemaProvider(SchemaProvider delegate) {
    this.delegate = delegate;
  }

  /**
   * Returns whether {@code text} is logical types DDL, without converting it.
   *
   * <p>This only decides the form of the body. A script that parses but is semantically invalid
   * is still DDL, and says so when converted.
   */
  public static boolean isLogical(String text) {
    if (text == null || text.trim().isEmpty()) {
      return false;
    }
    try {
      LogicalTypesParserFactory.parse(text);
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  @Override
  public String schemaType() {
    return delegate.schemaType();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    delegate.configure(configs);
  }

  @Override
  public ParsedSchema parseSchemaOrElseThrow(
      Schema schema, boolean validateAsNew, boolean normalize) {
    try {
      return delegate.parseSchemaOrElseThrow(schema, validateAsNew, normalize);
    } catch (RuntimeException e) {
      LogicalTypesParser.ScriptContext script;
      try {
        script = LogicalTypesParserFactory.parse(schema.getSchema());
      } catch (RuntimeException notDdl) {
        // Neither native nor DDL: the native failure is the one worth reporting.
        throw e;
      }
      return toNative(schema, toLogicalType(script));
    }
  }

  /**
   * Syntax decides logical-vs-native; semantics decides valid-vs-invalid. A script that parses
   * and then fails the visitor is a bad logical schema and must say so, rather than falling back
   * to a confusing native error.
   */
  private LogicalType toLogicalType(LogicalTypesParser.ScriptContext script) {
    try {
      LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
      visitor.visit(script);
      return visitor.toLogicalType();
    } catch (RuntimeException e) {
      throw new ValidationException("Invalid logical type schema: " + e.getMessage(), e);
    }
  }

  private ParsedSchema toNative(Schema schema, LogicalType logicalType) {
    if (!logicalType.getExternalImports().isEmpty()) {
      throw new ValidationException(
          "Cannot convert a logical type schema with external imports "
              + logicalType.getExternalImports().keySet()
              + "; resolving them requires the registry");
    }
    String rowName = rowName(schema.getSubject());
    try {
      switch (schemaType().toUpperCase(Locale.ROOT)) {
        case AvroSchema.TYPE:
          return LogicalTypeToAvroConverter.fromLogicalType(logicalType, rowName);
        case JsonSchema.TYPE:
          return LogicalTypeToJsonConverter.fromLogicalType(logicalType, rowName);
        case ProtobufSchema.TYPE:
          return LogicalTypeToProtoConverter.fromLogicalType(logicalType, rowName);
        default:
          throw new ValidationException(
              "Unsupported schemaType '" + schemaType() + "' for a logical type schema; "
                  + "must be one of " + AvroSchema.TYPE + ", " + JsonSchema.TYPE + ", "
                  + ProtobufSchema.TYPE);
      }
    } catch (ValidationException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new ValidationException(
          "Logical type schema cannot be represented as " + schemaType() + ": " + e.getMessage(),
          e);
    }
  }

  /**
   * Derives the name of the converted root record from the subject, matching how the registry
   * names it. The subject is absent on the entry points that take a bare schema string, in which
   * case the name falls back to the same default the registry uses for an empty subject.
   */
  private static String rowName(String subject) {
    String sanitized = subject == null ? "" : subject.replaceAll("[^A-Za-z0-9_]", "_");
    if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
      sanitized = "Envelope" + sanitized;
    }
    return sanitized;
  }
}
