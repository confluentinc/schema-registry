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
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Converts a logical types DDL script into the native schema it denotes.
 *
 * <p>Shared by everything that reads DDL, so that a script means the same thing wherever it is
 * read -- in the registry, which converts a submitted body before storing it, and in a
 * {@link LogicalSchemaProvider}, which converts one while parsing. Callers differ only in where
 * they resolve references and what they do with the result, so those stay with them; everything
 * that decides what the script <em>means</em> lives here.
 *
 * <p>Parsing and conversion are separate calls because a caller classifies a body before
 * converting it, and splitting them lets it do both from a single parse.
 */
public final class LogicalTypeConversion {

  private LogicalTypeConversion() {
  }

  /**
   * Parses {@code text} as a DDL script, or returns empty if it is not one.
   *
   * <p>This decides only the form of the body. A script that parses but is semantically invalid
   * is still DDL, and says so when converted.
   */
  public static Optional<LogicalTypesParser.ScriptContext> tryParse(String text) {
    if (text == null || text.trim().isEmpty()) {
      return Optional.empty();
    }
    try {
      return Optional.of(LogicalTypesParserFactory.parse(text));
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }

  /**
   * Converts an already-parsed script into the native schema for {@code schema}'s type, named
   * after its subject and carrying its metadata, rule set and references.
   *
   * @param script              the parsed script, from {@link #tryParse(String)}
   * @param schema              what was submitted: its subject names the converted root, and its
   *                            schema type decides the target format
   * @param resolvedReferences  the schema's references resolved to their definitions, which the
   *                            caller looks up however it can
   * @throws ValidationException if the script is not a valid logical type, or cannot be
   *     represented in the requested format
   */
  public static ParsedSchema toNative(
      LogicalTypesParser.ScriptContext script,
      Schema schema,
      Map<String, String> resolvedReferences) {
    LogicalType logicalType =
        attachReferences(toLogicalType(script), schema.getReferences(), resolvedReferences);
    return withRequested(convert(schema, logicalType), schema);
  }

  /**
   * Derives the name given to a converted root that the script leaves anonymous. A script that
   * names its root uses that name instead, so this applies only to a bare {@code STRUCT}.
   */
  public static String rowName(String subject) {
    String sanitized = subject == null ? "" : subject.replaceAll("[^A-Za-z0-9_]", "_");
    if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
      sanitized = "Envelope" + sanitized;
    }
    return sanitized;
  }

  /**
   * Syntax decides logical-vs-native; semantics decides valid-vs-invalid. A script that parses
   * and then fails the visitor is a bad logical schema and must say so, rather than being left to
   * fall back to a confusing native error.
   */
  /**
   * Reads any registry schema into a {@link LogicalType}, dispatching on its format.
   *
   * <p>A convenience over the three format readers for callers that hold a {@code ParsedSchema} and
   * do not care which it is. It covers the plain case only — use
   * {@code JsonToLogicalTypeConverter} and friends directly when you need their options, such as
   * V1 emission mode or an explicit reference context.
   */
  public static LogicalType toLogicalType(ParsedSchema schema) {
    if (schema instanceof AvroSchema) {
      return AvroToLogicalTypeConverter.toLogicalType((AvroSchema) schema);
    } else if (schema instanceof ProtobufSchema) {
      return ProtoToLogicalTypeConverter.toLogicalType((ProtobufSchema) schema);
    } else if (schema instanceof JsonSchema) {
      return JsonToLogicalTypeConverter.toLogicalType((JsonSchema) schema);
    }
    throw new ValidationException("Unsupported schema type: " + schema.schemaType());
  }

  private static LogicalType toLogicalType(LogicalTypesParser.ScriptContext script) {
    try {
      LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
      visitor.visit(script);
      return visitor.toLogicalType();
    } catch (RuntimeException e) {
      throw new ValidationException("Invalid logical type schema: " + e.getMessage(), e);
    }
  }

  /**
   * A logical type carries external-type bindings but never registry coordinates, so a type that
   * references anything external is given the caller-declared references and the definitions they
   * resolve to, which is what the conversion needs to emit them.
   */
  private static LogicalType attachReferences(
      LogicalType parsed, List<SchemaReference> references, Map<String, String> resolved) {
    if (references == null || references.isEmpty()) {
      return parsed;
    }
    return new LogicalType(
        parsed.getName(),
        parsed.getNamespace(),
        parsed.getRootSchema(),
        parsed.getNamedTypes(),
        parsed.getExternalTypes(),
        parsed.getExternalImports(),
        references,
        resolved,
        parsed.getDefaultValues());
  }

  /**
   * External imports are left to the converters, which decide per format: they are a JSON
   * construct, so the Avro and Protobuf converters reject a type that carries them.
   */
  private static ParsedSchema convert(Schema schema, LogicalType logicalType) {
    String schemaType = schema.getSchemaType();
    String rowName = rowName(schema.getSubject());
    try {
      switch (schemaType.toUpperCase(Locale.ROOT)) {
        case AvroSchema.TYPE:
          return LogicalTypeToAvroConverter.fromLogicalType(logicalType, rowName);
        case JsonSchema.TYPE:
          return LogicalTypeToJsonConverter.fromLogicalType(logicalType, rowName);
        case ProtobufSchema.TYPE:
          return LogicalTypeToProtoConverter.fromLogicalType(logicalType, rowName);
        default:
          throw new ValidationException(
              "Unsupported schemaType '" + schemaType + "' for a logical type schema; "
                  + "must be one of " + AvroSchema.TYPE + ", " + JsonSchema.TYPE + ", "
                  + ProtobufSchema.TYPE);
      }
    } catch (ValidationException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new ValidationException(
          "Logical type schema cannot be represented as " + schemaType + ": " + e.getMessage(), e);
    }
  }

  /**
   * Carries the requested metadata and rule set onto the converted schema, the way a native parse
   * carries them onto what it reads. The conversion sets metadata of its own, describing how the
   * type was emitted, so the two are merged rather than replaced, with the request winning on a
   * conflicting property.
   */
  private static ParsedSchema withRequested(ParsedSchema converted, Schema schema) {
    if (schema.getMetadata() == null && schema.getRuleSet() == null) {
      return converted;
    }
    Metadata metadata = Metadata.mergeMetadata(converted.metadata(), schema.getMetadata());
    RuleSet ruleSet = schema.getRuleSet() != null ? schema.getRuleSet() : converted.ruleSet();
    return converted.copy(metadata, ruleSet);
  }
}
