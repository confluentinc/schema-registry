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

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.LogicalPolicyChecker;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesParserFactory;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesSchemaVisitor;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Converts between Logical Type format and the native Avro/JSON/Protobuf formats.
 */
final class LogicalFormat {

  static final String FORMAT_LOGICAL = "logical";

  private LogicalFormat() {
  }

  static boolean isLogical(String format) {
    return FORMAT_LOGICAL.equalsIgnoreCase(format);
  }

  /**
   * Auto-detects whether {@code request}'s body is a logical-types DDL rather than a native
   * Avro/JSON/Protobuf schema and, if it is, converts it to the requested native
   * {@code schemaType} in place -- so callers don't need to pass {@code format=logical} on input.
   *
   * <p>Detection and conversion are one method because they share the DDL parse: splitting them
   * would parse a logical body twice, once to classify it and once to convert it.
   *
   * <p>Native-first: the body is parsed as its declared {@code schemaType}, and only if that fails
   * is the logical DDL parse attempted. So a native schema always wins, and a body is logical only
   * once it has been ruled out as native. {@code parseSchema} resolves references, so a referenced
   * native schema still classifies correctly, and unparseable garbage stays native so its native
   * error is what surfaces rather than a misleading DDL one.
   *
   * <p>The native parse is not extra work in the common case: it goes through the registry's
   * {@code oldSchemaCache} under the same key that the {@code lookUpSchemaUnderSubject} performed
   * moments later on the register/lookup path uses, so that call is served from the cache. (The
   * key includes {@code normalize}, so a {@code normalize=true} request does pay for one extra
   * parse.)
   *
   * <p>Only {@code InvalidSchemaException} means "not native" -- the registry funnels every parse
   * failure into it. Catching it alone keeps unrelated runtime failures from being misclassified
   * as logical input.
   *
   * @return whether the body was logical and has been replaced by its native equivalent
   */
  static boolean tryConvertToNative(
      final SchemaRegistry schemaRegistry,
      final String subject,
      final RegisterSchemaRequest request)
      throws SchemaRegistryException {
    if (request == null || request.getSchema() == null || request.getSchema().isBlank()) {
      return false;
    }
    try {
      schemaRegistry.parseSchema(new Schema(subject, request), false, false);
      return false; // parses as its declared native schemaType
    } catch (InvalidSchemaException e) {
      // Not native. Fall through: logical iff it parses as DDL.
    }

    // Syntax decides logical-vs-native; semantics decides valid-vs-invalid. A body that does not
    // even parse as DDL is native, but one that parses and then fails the visitor is a bad logical
    // schema and must say so, rather than falling through to a confusing native error.
    LogicalTypesParser.ScriptContext script;
    try {
      script = LogicalTypesParserFactory.parse(request.getSchema());
    } catch (RuntimeException e) {
      return false; // neither native nor DDL -- leave it native so its native error surfaces
    }

    convertToNative(schemaRegistry, subject, request, script);
    return true;
  }

  /**
   * Converts an already-parsed logical type into {@code request}'s declared {@code schemaType},
   * replacing the request's schema in place.
   *
   * <p>{@code schemaType} is required here. Unlike a native registration, a logical DDL body never
   * implies a target format, so there is no default to fall back to.
   */
  private static void convertToNative(
      final SchemaRegistry schemaRegistry,
      final String subject,
      final RegisterSchemaRequest request,
      final LogicalTypesParser.ScriptContext script)
      throws SchemaRegistryException {
    String schemaType = request.getSchemaType();
    if (schemaType == null || schemaType.trim().isEmpty()) {
      throw new InvalidSchemaException(
          "schemaType is required for a logical type schema, and must be one of "
              + "AVRO, JSON, PROTOBUF");
    }

    LogicalType parsed;
    try {
      LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
      visitor.visit(script);
      parsed = visitor.toLogicalType();
    } catch (RuntimeException e) {
      throw new InvalidSchemaException("Invalid logical type schema: " + e.getMessage(), e);
    }

    LogicalType logicalType =
        attachReferences(schemaRegistry, subject, parsed, request.getReferences());
    String rowName = rowNameFor(subject);

    ParsedSchema nativeSchema;
    try {
      switch (schemaType.toUpperCase(Locale.ROOT)) {
        case "AVRO":
          nativeSchema = LogicalTypeToAvroConverter.fromLogicalType(logicalType, rowName);
          break;
        case "JSON":
          nativeSchema = LogicalTypeToJsonConverter.fromLogicalType(logicalType, rowName);
          break;
        case "PROTOBUF":
          nativeSchema = LogicalTypeToProtoConverter.fromLogicalType(logicalType, rowName);
          break;
        default:
          throw new InvalidSchemaException(
              "Unsupported schemaType '" + schemaType + "' for a logical type schema; "
                  + "must be one of AVRO, JSON, PROTOBUF");
      }
    } catch (RuntimeException e) {
      throw new InvalidSchemaException(
          "Logical type schema cannot be represented as " + schemaType + ": "
              + e.getMessage(), e);
    }
    request.setSchemaType(nativeSchema.schemaType());
    request.setSchema(nativeSchema.canonicalString());
  }

  /**
   * Converts the native schema in {@code schema} to Logical Type, based on its stored
   * {@code schemaType}.
   */
  static String convertToLogical(final SchemaRegistry schemaRegistry, final Schema schema)
      throws InvalidSchemaException {
    ParsedSchema parsedSchema = schemaRegistry.parseSchema(schema, false, false);
    LogicalType logicalType;
    try {
      logicalType = LogicalPolicyChecker.toLogicalType(parsedSchema);
    } catch (ValidationException | IllegalArgumentException e) {
      throw new InvalidSchemaException(
          "Stored schema cannot be represented as a logical type: " + e.getMessage(), e);
    }
    return LogicalTypeToDdlConverter.toDdl(logicalType);
  }

  /**
   * Logical Type carries external-type bindings (name/alias to URI) but never SR coordinates, see
   * {@link LogicalType#getExternalImports()}. If the parsed type references anything external,
   * this resolves it against the caller-declared {@code references} the same way a native
   * registration would -- reusing {@link AbstractSchemaProvider#resolveReferences} so
   * parent-context qualification and transitive resolution match native registration exactly, and
   * keying the result by each reference's own name, the convention every native
   * {@code resolvedReferences}
   * map already follows.
   */
  private static LogicalType attachReferences(
      final SchemaRegistry schemaRegistry,
      final String subject,
      final LogicalType parsed,
      final List<SchemaReference> references)
      throws SchemaRegistryException {
    if (references == null || references.isEmpty()) {
      return parsed;
    }
    // Both flags are deliberately permissive, because this resolution is not a validation gate --
    // it only supplies the referenced definitions the conversion needs to emit a native schema.
    // Every caller feeds that native schema straight into a path that re-resolves the same
    // references and enforces the configured mode: validateAsNew is derived per-caller
    // (schemaId < 0 && schema.validate.new.schemas on register, the config alone on a
    // compatibility check, false on lookup), and referenceVersionsStrict comes from the provider.
    // Resolving strictly here would reject a soft-deleted reference that the equivalent native
    // request accepts whenever the caller's own flag works out to false.
    Map<String, String> resolvedReferences;
    try {
      resolvedReferences = AbstractSchemaProvider.resolveReferences(
          schemaRegistry, subject, references, false, false);
    } catch (IllegalArgumentException | IllegalStateException e) {
      // resolveReferences throws IllegalArgument/IllegalState on a missing or conflicting
      // reference; surface it as a clean InvalidSchemaException (422), the same way the native
      // register path wraps it in AbstractSchemaRegistry.loadSchema.
      throw new InvalidSchemaException("Could not resolve schema references: " + e.getMessage(), e);
    }
    return new LogicalType(
        parsed.getName(),
        parsed.getNamespace(),
        parsed.getRootSchema(),
        parsed.getNamedTypes(),
        parsed.getExternalTypes(),
        parsed.getExternalImports(),
        references,
        resolvedReferences,
        parsed.getDefaultValues());
  }

  private static String rowNameFor(final String subject) {
    String sanitized = subject == null ? "" : subject.replaceAll("[^A-Za-z0-9_]", "_");
    if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
      sanitized = "Envelope" + sanitized;
    }
    return sanitized;
  }
}
