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
   * Converts the Logical Type in {@code request}'s schema field into {@code request}'s declared
   * {@code schemaType}, replacing it in place.
   *
   * <p>{@code schemaType} is required here. Unlike a native registration, the request body under
   * {@code format=logical} never implies a target format, so there is no default to fall back to.
   */
  static void convertToNative(
      final SchemaRegistry schemaRegistry,
      final String subject,
      final RegisterSchemaRequest request)
      throws SchemaRegistryException {
    String schemaType = request.getSchemaType();
    if (schemaType == null || schemaType.trim().isEmpty()) {
      throw new InvalidSchemaException(
          "schemaType is required when format=logical, and must be one of "
              + "AVRO, JSON, PROTOBUF");
    }

    LogicalType parsed;
    if (request.getSchema() == null || request.getSchema().trim().isEmpty()) {
      throw new InvalidSchemaException("Schema is required when format=logical");
    }
    try {
      LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
      visitor.visit(LogicalTypesParserFactory.parse(request.getSchema()));
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
              "Unsupported schemaType '" + schemaType + "' for format=logical; "
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
    // validateAsNew=true: this is a new registration, so references to deleted versions must not
    // resolve, matching what the native register path does via canonicalizeSchema.
    // referenceVersionsStrict defaults to false: it is a per-provider setting configured on the
    // native SchemaProvider instances, which the logical registration path does not reach.
    Map<String, String> resolvedReferences;
    try {
      resolvedReferences = AbstractSchemaProvider.resolveReferences(
          schemaRegistry, subject, references, true, false);
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
