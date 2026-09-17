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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesParserFactory;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesSchemaVisitor;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import java.util.Locale;

/**
 * Recognizes and validates schemas authored as logical types DDL.
 *
 * <p>The DDL is registered as-is: the registry detects it and converts it to the requested native
 * format itself, so nothing here is needed to send one. These helpers only cover what has to
 * happen locally -- telling a DDL body apart from a malformed native one, and validating it
 * without a round trip.
 */
final class LogicalSchema {

  private LogicalSchema() {
  }

  /**
   * Returns whether {@code text} is logical types DDL.
   *
   * <p>Callers use this only after a native parse has already failed, mirroring the registry's
   * native-first rule: a body is logical only once it has been ruled out as native, so a malformed
   * native schema still reports its own error rather than a misleading DDL one.
   */
  static boolean isLogical(String text) {
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

  /**
   * Validates DDL the way registering it would: the script is parsed, then converted to
   * {@code schemaType} and validated in that form, since it is the converted schema that the
   * registry ultimately stores and enforces.
   *
   * @throws ValidationException if the DDL is malformed, carries external imports, or cannot be
   *     represented in the requested native format
   */
  static void validate(String subject, String schemaType, String text, boolean strict) {
    LogicalType logicalType = toLogicalType(text);
    if (!logicalType.getExternalImports().isEmpty()) {
      throw new ValidationException(
          "Cannot validate a logical type schema with external imports "
              + logicalType.getExternalImports().keySet()
              + "; resolving them requires the registry");
    }
    toNativeSchema(subject, schemaType, logicalType).validate(strict);
  }

  private static LogicalType toLogicalType(String text) {
    LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
    visitor.visit(LogicalTypesParserFactory.parse(text));
    return visitor.toLogicalType();
  }

  private static ParsedSchema toNativeSchema(
      String subject, String schemaType, LogicalType logicalType) {
    if (schemaType == null || schemaType.trim().isEmpty()) {
      throw new ValidationException(
          "schemaType is required for a logical type schema, and must be one of "
              + AvroSchema.TYPE + ", " + JsonSchema.TYPE + ", " + ProtobufSchema.TYPE);
    }
    String rowName = rowName(subject);
    switch (schemaType.toUpperCase(Locale.ROOT)) {
      case AvroSchema.TYPE:
        return LogicalTypeToAvroConverter.fromLogicalType(logicalType, rowName);
      case JsonSchema.TYPE:
        return LogicalTypeToJsonConverter.fromLogicalType(logicalType, rowName);
      case ProtobufSchema.TYPE:
        return LogicalTypeToProtoConverter.fromLogicalType(logicalType, rowName);
      default:
        throw new ValidationException(
            "Unsupported schemaType '" + schemaType + "' for a logical type schema; must be one of "
                + AvroSchema.TYPE + ", " + JsonSchema.TYPE + ", " + ProtobufSchema.TYPE);
    }
  }

  /**
   * Derives the name of the converted root record from the subject, matching how the registry
   * names it, so that local validation sees the same schema that registering would produce.
   */
  private static String rowName(String subject) {
    String sanitized = subject == null ? "" : subject.replaceAll("[^A-Za-z0-9_]", "_");
    if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
      sanitized = "Envelope" + sanitized;
    }
    return sanitized;
  }
}
