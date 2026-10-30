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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.storage.LogicalPolicyChecker;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;


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
   * Converts the native schema in {@code schema} to Logical Type, based on its stored
   * {@code schemaType}.
   */
  static String convertToLogical(final SchemaRegistry schemaRegistry, final Schema schema)
      throws InvalidSchemaException {
    ParsedSchema parsedSchema = schemaRegistry.parseSchema(schema, false, false);
    try {
      // Read as all the messages of the file it re-exports, a wrapper has no DDL: a script's
      // messages are its own.
      if (parsedSchema instanceof ProtobufSchema
          && ProtoToLogicalTypeConverter.isPublicImportWrapper((ProtobufSchema) parsedSchema)
          && ProtoToLogicalTypeConverter.readsAsMultipleMessages((ProtobufSchema) parsedSchema)) {
        throw new ValidationException("it publicly imports a file of several messages and names "
            + "no root among them");
      }
      LogicalType logicalType = LogicalPolicyChecker.toLogicalType(parsedSchema);
      return LogicalTypeToDdlConverter.toDdl(logicalType);
    } catch (ValidationException | IllegalArgumentException e) {
      throw new InvalidSchemaException(
          "Stored schema cannot be represented as a logical type: " + e.getMessage(), e);
    }
  }
}
