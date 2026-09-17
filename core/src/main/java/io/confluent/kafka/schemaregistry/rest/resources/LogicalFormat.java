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
import io.confluent.kafka.schemaregistry.storage.LogicalPolicyChecker;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;


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
    LogicalType logicalType;
    try {
      logicalType = LogicalPolicyChecker.toLogicalType(parsedSchema);
    } catch (ValidationException | IllegalArgumentException e) {
      throw new InvalidSchemaException(
          "Stored schema cannot be represented as a logical type: " + e.getMessage(), e);
    }
    return LogicalTypeToDdlConverter.toDdl(logicalType);
  }
}
