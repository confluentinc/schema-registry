/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <p>
 * A Builder for creating SchemaValidators.
 * </p>
 */
public final class SchemaValidatorBuilder {
  private static final Logger log = LoggerFactory.getLogger(SchemaValidatorBuilder.class);

  private SchemaValidationStrategy strategy;
  private static final String NEW_PREFIX = "new";
  private static final String OLD_PREFIX = "old";
  private static int MAX_SCHEMA_SIZE_FOR_LOGGING = 10 * 1024;
  private static String DIFFERENT_SCHEMA_TYPE = "Incompatible because of different schema type";

  /**
   * Use a strategy that validates that a schema can be used to read existing
   * schema(s) according to the JSON default schema resolution.
   */
  public SchemaValidatorBuilder canReadStrategy() {
    this.strategy = (toValidate, existing) -> formatErrorMessages(
      toValidate.isBackwardCompatible(existing),
      existing, NEW_PREFIX, OLD_PREFIX, true);
    return this;
  }

  /**
   * Use a strategy that validates that a schema can be read by existing
   * schema(s) according to the JSON default schema resolution.
   */
  public SchemaValidatorBuilder canBeReadStrategy() {
    this.strategy = (toValidate, existing) -> formatErrorMessages(
      existing.isBackwardCompatible(toValidate),
      existing, OLD_PREFIX, NEW_PREFIX, true);
    return this;
  }

  /**
   * Use a strategy that validates that a schema can read existing schema(s),
   * and vice-versa, according to the JSON default schema resolution.
   */
  public SchemaValidatorBuilder mutualReadStrategy() {

    this.strategy = (toValidate, existing) -> {
      List<String> result = formatErrorMessages(existing.isBackwardCompatible(toValidate),
          existing, OLD_PREFIX, NEW_PREFIX, false);
      result.addAll(formatErrorMessages(toValidate.isBackwardCompatible(existing),
          existing, NEW_PREFIX, OLD_PREFIX, true));
      return result;
    };
    return this;
  }

  public SchemaValidator validateLatest() {
    valid();
    return (toValidate, schemasInOrder) -> {
      Iterator<ParsedSchemaHolder> schemas = schemasInOrder.iterator();
      if (schemas.hasNext()) {
        ParsedSchemaHolder existing = schemas.next();
        ParsedSchema existingSchema = existing.schema();
        List<String> errorMessages;
        if (toValidate.schemaType().equals(existingSchema.schemaType())) {
          errorMessages = strategy.validate(toValidate, existingSchema);
        } else {
          errorMessages = Lists.newArrayList(DIFFERENT_SCHEMA_TYPE);
        }
        existing.clear();
        return errorMessages;
      }
      return new ArrayList<>();
    };
  }

  public SchemaValidator validateAll() {
    valid();
    return (toValidate, schemasInOrder) -> {
      for (ParsedSchemaHolder existing : schemasInOrder) {
        ParsedSchema existingSchema = existing.schema();
        List<String> errorMessages;
        if (toValidate.schemaType().equals(existingSchema.schemaType())) {
          errorMessages = strategy.validate(toValidate, existingSchema);
        } else {
          errorMessages = Lists.newArrayList(DIFFERENT_SCHEMA_TYPE);
        }
        existing.clear();
        if (!errorMessages.isEmpty()) {
          return errorMessages;
        }
      }
      return new ArrayList<>();
    };
  }

  private void valid() {
    if (null == strategy) {
      throw new RuntimeException("SchemaValidationStrategy not specified in builder");
    }
  }

  private List<String> formatErrorMessages(List<String> messages, ParsedSchema existing,
                                           String reader, String writer, boolean appendSchema) {
    if (messages.size() > 0) {
      try {
        messages.replaceAll(e -> String.format(e, reader, writer));
        if (appendSchema) {
          if (existing.version() != null) {
            messages.add("{oldSchemaVersion: " + existing.version() + "}");
          }
          if (existing.toString().length() <= MAX_SCHEMA_SIZE_FOR_LOGGING) {
            messages.add("{oldSchema: '" + existing + "'}");
          } else {
            messages.add("{oldSchema: <truncated> '"
                           + existing.toString().substring(0, MAX_SCHEMA_SIZE_FOR_LOGGING)
                           + "...'}");
          }
        }
      } catch (UnsupportedOperationException e) {
        // Ignore and return messages
        log.warn("Failed to append schema and version to error messages");
      }
    }
    return messages;
  }
}