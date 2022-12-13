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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * <p>
 * A Builder for creating SchemaValidators.
 * </p>
 */
public final class SchemaValidatorBuilder {
  private SchemaValidationStrategy strategy;
  private static final String NEW_PREFIX = "new";
  private static final String OLD_PREFIX = "old";

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
      Iterator<? extends ParsedSchema> schemas = schemasInOrder.iterator();
      if (schemas.hasNext()) {
        ParsedSchema existing = schemas.next();
        return strategy.validate(toValidate, existing);
      }
      return Collections.emptyList();
    };
  }

  public SchemaValidator validateAll() {
    valid();
    return (toValidate, schemasInOrder) -> {
      for (ParsedSchema existing : schemasInOrder) {
        List<String> errorMessages = strategy.validate(toValidate, existing);
        if (!errorMessages.isEmpty()) {
          return errorMessages;
        }
      }
      return Collections.emptyList();
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
      messages.replaceAll(e -> String.format(e, reader, writer));
      if (appendSchema) {
        if (existing.version() != null) {
          messages.add("{oldSchemaVersion: " + existing.version() + "}");
        }
        messages.add("{oldSchema: '" + existing + "'}");
      }
    }
    return messages;
  }
}
