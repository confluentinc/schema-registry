/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry;

import java.util.List;

/**
 * <p>
 * A SchemaValidator has one method, which validates that a {@link ParsedSchema} is
 * <b>compatible</b> with the other schemas provided.
 * </p>
 * <p>
 * What makes one Schema compatible with another is not part of the interface
 * contract.
 * </p>
 */
public interface SchemaValidator {

  /**
   * Validate one schema against others. The order of the schemas to validate
   * against is chronological from most recent to oldest, if there is a natural
   * chronological order. This allows some validators to identify which schemas
   * are the most "recent" in order to validate only against the most recent
   * schema(s).
   *
   * @param toValidate The schema to validate
   * @param existing The schemas to validate against, in order from most recent to latest if
   *     applicable
   * @return List of error message, otherwise empty list
   */
  List<String> validate(ParsedSchema toValidate, Iterable<ParsedSchemaHolder> existing);
}
