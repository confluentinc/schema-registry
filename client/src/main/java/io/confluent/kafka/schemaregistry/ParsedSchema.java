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

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

/**
 * A parsed schema.
 *
 * <p>Implementations of this interface are instantiated by a corresponding
 * {@link io.confluent.kafka.schemaregistry.SchemaProvider}.
 */
public interface ParsedSchema {

  /**
   * Returns the schema type.
   *
   * @return the schema type
   */
  String schemaType();

  /**
   * Returns a name for the schema.
   *
   * @return the name, or null
   */
  String name();

  /**
   * Returns a canonical string representation of the schema.
   *
   * @return the canonical representation
   */
  String canonicalString();

  /**
   * Returns a formatted string according to a type-specific format.
   *
   * @return the formatted string
   */
  default String formattedString(String format) {
    if (format == null || format.trim().isEmpty()) {
      return canonicalString();
    }
    throw new IllegalArgumentException("Format not supported: " + format);
  }

  /**
   * Returns the version of the schema if set.
   *
   * @return the version
   */
  Integer version();

  /**
   * Returns a list of schema references.
   *
   * @return the schema references
   */
  List<SchemaReference> references();

  /**
   * Returns metadata.
   *
   * @return the metadata
   */
  Metadata metadata();

  /**
   * Returns a rule set.
   *
   * @return the rule set
   */
  RuleSet ruleSet();

  /**
   * Returns the set of tags used by the schema.
   *
   * @return the tags
   */
  default Set<String> tags() {
    Set<String> inlineTags = inlineTags();
    Metadata metadata = metadata();
    if (metadata == null || metadata.getTags() == null) {
      return inlineTags;
    }
    Set<String> allTags = new LinkedHashSet<>(inlineTags);
    metadata.getTags().forEach((key, value) -> allTags.addAll(value));
    return allTags;
  }

  /**
   * Returns the set of inline tags embedded in the schema.
   *
   * @return the tags
   */
  default Set<String> inlineTags() {
    return Collections.emptySet();
  }

  /**
   * Returns a copy of this schema.
   *
   * @return a copy of this schema
   */
  ParsedSchema copy();

  /**
   * Returns a copy of this schema, but with the given version.
   *
   * @param version the version
   * @return a copy of this schema, but with the given version
   */
  ParsedSchema copy(Integer version);

  /**
   * Returns a copy of this schema, but with the given metadata and rule set.
   *
   * @param metadata the metadata
   * @param ruleSet the rule set
   * @return a copy of this schema, but with the given metadata and rule set
   */
  ParsedSchema copy(Metadata metadata, RuleSet ruleSet);

  /**
   * Returns a copy of this schema, but with the given tags.
   *
   * @param tagsToAdd map of tags to add to the schema record or field, where the key is the entity
   *                  and the value is the set of tags. If the tag already exists, do nothing.
   * @param tagsToRemove map of tags to remove from the schema record or field, where the key is
   *                     the entity and the value is the set of tags. If the tag does not exist,
   *                     do nothing.
   * @return a copy of this schema, but with the given tags
   */
  ParsedSchema copy(Map<SchemaEntity, Set<String>> tagsToAdd,
                    Map<SchemaEntity, Set<String>> tagsToRemove);

  /**
   * Returns a normalized copy of this schema.
   * Normalization generally ignores ordering when it is not significant.
   *
   * @return the normalized representation
   */
  default ParsedSchema normalize() {
    return this;
  }

  /**
   * Validates the schema and ensures all references are resolved properly.
   * Throws an exception if the schema is not valid.
   */
  default void validate() {
  }

  /**
   * Checks the backward compatibility between this schema and the specified schema.
   * <p/>
   * Custom providers may choose to modify this schema during this check,
   * to ensure that it is compatible with the specified schema.
   *
   * @param previousSchema previous schema
   * @return an empty list if this schema is backward compatible with the previous schema,
   *         otherwise the list of error messages
   */
  List<String> isBackwardCompatible(ParsedSchema previousSchema);

  /**
   * Checks the compatibility between this schema and the specified schemas.
   * <p/>
   * Custom providers may choose to modify this schema during this check,
   * to ensure that it is compatible with the specified schemas.
   *
   * @param level the compatibility level
   * @param previousSchemas full schema history in chronological order
   * @return an empty list if this schema is backward compatible with the previous schema, otherwise
   *         the list of error messages
   */
  default List<String> isCompatible(
      CompatibilityLevel level, List<ParsedSchemaHolder> previousSchemas) {
    return CompatibilityChecker.checker(level).isCompatibleWithHolders(this, previousSchemas);
  }

  /**
   * Returns the underlying raw representation of the schema.
   *
   * @return the raw schema
   */
  Object rawSchema();

  /**
   * Returns whether the underlying raw representations are equal.
   *
   * @return whether the underlying raw representations are equal
   */
  default boolean deepEquals(ParsedSchema schema) {
    return Objects.equals(rawSchema(), schema.rawSchema())
        && Objects.equals(metadata(), schema.metadata())
        && Objects.equals(ruleSet(), schema.ruleSet());
  }

  default Object fromJson(JsonNode json) throws IOException {
    throw new UnsupportedOperationException();
  }

  default JsonNode toJson(Object object) throws IOException {
    throw new UnsupportedOperationException();
  }

  default Object copyMessage(Object message) throws IOException {
    throw new UnsupportedOperationException();
  }

  default Object transformMessage(RuleContext ctx, FieldTransform transform, Object message)
      throws RuleException {
    throw new UnsupportedOperationException();
  }
}
