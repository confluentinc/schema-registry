/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.json.diff;

import org.everit.json.schema.NumberSchema;

import java.math.BigDecimal;
import java.util.Objects;

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.EXCLUSIVE_MAXIMUM_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.EXCLUSIVE_MAXIMUM_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.EXCLUSIVE_MAXIMUM_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.EXCLUSIVE_MAXIMUM_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.EXCLUSIVE_MINIMUM_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.EXCLUSIVE_MINIMUM_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.EXCLUSIVE_MINIMUM_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.EXCLUSIVE_MINIMUM_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAXIMUM_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAXIMUM_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAXIMUM_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAXIMUM_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MINIMUM_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MINIMUM_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MINIMUM_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MINIMUM_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MULTIPLE_OF_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MULTIPLE_OF_CHANGED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MULTIPLE_OF_EXPANDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MULTIPLE_OF_REDUCED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MULTIPLE_OF_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.TYPE_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.TYPE_NARROWED;

class NumberSchemaDiff {
  static void compare(final Context ctx, final NumberSchema original, final NumberSchema update) {
    if (!Objects.equals(original.getMaximum(), update.getMaximum())) {
      if (original.getMaximum() == null && update.getMaximum() != null) {
        ctx.addDifference("maximum", MAXIMUM_ADDED);
      } else if (original.getMaximum() != null && update.getMaximum() == null) {
        ctx.addDifference("maximum", MAXIMUM_REMOVED);
      } else if (original.getMaximum().doubleValue() < update.getMaximum().doubleValue()) {
        ctx.addDifference("maximum", MAXIMUM_INCREASED);
      } else if (original.getMaximum().doubleValue() > update.getMaximum().doubleValue()) {
        ctx.addDifference("maximum", MAXIMUM_DECREASED);
      }
    }
    if (!Objects.equals(original.getMinimum(), update.getMinimum())) {
      if (original.getMinimum() == null && update.getMinimum() != null) {
        ctx.addDifference("minimum", MINIMUM_ADDED);
      } else if (original.getMinimum() != null && update.getMinimum() == null) {
        ctx.addDifference("minimum", MINIMUM_REMOVED);
      } else if (original.getMinimum().doubleValue() < update.getMinimum().doubleValue()) {
        ctx.addDifference("minimum", MINIMUM_INCREASED);
      } else if (original.getMinimum().doubleValue() > update.getMinimum().doubleValue()) {
        ctx.addDifference("minimum", MINIMUM_DECREASED);
      }
    }
    if (!Objects.equals(original.getExclusiveMaximumLimit(), update.getExclusiveMaximumLimit())) {
      if (original.getExclusiveMaximumLimit() == null
          && update.getExclusiveMaximumLimit() != null) {
        ctx.addDifference("exclusiveMaximum", EXCLUSIVE_MAXIMUM_ADDED);
      } else if (original.getExclusiveMaximumLimit() != null
          && update.getExclusiveMaximumLimit() == null) {
        ctx.addDifference("exclusiveMaximum", EXCLUSIVE_MAXIMUM_REMOVED);
      } else if (original.getExclusiveMaximumLimit().doubleValue()
          < update.getExclusiveMaximumLimit().doubleValue()) {
        ctx.addDifference("exclusiveMaximum", EXCLUSIVE_MAXIMUM_INCREASED);
      } else if (original.getExclusiveMaximumLimit().doubleValue() > update.getMaximum()
          .doubleValue()) {
        ctx.addDifference("exclusiveMaximum", EXCLUSIVE_MAXIMUM_DECREASED);
      }
    }
    if (!Objects.equals(original.getExclusiveMinimumLimit(), update.getExclusiveMinimumLimit())) {
      if (original.getExclusiveMinimumLimit() == null
          && update.getExclusiveMinimumLimit() != null) {
        ctx.addDifference("exclusiveMinimum", EXCLUSIVE_MINIMUM_ADDED);
      } else if (original.getExclusiveMinimumLimit() != null
          && update.getExclusiveMinimumLimit() == null) {
        ctx.addDifference("exclusiveMinimum", EXCLUSIVE_MINIMUM_REMOVED);
      } else if (original.getExclusiveMinimumLimit().doubleValue()
          < update.getExclusiveMinimumLimit().doubleValue()) {
        ctx.addDifference("exclusiveMinimum", EXCLUSIVE_MINIMUM_INCREASED);
      } else if (original.getExclusiveMinimumLimit().doubleValue()
          > update.getExclusiveMinimumLimit().doubleValue()) {
        ctx.addDifference("exclusiveMinimum", EXCLUSIVE_MINIMUM_DECREASED);
      }
    }
    BigDecimal updateMultipleOf = update.getMultipleOf() != null
        ? new BigDecimal(update.getMultipleOf().toString())
        : null;
    BigDecimal originalMultipleOf = original.getMultipleOf() != null
        ? new BigDecimal(original.getMultipleOf().toString())
        : null;
    if (!Objects.equals(originalMultipleOf, updateMultipleOf)) {
      if (originalMultipleOf == null) {
        ctx.addDifference("multipleOf", MULTIPLE_OF_ADDED);
      } else if (updateMultipleOf == null) {
        ctx.addDifference("multipleOf", MULTIPLE_OF_REMOVED);
      } else if (update.getMultipleOf().intValue() % original.getMultipleOf().intValue() == 0) {
        ctx.addDifference("multipleOf", MULTIPLE_OF_EXPANDED);
      } else if (original.getMultipleOf().intValue() % update.getMultipleOf().intValue() == 0) {
        ctx.addDifference("multipleOf", MULTIPLE_OF_REDUCED);
      } else {
        ctx.addDifference("multipleOf", MULTIPLE_OF_CHANGED);
      }
    }
    if (original.requiresInteger() != update.requiresInteger()) {
      if (original.requiresInteger()) {
        ctx.addDifference(TYPE_EXTENDED);
      } else {
        ctx.addDifference(TYPE_NARROWED);
      }
    }
  }
}
