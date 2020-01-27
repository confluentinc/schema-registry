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

import org.everit.json.schema.StringSchema;

import java.util.Objects;

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_LENGTH_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_LENGTH_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_LENGTH_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_LENGTH_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_LENGTH_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_LENGTH_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_LENGTH_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_LENGTH_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PATTERN_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PATTERN_CHANGED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PATTERN_REMOVED;

class StringSchemaDiff {
  static void compare(final Context ctx, final StringSchema original, final StringSchema update) {
    if (!Objects.equals(original.getMaxLength(), update.getMaxLength())) {
      if (original.getMaxLength() == null && update.getMaxLength() != null) {
        ctx.addDifference("maxLength", MAX_LENGTH_ADDED);
      } else if (original.getMaxLength() != null && update.getMaxLength() == null) {
        ctx.addDifference("maxLength", MAX_LENGTH_REMOVED);
      } else if (original.getMaxLength() < update.getMaxLength()) {
        ctx.addDifference("maxLength", MAX_LENGTH_INCREASED);
      } else if (original.getMaxLength() > update.getMaxLength()) {
        ctx.addDifference("maxLength", MAX_LENGTH_DECREASED);
      }
    }
    if (!Objects.equals(original.getMinLength(), update.getMinLength())) {
      if (original.getMinLength() == null && update.getMinLength() != null) {
        ctx.addDifference("minLength", MIN_LENGTH_ADDED);
      } else if (original.getMinLength() != null && update.getMinLength() == null) {
        ctx.addDifference("minLength", MIN_LENGTH_REMOVED);
      } else if (original.getMinLength() < update.getMinLength()) {
        ctx.addDifference("minLength", MIN_LENGTH_INCREASED);
      } else if (original.getMinLength() > update.getMinLength()) {
        ctx.addDifference("minLength", MIN_LENGTH_DECREASED);
      }
    }
    if (original.getPattern() == null && update.getPattern() != null) {
      ctx.addDifference("pattern", PATTERN_ADDED);
    } else if (original.getPattern() != null && update.getPattern() == null) {
      ctx.addDifference("pattern", PATTERN_REMOVED);
    } else if (original.getPattern() != null
        && update.getPattern() != null
        && !original.getPattern().pattern().equals(update.getPattern().pattern())) {
      ctx.addDifference("pattern", PATTERN_CHANGED);
    }
  }
}
