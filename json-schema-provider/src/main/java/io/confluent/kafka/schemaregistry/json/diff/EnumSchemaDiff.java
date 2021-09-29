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

import org.everit.json.schema.EnumSchema;

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ENUM_ARRAY_CHANGED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ENUM_ARRAY_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ENUM_ARRAY_NARROWED;

class EnumSchemaDiff {
  static void compare(
      final Context ctx, final EnumSchema original, final EnumSchema update
  ) {
    if (!original.getPossibleValues().equals(update.getPossibleValues())) {
      if (update.getPossibleValues().containsAll(original.getPossibleValues())) {
        ctx.addDifference("enum", ENUM_ARRAY_EXTENDED);
      } else if (original.getPossibleValues().containsAll(update.getPossibleValues())) {
        ctx.addDifference("enum", ENUM_ARRAY_NARROWED);
      } else {
        ctx.addDifference("enum", ENUM_ARRAY_CHANGED);
      }
    }
  }
}
