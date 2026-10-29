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

import java.util.Collections;
import java.util.Set;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.Schema;

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ENUM_ARRAY_CHANGED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ENUM_ARRAY_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ENUM_ARRAY_NARROWED;

class EnumSchemaDiff {
  static void compare(
      final Context ctx, final EnumSchema original, final EnumSchema update
  ) {
    compareValues(ctx, original.getPossibleValues(), update.getPossibleValues());
  }

  // A const is an enum of its one value, so a const and an enum compare by their values.
  static void compareConstAndEnum(final Context ctx, final Schema original, final Schema update) {
    compareValues(ctx, values(original), values(update));
  }

  static boolean isConstOrEnum(final Schema schema) {
    return schema instanceof ConstSchema || schema instanceof EnumSchema;
  }

  private static Set<Object> values(final Schema schema) {
    return schema instanceof ConstSchema
        ? Collections.singleton(((ConstSchema) schema).getPermittedValue())
        : ((EnumSchema) schema).getPossibleValues();
  }

  private static void compareValues(
      final Context ctx, final Set<Object> original, final Set<Object> update
  ) {
    if (!original.equals(update)) {
      if (update.containsAll(original)) {
        ctx.addDifference("enum", ENUM_ARRAY_EXTENDED);
      } else if (original.containsAll(update)) {
        ctx.addDifference("enum", ENUM_ARRAY_NARROWED);
      } else {
        ctx.addDifference("enum", ENUM_ARRAY_CHANGED);
      }
    }
  }
}
