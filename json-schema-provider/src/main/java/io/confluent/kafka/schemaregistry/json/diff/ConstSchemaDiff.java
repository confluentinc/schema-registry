/*
 * Copyright 2024 Confluent Inc.
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

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ENUM_ARRAY_CHANGED;

import org.everit.json.schema.ConstSchema;

class ConstSchemaDiff {
  static void compare(
      final Context ctx, final ConstSchema original, final ConstSchema update
  ) {
    if (!original.getPermittedValue().equals(update.getPermittedValue())) {
      ctx.addDifference("const", ENUM_ARRAY_CHANGED);
    }
  }
}
