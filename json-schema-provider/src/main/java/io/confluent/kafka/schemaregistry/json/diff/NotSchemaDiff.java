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

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.NOT_TYPE_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.NOT_TYPE_NARROWED;

import org.everit.json.schema.NotSchema;

class NotSchemaDiff {
  static void compare(
      final Context ctx,
      final NotSchema original,
      final NotSchema update
  ) {
    try (Context.PathScope pathScope = ctx.enterPath("not")) {
      final Context subctx = ctx.getSubcontext();
      // Reverse wrapped schemas during compare
      SchemaDiff.compare(subctx, update.getMustNotMatch(), original.getMustNotMatch());
      if (subctx.isCompatible()) {
        ctx.addDifference(NOT_TYPE_NARROWED);
      } else {
        ctx.addDifference(NOT_TYPE_EXTENDED);
      }
    }
  }
}
