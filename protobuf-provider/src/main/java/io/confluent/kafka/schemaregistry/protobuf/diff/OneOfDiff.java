/*
 * Copyright 2020-2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.protobuf.diff;

import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_FIELD_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_FIELD_REMOVED;

public class OneOfDiff {
  static void compare(final Context ctx, final OneOfElement original, final OneOfElement update) {
    Map<Integer, FieldElement> originalByTag = new HashMap<>();
    for (FieldElement field : original.getFields()) {
      originalByTag.put(field.getTag(), field);
    }
    Map<Integer, FieldElement> updateByTag = new HashMap<>();
    for (FieldElement field : update.getFields()) {
      updateByTag.put(field.getTag(), field);
    }

    Set<Integer> allTags = new HashSet<>(originalByTag.keySet());
    allTags.addAll(updateByTag.keySet());
    for (Integer tag : allTags) {
      try (Context.PathScope pathScope = ctx.enterPath(tag.toString())) {
        FieldElement originalField = originalByTag.get(tag);
        FieldElement updateField = updateByTag.get(tag);
        if (updateField == null) {
          ctx.addDifference(ONEOF_FIELD_REMOVED);
        } else if (originalField == null) {
          ctx.addDifference(ONEOF_FIELD_ADDED);
        } else {
          FieldSchemaDiff.compare(ctx, originalField, updateField);
        }
      }
    }
  }
}
