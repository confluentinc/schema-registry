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

package io.confluent.kafka.schemaregistry.protobuf.diff;

import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_CONST_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_CONST_CHANGED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_CONST_REMOVED;

public class EnumSchemaDiff {
  static void compare(final Context ctx, final EnumElement original, final EnumElement update) {
    Map<Integer, EnumConstantElement> originalByTag = new HashMap<>();
    for (EnumConstantElement enumer : original.getConstants()) {
      originalByTag.put(enumer.getTag(), enumer);
    }
    Map<Integer, EnumConstantElement> updateByTag = new HashMap<>();
    for (EnumConstantElement enumer : update.getConstants()) {
      updateByTag.put(enumer.getTag(), enumer);
    }
    Set<Integer> allTags = new HashSet<>(originalByTag.keySet());
    allTags.addAll(updateByTag.keySet());

    for (Integer tag : allTags) {
      try (Context.PathScope pathScope = ctx.enterPath(tag.toString())) {
        EnumConstantElement originalEnumConstant = originalByTag.get(tag);
        EnumConstantElement updateEnumConstant = updateByTag.get(tag);
        if (updateEnumConstant == null) {
          ctx.addDifference(ENUM_CONST_REMOVED);
        } else if (originalEnumConstant == null) {
          ctx.addDifference(ENUM_CONST_ADDED);
        } else if (!originalEnumConstant.getName().equals(updateEnumConstant.getName())) {
          ctx.addDifference(ENUM_CONST_CHANGED);
        }
      }
    }
  }
}
