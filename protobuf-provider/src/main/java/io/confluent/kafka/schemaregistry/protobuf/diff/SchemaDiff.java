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
 *
 */

package io.confluent.kafka.schemaregistry.protobuf.diff;

import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_CONST_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_CONST_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_NAME_CHANGED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.MESSAGE_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.MESSAGE_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_FIELD_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_REMOVED;

public class SchemaDiff {
  public static final Set<Difference.Type> COMPATIBLE_CHANGES;

  static {
    Set<Difference.Type> changes = new HashSet<>();

    changes.add(MESSAGE_ADDED);
    changes.add(MESSAGE_REMOVED);
    changes.add(ENUM_ADDED);
    changes.add(ENUM_REMOVED);
    changes.add(ENUM_CONST_ADDED);
    changes.add(ENUM_CONST_REMOVED);
    changes.add(FIELD_ADDED);
    changes.add(FIELD_REMOVED);
    changes.add(FIELD_NAME_CHANGED);
    changes.add(ONEOF_ADDED);
    changes.add(ONEOF_REMOVED);
    changes.add(ONEOF_FIELD_ADDED);

    COMPATIBLE_CHANGES = Collections.unmodifiableSet(changes);
  }

  public static List<Difference> compare(
      final ProtoFileElement original,
      final ProtoFileElement update
  ) {
    final Context ctx = new Context(COMPATIBLE_CHANGES);
    compare(ctx, original, update);
    return ctx.getDifferences();
  }

  @SuppressWarnings("ConstantConditions")
  static void compare(final Context ctx, ProtoFileElement original, ProtoFileElement update) {
    collectEnums(ctx, original.getTypes(), true);
    collectEnums(ctx, update.getTypes(), false);
    compareTypeElements(ctx, original.getTypes(), update.getTypes());
  }

  private static void collectEnums(
      final Context ctx,
      final List<TypeElement> types,
      boolean isOriginal
  ) {
    for (TypeElement typeElement : types) {
      if (typeElement instanceof MessageElement) {
        MessageElement messageElement = (MessageElement) typeElement;
        collectEnums(ctx, messageElement.getNestedTypes(), isOriginal);
      } else if (typeElement instanceof EnumElement) {
        ctx.addEnum(typeElement.getName(), isOriginal);
      }
    }
  }

  public static void compareTypeElements(
      final Context ctx, final List<TypeElement> original, final List<TypeElement> update
  ) {
    Map<String, MessageElement> originalMessages = new HashMap<>();
    Map<String, MessageElement> updateMessages = new HashMap<>();
    Map<String, EnumElement> originalEnums = new HashMap<>();
    Map<String, EnumElement> updateEnums = new HashMap<>();
    for (TypeElement typeElement : original) {
      if (typeElement instanceof MessageElement) {
        MessageElement messageElement = (MessageElement) typeElement;
        originalMessages.put(messageElement.getName(), messageElement);
      } else if (typeElement instanceof EnumElement) {
        EnumElement enumElement = (EnumElement) typeElement;
        originalEnums.put(enumElement.getName(), enumElement);
      }
    }
    for (TypeElement typeElement : update) {
      if (typeElement instanceof MessageElement) {
        MessageElement messageElement = (MessageElement) typeElement;
        updateMessages.put(messageElement.getName(), messageElement);
      } else if (typeElement instanceof EnumElement) {
        EnumElement enumElement = (EnumElement) typeElement;
        updateEnums.put(enumElement.getName(), enumElement);
      }
    }

    Set<String> allMessageNames = new HashSet<>(originalMessages.keySet());
    allMessageNames.addAll(updateMessages.keySet());
    Set<String> allEnumNames = new HashSet<>(originalEnums.keySet());
    allEnumNames.addAll(updateEnums.keySet());

    for (String name : allMessageNames) {
      try (Context.PathScope pathScope = ctx.enterPath(name)) {
        if (!updateMessages.containsKey(name)) {
          ctx.addDifference(MESSAGE_REMOVED);
        } else if (!originalMessages.containsKey(name)) {
          ctx.addDifference(MESSAGE_ADDED);
        } else {
          MessageSchemaDiff.compare(ctx, originalMessages.get(name), updateMessages.get(name));
        }
      }
    }

    for (String name : allEnumNames) {
      try (Context.PathScope pathScope = ctx.enterPath(name)) {
        if (!updateEnums.containsKey(name)) {
          ctx.addDifference(ENUM_REMOVED);
        } else if (!originalEnums.containsKey(name)) {
          ctx.addDifference(ENUM_ADDED);
        } else {
          EnumSchemaDiff.compare(ctx, originalEnums.get(name), updateEnums.get(name));
        }
      }
    }
  }
}
