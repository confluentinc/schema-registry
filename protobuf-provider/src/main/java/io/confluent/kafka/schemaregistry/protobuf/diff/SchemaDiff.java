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

import com.google.common.base.Objects;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import io.confluent.kafka.schemaregistry.protobuf.diff.Context.TypeElementInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_CONST_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_CONST_CHANGED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_CONST_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_NAME_CHANGED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_STRING_OR_BYTES_LABEL_CHANGED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.MESSAGE_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.MESSAGE_MOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.MESSAGE_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_FIELD_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.PACKAGE_CHANGED;

public class SchemaDiff {
  public static final Set<Difference.Type> COMPATIBLE_CHANGES;

  static {
    Set<Difference.Type> changes = new HashSet<>();

    changes.add(PACKAGE_CHANGED);
    changes.add(MESSAGE_ADDED);
    changes.add(MESSAGE_MOVED);
    changes.add(ENUM_ADDED);
    changes.add(ENUM_REMOVED);
    changes.add(ENUM_CONST_ADDED);
    changes.add(ENUM_CONST_CHANGED);
    changes.add(ENUM_CONST_REMOVED);
    changes.add(FIELD_ADDED);
    changes.add(FIELD_REMOVED);
    changes.add(FIELD_NAME_CHANGED);
    changes.add(FIELD_STRING_OR_BYTES_LABEL_CHANGED);
    changes.add(ONEOF_ADDED);
    changes.add(ONEOF_REMOVED);
    changes.add(ONEOF_FIELD_ADDED);

    COMPATIBLE_CHANGES = Collections.unmodifiableSet(changes);
  }

  public static List<Difference> compare(
      final ProtobufSchema original,
      final ProtobufSchema update
  ) {
    final Context ctx = new Context(COMPATIBLE_CHANGES);
    ctx.collectTypeInfo(original, true);
    ctx.collectTypeInfo(update, false);
    compare(ctx, original.rawSchema(), update.rawSchema());
    return ctx.getDifferences();
  }

  static void compare(final Context ctx, ProtoFileElement original, ProtoFileElement update) {
    if (!Objects.equal(original.getPackageName(), update.getPackageName())) {
      ctx.addDifference(PACKAGE_CHANGED);
    }
    compareTypeElements(ctx, original.getTypes(), update.getTypes());
  }

  public static void compareTypeElements(
      final Context ctx, final List<TypeElement> original, final List<TypeElement> update
  ) {
    Map<String, MessageElement> originalMessages = new HashMap<>();
    Map<String, MessageElement> updateMessages = new HashMap<>();
    Map<String, Integer> originalMessageIndexes = new HashMap<>();
    Map<String, Integer> updateMessageIndexes = new HashMap<>();
    Map<String, EnumElement> originalEnums = new HashMap<>();
    Map<String, EnumElement> updateEnums = new HashMap<>();
    compareMessageElements(original, originalMessages, originalMessageIndexes, originalEnums);
    compareMessageElements(update, updateMessages, updateMessageIndexes, updateEnums);

    Set<String> allMessageNames = new HashSet<>(originalMessages.keySet());
    allMessageNames.addAll(updateMessages.keySet());
    Set<String> allEnumNames = new HashSet<>(originalEnums.keySet());
    allEnumNames.addAll(updateEnums.keySet());

    for (String name : allMessageNames) {
      try (Context.NamedScope nameScope = ctx.enterName(name)) {
        MessageElement originalMessage = originalMessages.get(name);
        MessageElement updateMessage = updateMessages.get(name);
        if (updateMessage == null) {
          TypeElementInfo originalType = ctx.getType(name, true);
          if (originalType != null && !originalType.isMap()) {
            ctx.addDifference(MESSAGE_REMOVED);
          }
        } else if (originalMessage == null) {
          TypeElementInfo updateType = ctx.getType(name, false);
          if (updateType != null && !updateType.isMap()) {
            ctx.addDifference(MESSAGE_ADDED);
          }
        } else {
          MessageSchemaDiff.compare(ctx, originalMessage, updateMessage);
          Integer originalMessageIndex = originalMessageIndexes.get(name);
          Integer updateMessageIndex = updateMessageIndexes.get(name);
          if (originalMessageIndex == null || !originalMessageIndex.equals(updateMessageIndex)) {
            // Moving or reordering a message is compatible since serialized message indexes
            // are w.r.t. the schema of the corresponding ID
            ctx.addDifference(MESSAGE_MOVED);
          }
        }
      }
    }

    for (String name : allEnumNames) {
      try (Context.NamedScope nameScope = ctx.enterName(name)) {
        EnumElement originalEnum = originalEnums.get(name);
        EnumElement updateEnum = updateEnums.get(name);
        if (updateEnum == null) {
          ctx.addDifference(ENUM_REMOVED);
        } else if (originalEnum == null) {
          ctx.addDifference(ENUM_ADDED);
        } else {
          EnumSchemaDiff.compare(ctx, originalEnum, updateEnum);
        }
      }
    }
  }

  private static void compareMessageElements(
      List<TypeElement> types,
      Map<String, MessageElement> messages,
      Map<String, Integer> messageIndexes,
      Map<String, EnumElement> enums
  ) {
    int index = 0;
    for (TypeElement typeElement : types) {
      if (typeElement instanceof MessageElement) {
        MessageElement messageElement = (MessageElement) typeElement;
        messages.put(messageElement.getName(), messageElement);
        messageIndexes.put(messageElement.getName(), index++);
      } else if (typeElement instanceof EnumElement) {
        EnumElement enumElement = (EnumElement) typeElement;
        enums.put(enumElement.getName(), enumElement);
      }
    }
  }
}
