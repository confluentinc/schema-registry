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
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.stream.Collectors;

import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_CONST_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ENUM_CONST_CHANGED;
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
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.PACKAGE_CHANGED;

public class SchemaDiff {
  public static final Set<Difference.Type> COMPATIBLE_CHANGES;

  static {
    Set<Difference.Type> changes = new HashSet<>();

    changes.add(PACKAGE_CHANGED);
    changes.add(MESSAGE_ADDED);
    changes.add(MESSAGE_REMOVED);
    changes.add(ENUM_ADDED);
    changes.add(ENUM_REMOVED);
    changes.add(ENUM_CONST_ADDED);
    changes.add(ENUM_CONST_CHANGED);
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
      final ProtobufSchema original,
      final ProtobufSchema update
  ) {
    Map<String, SchemaReference> originalReferences = original.references().stream()
        .collect(Collectors.toMap(SchemaReference::getName, r -> r));
    Map<String, SchemaReference> updateReferences = update.references().stream()
        .collect(Collectors.toMap(SchemaReference::getName, r -> r));
    Map<String, ProtoFileElement> originalDependencies = original.dependencies();
    Map<String, ProtoFileElement> updateDependencies = update.dependencies();
    final Context ctx = new Context(COMPATIBLE_CHANGES);
    collectContextInfoForRefs(ctx, originalReferences, originalDependencies, true);
    collectContextInfoForRefs(ctx, updateReferences, updateDependencies, false);
    compare(ctx, original.rawSchema(), update.rawSchema());
    return ctx.getDifferences();
  }

  @SuppressWarnings("ConstantConditions")
  static void compare(final Context ctx, ProtoFileElement original, ProtoFileElement update) {
    String originalPackageName = original.getPackageName();
    if (originalPackageName == null) {
      originalPackageName = "";
    }
    String updatePackageName = update.getPackageName();
    if (updatePackageName == null) {
      updatePackageName = "";
    }
    ctx.setPackageName(originalPackageName, true);
    ctx.setPackageName(updatePackageName, false);
    if (!Objects.equal(originalPackageName, updatePackageName)) {
      ctx.addDifference(PACKAGE_CHANGED);
    }
    SchemaReference dummyRef = new SchemaReference("", "", -1);
    collectContextInfo(ctx, originalPackageName, originalPackageName,
        dummyRef, original.getTypes(), true);
    collectContextInfo(ctx, updatePackageName, updatePackageName,
        dummyRef, update.getTypes(), false);
    compareTypeElements(ctx, original.getTypes(), update.getTypes());
  }

  private static void collectContextInfoForRefs(
      Context ctx,
      Map<String, SchemaReference> references,
      Map<String, ProtoFileElement> dependencies,
      boolean isOriginal) {
    for (Map.Entry<String, ProtoFileElement> entry : dependencies.entrySet()) {
      String refName = entry.getKey();
      ProtoFileElement protoFile = entry.getValue();
      SchemaReference ref = references.get(refName);
      String packageName = protoFile.getPackageName();
      if (packageName == null) {
        packageName = "";
      }
      collectContextInfo(ctx, packageName, packageName, ref, protoFile.getTypes(), isOriginal);
    }
  }

  private static void collectContextInfo(
      final Context ctx,
      final String scope,
      final String packageName,
      final SchemaReference ref,
      final List<TypeElement> types,
      boolean isOriginal
  ) {
    String prefix = scope.isEmpty() ? scope : scope + ".";
    for (TypeElement typeElement : types) {
      String qualifiedName = prefix + typeElement.getName();
      boolean isMap = false;
      Optional<FieldElement> key = Optional.empty();
      Optional<FieldElement> value = Optional.empty();
      if (typeElement instanceof MessageElement) {
        MessageElement messageElement = (MessageElement) typeElement;
        isMap = ProtobufSchema.findOption("map_entry", messageElement.getOptions())
            .map(o -> Boolean.valueOf(o.getValue().toString())).orElse(false);
        key = findField(ProtobufSchema.KEY_FIELD,
            messageElement.getFields());
        value = findField(ProtobufSchema.VALUE_FIELD,
            messageElement.getFields());
      }
      ctx.addType(qualifiedName, packageName, ref, typeElement,
          isMap, key.orElse(null), value.orElse(null), isOriginal);
      collectContextInfo(ctx, qualifiedName,
          packageName, ref, typeElement.getNestedTypes(), isOriginal);
    }
  }

  public static Optional<FieldElement> findField(String name, List<FieldElement> options) {
    return options.stream().filter(o -> o.getName().equals(name)).findFirst();
  }

  public static void compareTypeElements(
      final Context ctx, final List<TypeElement> original, final List<TypeElement> update
  ) {
    Map<String, MessageElement> originalMessages = new HashMap<>();
    Map<String, MessageElement> updateMessages = new HashMap<>();
    Map<String, EnumElement> originalEnums = new HashMap<>();
    Map<String, EnumElement> updateEnums = new HashMap<>();
    compareMessageElements(original, originalMessages, originalEnums);
    compareMessageElements(update, updateMessages, updateEnums);

    Set<String> allMessageNames = new HashSet<>(originalMessages.keySet());
    allMessageNames.addAll(updateMessages.keySet());
    Set<String> allEnumNames = new HashSet<>(originalEnums.keySet());
    allEnumNames.addAll(updateEnums.keySet());

    for (String name : allMessageNames) {
      try (Context.PathScope pathScope = ctx.enterName(name)) {
        MessageElement originalMessage = originalMessages.get(name);
        MessageElement updateMessage = updateMessages.get(name);
        if (updateMessage == null) {
          ctx.addDifference(MESSAGE_REMOVED);
        } else if (originalMessage == null) {
          ctx.addDifference(MESSAGE_ADDED);
        } else {
          MessageSchemaDiff.compare(ctx, originalMessage, updateMessage);
        }
      }
    }

    for (String name : allEnumNames) {
      try (Context.PathScope pathScope = ctx.enterName(name)) {
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
      Map<String, EnumElement> enums
  ) {
    for (TypeElement typeElement : types) {
      if (typeElement instanceof MessageElement) {
        MessageElement messageElement = (MessageElement) typeElement;
        messages.put(messageElement.getName(), messageElement);
      } else if (typeElement instanceof EnumElement) {
        EnumElement enumElement = (EnumElement) typeElement;
        enums.put(enumElement.getName(), enumElement);
      }
    }
  }
}
