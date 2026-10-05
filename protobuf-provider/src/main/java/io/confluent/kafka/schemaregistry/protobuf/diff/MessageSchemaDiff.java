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

import com.squareup.wire.schema.Field.Label;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_MOVED_TO_EXISTING_ONEOF;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.MULTIPLE_FIELDS_MOVED_TO_ONEOF;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_FIELD_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_FIELD_MOVED_TO_TOP_LEVEL;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_FIELD_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.REQUIRED_FIELD_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.REQUIRED_FIELD_REMOVED;

public class MessageSchemaDiff {
  static void compare(
      final Context ctx,
      final MessageElement original,
      final MessageElement update
  ) {
    try (Context.SchemaScope schemaScope = ctx.enterSchema(original)) {
      if (schemaScope != null) {
        // Index every field by tag (top-level fields and oneof members alike) and
        // track each tag's enclosing oneof, for both schemas. The diff is computed
        // per field by tag across the whole message, independent of oneof identity,
        // so that membership changes (rename, move in/out, merge) cannot hide
        // field-level changes such as a type change or a removal.
        Map<Integer, FieldElement> originalByTag = new HashMap<>();
        Map<Integer, String> originalTagToOneOf = new HashMap<>();
        Map<String, OneOfElement> originalOneOfs = new HashMap<>();
        collectFields(original, originalByTag, originalTagToOneOf, originalOneOfs);

        Map<Integer, FieldElement> updateByTag = new HashMap<>();
        Map<Integer, String> updateTagToOneOf = new HashMap<>();
        Map<String, OneOfElement> updateOneOfs = new HashMap<>();
        collectFields(update, updateByTag, updateTagToOneOf, updateOneOfs);

        // A oneof that is removed (by name) and none of whose members survive
        // anywhere in the update is a complete deletion, which is treated as
        // backward compatible. If some members survive (relocated) while others
        // are dropped, the dropped members are reported as removals in the
        // per-field pass below.
        Set<String> completelyDeletedOneOfs = new HashSet<>();
        for (Map.Entry<String, OneOfElement> entry : originalOneOfs.entrySet()) {
          if (!updateOneOfs.containsKey(entry.getKey())) {
            boolean anyMemberSurvives = false;
            for (FieldElement field : entry.getValue().getFields()) {
              if (updateByTag.containsKey(field.getTag())) {
                anyMemberSurvives = true;
                break;
              }
            }
            if (!anyMemberSurvives) {
              completelyDeletedOneOfs.add(entry.getKey());
            }
          }
        }

        // Detect newly introduced mutual exclusivity. For each update oneof, the
        // members were already mutually exclusive only if they came from the same
        // original oneof; each original top-level field is its own group; brand-new
        // fields are ignored (old data never set them). If the pre-existing members
        // come from more than one original group, previously independent fields are
        // now mutually exclusive, which is a backward-incompatible change.
        for (OneOfElement oneOf : update.getOneOfs()) {
          try (Context.PathScope pathScope = ctx.enterPath(oneOf.getName())) {
            Set<String> originGroups = new HashSet<>();
            int topLevelMovedIn = 0;
            for (FieldElement oneOfField : oneOf.getFields()) {
              int tag = oneOfField.getTag();
              if (originalTagToOneOf.containsKey(tag)) {
                originGroups.add("oneof:" + originalTagToOneOf.get(tag));
              } else if (originalByTag.containsKey(tag)) {
                originGroups.add("field:" + tag);
                topLevelMovedIn++;
              }
              // else: brand-new field -> ignored
            }
            if (originGroups.size() > 1) {
              if (topLevelMovedIn > 1) {
                ctx.addDifference(MULTIPLE_FIELDS_MOVED_TO_ONEOF);
              } else {
                ctx.addDifference(FIELD_MOVED_TO_EXISTING_ONEOF);
              }
            }
          }
        }

        // Oneof containers added/removed (informational, backward compatible).
        // Iterate in a stable order for deterministic difference ordering.
        Set<String> allOneOfs = new TreeSet<>(originalOneOfs.keySet());
        allOneOfs.addAll(updateOneOfs.keySet());
        for (String oneOfName : allOneOfs) {
          try (Context.PathScope pathScope = ctx.enterPath(oneOfName)) {
            if (!updateOneOfs.containsKey(oneOfName)) {
              ctx.addDifference(ONEOF_REMOVED);
            } else if (!originalOneOfs.containsKey(oneOfName)) {
              ctx.addDifference(ONEOF_ADDED);
            }
          }
        }

        // Single pass over every field by tag. Iterate in tag order for
        // deterministic difference ordering.
        Set<Integer> allTags = new TreeSet<>(originalByTag.keySet());
        allTags.addAll(updateByTag.keySet());
        for (Integer tag : allTags) {
          FieldElement originalField = originalByTag.get(tag);
          FieldElement updateField = updateByTag.get(tag);
          String originalOneOf = originalTagToOneOf.get(tag);
          String updateOneOf = updateTagToOneOf.get(tag);
          if (updateField == null) {
            // Removed from the message entirely.
            if (originalOneOf != null) {
              // A member of an original oneof that no longer exists anywhere. This
              // is a genuine removal (data loss), unless the whole oneof was deleted.
              if (!completelyDeletedOneOfs.contains(originalOneOf)) {
                addAtOneOf(ctx, originalOneOf, tag, ONEOF_FIELD_REMOVED);
              }
            } else {
              try (Context.PathScope pathScope = ctx.enterPath(tag.toString())) {
                ctx.addDifference(originalField.getLabel() == Label.REQUIRED
                    ? REQUIRED_FIELD_REMOVED : FIELD_REMOVED);
              }
            }
          } else if (originalField == null) {
            // Added to the message.
            if (updateOneOf != null) {
              addAtOneOf(ctx, updateOneOf, tag, ONEOF_FIELD_ADDED);
            } else {
              try (Context.PathScope pathScope = ctx.enterPath(tag.toString())) {
                ctx.addDifference(updateField.getLabel() == Label.REQUIRED
                    ? REQUIRED_FIELD_ADDED : FIELD_ADDED);
              }
            }
          } else {
            // Present in both. First record a compatible relocation of a field out
            // of its oneof to a top-level field (the field keeps its tag and type,
            // so this only relaxes mutual exclusivity). Then compare contents
            // regardless of oneof membership, reported at the field's current
            // location (oneof-qualified when it is a oneof member in the update,
            // otherwise the top-level tag path).
            if (originalOneOf != null && updateOneOf == null) {
              try (Context.PathScope pathScope = ctx.enterPath(tag.toString())) {
                ctx.addDifference(ONEOF_FIELD_MOVED_TO_TOP_LEVEL);
              }
            }
            if (updateOneOf != null) {
              try (Context.PathScope p1 = ctx.enterPath(updateOneOf);
                  Context.PathScope p2 = ctx.enterPath(tag.toString())) {
                FieldSchemaDiff.compare(ctx, originalField, updateField);
              }
            } else {
              try (Context.PathScope pathScope = ctx.enterPath(tag.toString())) {
                FieldSchemaDiff.compare(ctx, originalField, updateField);
              }
            }
          }
        }
      }
      SchemaDiff.compareTypeElements(ctx, original.getNestedTypes(), update.getNestedTypes());
    }
  }

  private static void collectFields(
      final MessageElement message,
      final Map<Integer, FieldElement> byTag,
      final Map<Integer, String> tagToOneOf,
      final Map<String, OneOfElement> oneOfs
  ) {
    for (FieldElement field : message.getFields()) {
      byTag.put(field.getTag(), field);
    }
    for (OneOfElement oneOf : message.getOneOfs()) {
      oneOfs.put(oneOf.getName(), oneOf);
      for (FieldElement field : oneOf.getFields()) {
        byTag.put(field.getTag(), field);
        tagToOneOf.put(field.getTag(), oneOf.getName());
      }
    }
  }

  private static void addAtOneOf(
      final Context ctx,
      final String oneOfName,
      final Integer tag,
      final Difference.Type type
  ) {
    try (Context.PathScope p1 = ctx.enterPath(oneOfName);
        Context.PathScope p2 = ctx.enterPath(tag.toString())) {
      ctx.addDifference(type);
    }
  }
}
