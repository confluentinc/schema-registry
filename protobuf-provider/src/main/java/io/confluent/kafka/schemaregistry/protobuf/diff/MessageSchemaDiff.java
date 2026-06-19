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

import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_ADDED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_MOVED_TO_EXISTING_ONEOF;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_REMOVED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.MULTIPLE_FIELDS_MOVED_TO_ONEOF;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.ONEOF_ADDED;
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
        Map<Integer, FieldElement> originalByTag = new HashMap<>();
        for (FieldElement field : original.getFields()) {
          originalByTag.put(field.getTag(), field);
        }
        Map<Integer, FieldElement> updateByTag = new HashMap<>();
        for (FieldElement field : update.getFields()) {
          updateByTag.put(field.getTag(), field);
        }

        // Maps of every field by tag (top-level fields and oneof members alike),
        // used to compare field contents (type, kind, label) regardless of oneof
        // membership. This ensures incompatible field changes are detected even
        // when a field's enclosing oneof is renamed, or the field is moved into or
        // out of a oneof. The name of the enclosing oneof (if any) is tracked per
        // tag so that fields already compared elsewhere can be skipped.
        Map<Integer, FieldElement> originalAllByTag = new HashMap<>(originalByTag);
        Map<Integer, FieldElement> updateAllByTag = new HashMap<>(updateByTag);
        Map<Integer, String> originalTagToOneOf = new HashMap<>();
        Map<Integer, String> updateTagToOneOf = new HashMap<>();

        Map<String, OneOfElement> originalOneOfs = new HashMap<>();
        Map<String, OneOfElement> updateOneOfs = new HashMap<>();

        Map<Integer, FieldElement> originalOneOfsByTag = new HashMap<>();
        for (OneOfElement oneOf : original.getOneOfs()) {
          originalOneOfs.put(oneOf.getName(), oneOf);

          for (FieldElement oneOfField : oneOf.getFields()) {
            originalOneOfsByTag.put(oneOfField.getTag(), oneOfField);
            originalAllByTag.put(oneOfField.getTag(), oneOfField);
            originalTagToOneOf.put(oneOfField.getTag(), oneOf.getName());
          }
        }

        for (OneOfElement oneOf : update.getOneOfs()) {
          updateOneOfs.put(oneOf.getName(), oneOf);

          try (Context.PathScope pathScope = ctx.enterPath(oneOf.getName())) {
            int numMoved = 0;
            int numExisting = 0;
            for (FieldElement oneOfField : oneOf.getFields()) {
              updateAllByTag.put(oneOfField.getTag(), oneOfField);
              updateTagToOneOf.put(oneOfField.getTag(), oneOf.getName());
              // Remove the field so that a FIELD_REMOVED difference is not generated
              FieldElement originalField = originalByTag.remove(oneOfField.getTag());
              if (originalField != null) {
                numMoved++;
              } else if (originalOneOfsByTag.get(oneOfField.getTag()) != null) {
                numExisting++;
              }
            }
            // Check that each oneOf in the updated message maps to
            // at most one field in the original message
            if (numMoved > 1) {
              ctx.addDifference(MULTIPLE_FIELDS_MOVED_TO_ONEOF);
            // Check that if a single field is moved that each oneOf field
            // did not exist in a previous oneOf
            } else if (numMoved == 1 && numExisting > 0) {
              ctx.addDifference(FIELD_MOVED_TO_EXISTING_ONEOF);
            }
          }
        }

        Set<String> allOneOfs = new HashSet<>(originalOneOfs.keySet());
        allOneOfs.addAll(updateOneOfs.keySet());
        for (String oneOfName : allOneOfs) {
          try (Context.PathScope pathScope = ctx.enterPath(oneOfName)) {
            OneOfElement originalOneOf = originalOneOfs.get(oneOfName);
            OneOfElement updateOneOf = updateOneOfs.get(oneOfName);
            if (updateOneOf == null) {
              ctx.addDifference(ONEOF_REMOVED);
            } else if (originalOneOf == null) {
              ctx.addDifference(ONEOF_ADDED);
            } else {
              OneOfDiff.compare(ctx, originalOneOf, updateOneOf);
            }
          }
        }

        Set<Integer> allTags = new HashSet<>(originalByTag.keySet());
        allTags.addAll(updateByTag.keySet());
        for (Integer tag : allTags) {
          try (Context.PathScope pathScope = ctx.enterPath(tag.toString())) {
            FieldElement originalField = originalByTag.get(tag);
            FieldElement updateField = updateByTag.get(tag);
            if (updateField == null) {
              if (originalField.getLabel() == Label.REQUIRED) {
                ctx.addDifference(REQUIRED_FIELD_REMOVED);
              } else {
                ctx.addDifference(FIELD_REMOVED);
              }
            } else if (originalField == null) {
              if (updateField.getLabel() == Label.REQUIRED) {
                ctx.addDifference(REQUIRED_FIELD_ADDED);
              } else {
                ctx.addDifference(FIELD_ADDED);
              }
            } else {
              FieldSchemaDiff.compare(ctx, originalField, updateField);
            }
          }
        }

        // Compare field contents by tag across the entire message for fields whose
        // oneof membership changed (the enclosing oneof was renamed, or the field
        // was moved into or out of a oneof). Fields that are top-level in both
        // schemas, or in a same-named oneof in both schemas, are already compared
        // above; skip them so that duplicate differences are not generated.
        Set<Integer> commonTags = new HashSet<>(originalAllByTag.keySet());
        commonTags.retainAll(updateAllByTag.keySet());
        for (Integer tag : commonTags) {
          boolean topLevelInBoth =
              originalByTag.containsKey(tag) && updateByTag.containsKey(tag);
          String originalOneOfName = originalTagToOneOf.get(tag);
          String updateOneOfName = updateTagToOneOf.get(tag);
          boolean sameOneOfInBoth = originalOneOfName != null
              && originalOneOfName.equals(updateOneOfName);
          if (topLevelInBoth || sameOneOfInBoth) {
            continue;
          }
          try (Context.PathScope pathScope = ctx.enterPath(tag.toString())) {
            FieldSchemaDiff.compare(ctx, originalAllByTag.get(tag), updateAllByTag.get(tag));
          }
        }
      }
      SchemaDiff.compareTypeElements(ctx, original.getNestedTypes(), update.getNestedTypes());
    }
  }
}
