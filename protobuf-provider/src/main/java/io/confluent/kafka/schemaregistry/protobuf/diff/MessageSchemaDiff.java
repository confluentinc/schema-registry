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

        Map<String, OneOfElement> originalOneOfs = new HashMap<>();
        Map<String, OneOfElement> updateOneOfs = new HashMap<>();

        Map<Integer, FieldElement> originalOneOfsByTag = new HashMap<>();
        for (OneOfElement oneOf : original.getOneOfs()) {
          originalOneOfs.put(oneOf.getName(), oneOf);

          for (FieldElement oneOfField : oneOf.getFields()) {
            originalOneOfsByTag.put(oneOfField.getTag(), oneOfField);
          }
        }

        for (OneOfElement oneOf : update.getOneOfs()) {
          updateOneOfs.put(oneOf.getName(), oneOf);

          try (Context.PathScope pathScope = ctx.enterPath(oneOf.getName())) {
            int numMoved = 0;
            int numExisting = 0;
            for (FieldElement oneOfField : oneOf.getFields()) {
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
      }
      SchemaDiff.compareTypeElements(ctx, original.getNestedTypes(), update.getNestedTypes());
    }
  }
}
