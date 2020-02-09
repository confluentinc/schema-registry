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

import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.internal.parser.FieldElement;

import java.util.Objects;
import java.util.Optional;

import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_KIND_CHANGED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_NAMED_TYPE_CHANGED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_NAME_CHANGED;
import static io.confluent.kafka.schemaregistry.protobuf.diff.Difference.Type.FIELD_SCALAR_KIND_CHANGED;

public class FieldSchemaDiff {
  static void compare(final Context ctx, final FieldElement original, final FieldElement update) {
    if (!Objects.equals(original.getName(), update.getName())) {
      ctx.addDifference(FIELD_NAME_CHANGED);
    }
    ProtoType originalType = ProtoType.get(original.getType());
    ProtoType updateType = ProtoType.get(update.getType());
    compareTypes(ctx, originalType, updateType);
  }

  static void compareTypes(final Context ctx, ProtoType original, ProtoType update) {
    Optional<ProtoType> originalMap = ctx.getMap(original.simpleName(), true);
    if (originalMap.isPresent()) original = originalMap.get();
    Optional<ProtoType> updateMap = ctx.getMap(update.simpleName(), false);
    if (updateMap.isPresent()) update = updateMap.get();

    Kind originalKind = kind(ctx, original, true);
    Kind updateKind = kind(ctx, update, false);
    if (!Objects.equals(originalKind, updateKind)) {
      ctx.addDifference(FIELD_KIND_CHANGED);
    } else {
      switch (originalKind) {
        case SCALAR:
          compareScalarTypes(ctx, original, update);
          break;
        case NAMED:
          compareNamedTypes(ctx, original, update);
          break;
        case MAP:
          compareMapTypes(ctx, original, update);
          break;
        default:
          break;
      }
    }
  }

  static void compareScalarTypes(
      final Context ctx,
      final ProtoType original,
      final ProtoType update
  ) {
    if (!Objects.equals(scalarKind(ctx, original, true), scalarKind(ctx, update, false))) {
      ctx.addDifference(FIELD_SCALAR_KIND_CHANGED);
    }
  }

  static void compareNamedTypes(
      final Context ctx,
      final ProtoType original,
      final ProtoType update
  ) {
    if (!Objects.equals(original.toString(), update.toString())) {
      ctx.addDifference(FIELD_NAMED_TYPE_CHANGED);
    }
  }

  static void compareMapTypes(final Context ctx, final ProtoType original, final ProtoType update) {
    compareTypes(ctx, original.keyType(), update.keyType());
    compareTypes(ctx, original.valueType(), update.valueType());
  }

  static Kind kind(final Context ctx, ProtoType type, boolean isOriginal) {
    if (type.isScalar()) {
      return Kind.SCALAR;
    } else if (type.isMap()) {
      return Kind.MAP;
    } else {
      if (ctx.containsEnum(type.simpleName(), isOriginal)) {
        return Kind.SCALAR;
      }
      return Kind.NAMED;
    }
  }

  enum Kind {
    SCALAR, MAP, NAMED
  }

  // Group the scalars, see https://developers.google.com/protocol-buffers/docs/proto3#updating
  static ScalarKind scalarKind(final Context ctx, ProtoType type, boolean isOriginal) {
    if (ctx.containsEnum(type.simpleName(), isOriginal)) {
      return ScalarKind.GENERAL_NUMBER;
    }
    switch (type.toString()) {
      case "int32":
      case "int64":
      case "uint32":
      case "uint64":
      case "bool":
        return ScalarKind.GENERAL_NUMBER;
      case "sint32":
      case "sint64":
        return ScalarKind.SIGNED_NUMBER;
      case "string":
      case "bytes":
        return ScalarKind.STRING_OR_BYTES;
      case "fixed32":
      case "sfixed32":
        return ScalarKind.FIXED32;
      case "fixed64":
      case "sfixed64":
        return ScalarKind.FIXED64;
      case "float":
        return ScalarKind.FLOAT;
      case "double":
        return ScalarKind.DOUBLE;
      default:
        break;
    }
    throw new IllegalArgumentException("Unknown type " + type);
  }

  enum ScalarKind {
    GENERAL_NUMBER, SIGNED_NUMBER, STRING_OR_BYTES, FIXED32, FIXED64, FLOAT, DOUBLE, ANY
  }
}
