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
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.FieldElement;

import com.squareup.wire.schema.internal.parser.MessageElement;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.diff.Context.TypeElementInfo;
import java.util.Objects;

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
    TypeElementInfo originalType = ctx.getType(original.toString(), true);
    if (originalType != null && originalType.isMap()) {
      original = originalType.getMapType();
    }
    TypeElementInfo updateType = ctx.getType(update.toString(), false);
    if (updateType != null && updateType.isMap()) {
      update = updateType.getMapType();
    }

    Kind originalKind = kind(ctx, original, true);
    Kind updateKind = kind(ctx, update, false);
    if (!Objects.equals(originalKind, updateKind)) {
      ctx.addDifference(FIELD_KIND_CHANGED);
    } else {
      switch (originalKind) {
        case SCALAR:
          compareScalarTypes(ctx, original, update);
          break;
        case MESSAGE:
          compareMessageTypes(ctx, original, update);
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

  static void compareMessageTypes(
      final Context ctx,
      final ProtoType original,
      final ProtoType update
  ) {
    String originalFullName = ctx.resolve(original.toString(), true);
    String updateFullName = ctx.resolve(update.toString(), false);
    if (originalFullName == null || updateFullName == null) {
      // Could not resolve full names, use simple names
      if (!Objects.equals(original.toString(), update.toString())) {
        ctx.addDifference(FIELD_NAMED_TYPE_CHANGED);
      }
      return;
    }
    TypeElementInfo originalType = ctx.getType(originalFullName, true);
    TypeElementInfo updateType = ctx.getType(updateFullName, false);
    String originalLocalName = originalFullName.startsWith(originalType.packageName() + ".")
        ? originalFullName.substring(originalType.packageName().length() + 1)
        : originalFullName;
    String updateLocalName = updateFullName.startsWith(updateType.packageName() + ".")
        ? updateFullName.substring(updateType.packageName().length() + 1)
        : updateFullName;
    if (!Objects.equals(originalLocalName, updateLocalName)) {
      ctx.addDifference(FIELD_NAMED_TYPE_CHANGED);
    } else {
      SchemaReference originalRef = originalType.reference();
      SchemaReference updateRef = updateType.reference();
      // Don't need to compare if both are local or refer to same subject-version
      if (originalRef == null || updateRef == null
          || !Objects.equals(originalRef.getSubject(), updateRef.getSubject())
          || !Objects.equals(originalRef.getVersion(), updateRef.getVersion())) {
        final Context subctx = ctx.getSubcontext();
        subctx.setPackageName(originalType.packageName(), true);
        subctx.setPackageName(updateType.packageName(), false);
        subctx.setFullName(originalLocalName);  // same as updateLocalName
        MessageSchemaDiff.compare(
            subctx, (MessageElement) originalType.type(), (MessageElement) updateType.type());
        ctx.addDifferences(subctx.getDifferences());
        if (!subctx.isCompatible()) {
          ctx.addDifference(FIELD_NAMED_TYPE_CHANGED);
        }
      }
    }
  }

  static void compareMapTypes(final Context ctx, final ProtoType original, final ProtoType update) {
    compareTypes(ctx, original.getKeyType(), update.getKeyType());
    compareTypes(ctx, original.getValueType(), update.getValueType());
  }

  static Kind kind(final Context ctx, ProtoType type, boolean isOriginal) {
    if (type.isScalar()) {
      return Kind.SCALAR;
    } else if (type.isMap()) {
      return Kind.MAP;
    } else {
      TypeElementInfo typeInfo = ctx.getType(type.toString(), isOriginal);
      if (typeInfo != null && typeInfo.type() instanceof EnumElement) {
        return Kind.SCALAR;
      }
      return Kind.MESSAGE;
    }
  }

  enum Kind {
    SCALAR, MAP, MESSAGE
  }

  // Group the scalars, see https://developers.google.com/protocol-buffers/docs/proto3#updating
  static ScalarKind scalarKind(final Context ctx, ProtoType type, boolean isOriginal) {
    TypeElementInfo typeInfo = ctx.getType(type.toString(), isOriginal);
    if (typeInfo != null && typeInfo.type() instanceof EnumElement) {
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
