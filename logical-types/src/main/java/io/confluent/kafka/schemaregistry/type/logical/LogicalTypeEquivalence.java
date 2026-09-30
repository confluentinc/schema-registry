/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.type.logical;

import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Whether two logical types describe the same data: {@link LogicalType#equivalent(LogicalType)}.
 */
final class LogicalTypeEquivalence {

  // The params naming a location or what it continues; any other param only documents.
  private static final List<String> IDENTITY_PARAMS = Arrays.asList(
      Schema.PROTOBUF_FIELD_NUMBER, Schema.PROTOBUF_ENUM_NUMBER, Schema.AVRO_ALIASES,
      ProtoToLogicalTypeConverter.MULTI_MESSAGE_ROOT_PARAM);

  private final Map<String, Schema> mine;
  private final Map<String, Schema> theirs;
  // Named type pairs already being compared: a recursive type is equivalent where it recurs.
  private final Set<List<String>> comparing = new HashSet<>();

  private LogicalTypeEquivalence(Map<String, Schema> mine, Map<String, Schema> theirs) {
    this.mine = mine;
    this.theirs = theirs;
  }

  // The root's own name and namespace (a JSON title, a Protobuf package) are never read by
  // provenance; named types are still compared by qualified name.
  static boolean equivalent(LogicalType a, LogicalType b) {
    return new LogicalTypeEquivalence(a.getNamedTypes(), b.getNamedTypes())
        .schemas(a.getRootSchema(), b.getRootSchema());
  }

  private boolean schemas(Schema a, Schema b) {
    if (a == null || b == null) {
      return a == b;
    }
    if (a.getType() != b.getType() || a.isNullable() != b.isNullable()) {
      return false;
    }
    if (!nativeSteps(a).equals(nativeSteps(b)) || !identityParams(a.getParams(), b.getParams())) {
      return false;
    }
    switch (a.getType()) {
      case STRUCT:
        // A multi-message root's fields name the file's messages; their order carries no data.
        return isMultiMessageRoot(a)
            ? fields(byName(a.getFields()), byName(b.getFields()), false)
            : fields(a.getFields(), b.getFields(), true);
      case ENUM:
        return enumValues(a.getEnumValues(), b.getEnumValues());
      case UNION:
        return branches(a.getBranches(), b.getBranches());
      case ARRAY:
      case MULTISET:
        return schemas(a.getElementType(), b.getElementType());
      case MAP:
        return schemas(a.getKeyType(), b.getKeyType())
            && schemas(a.getValueType(), b.getValueType());
      case NAMED_TYPE_REF:
        return namedTypes(a.getQualifiedName(), b.getQualifiedName());
      case DECIMAL:
        return a.getPrecision() == b.getPrecision() && a.getScale() == b.getScale();
      case VARCHAR:
      case CHAR:
      case BINARY:
      case VARBINARY:
        return a.getLength() == b.getLength();
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        return a.getPrecision() == b.getPrecision();
      default:
        return true;
    }
  }

  private boolean fields(List<Schema.Field> a, List<Schema.Field> b, boolean positions) {
    if (a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); i++) {
      Schema.Field x = a.get(i);
      Schema.Field y = b.get(i);
      boolean same = Objects.equals(x.getName(), y.getName())
          && (!positions || x.getPosition() == y.getPosition())
          && Objects.equals(x.getNativeNames(), y.getNativeNames());
      if (!same || !identityParams(x.getParams(), y.getParams())
          || !schemas(x.getSchema(), y.getSchema())) {
        return false;
      }
    }
    return true;
  }

  private boolean enumValues(List<Schema.EnumValue> a, List<Schema.EnumValue> b) {
    if (a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); i++) {
      if (!a.get(i).getSymbol().equals(b.get(i).getSymbol())
          || !identityParams(a.get(i).getParams(), b.get(i).getParams())) {
        return false;
      }
    }
    return true;
  }

  private boolean branches(List<Schema.UnionBranch> a, List<Schema.UnionBranch> b) {
    if (a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); i++) {
      Schema.UnionBranch x = a.get(i);
      Schema.UnionBranch y = b.get(i);
      boolean same = Arrays.asList(x.getName(), x.getNativeNames(), x.getNativeAliases(),
          x.getNativeTitle()).equals(Arrays.asList(y.getName(), y.getNativeNames(),
          y.getNativeAliases(), y.getNativeTitle()));
      if (!same || !identityParams(x.getParams(), y.getParams())
          || !schemas(x.getSchema(), y.getSchema())) {
        return false;
      }
    }
    return true;
  }

  // Named types are compared by name and by what each side defines under it.
  private boolean namedTypes(String a, String b) {
    if (!a.equals(b)) {
      return false;
    }
    Schema x = mine.get(a);
    Schema y = theirs.get(b);
    if (x == null || y == null) {
      return x == y;
    }
    List<String> pair = Arrays.asList(a, b);
    if (!comparing.add(pair)) {
      return true;
    }
    return schemas(x, y);
  }

  private static boolean isMultiMessageRoot(Schema struct) {
    return Boolean.TRUE.equals(
        struct.getParams().get(ProtoToLogicalTypeConverter.MULTI_MESSAGE_ROOT_PARAM));
  }

  private static List<Schema.Field> byName(List<Schema.Field> fields) {
    List<Schema.Field> sorted = new ArrayList<>(fields);
    sorted.sort(Comparator.comparing(Schema.Field::getName));
    return sorted;
  }

  // The native steps its converter recorded into and below the node.
  private static List<Object> nativeSteps(Schema schema) {
    return Arrays.asList(schema.getNativeEntryNames(), schema.getElementNativeNames(),
        schema.getKeyNativeNames(), schema.getValueNativeNames());
  }

  private static boolean identityParams(Map<String, Object> a, Map<String, Object> b) {
    for (String key : IDENTITY_PARAMS) {
      if (!Objects.equals(a.get(key), b.get(key))) {
        return false;
      }
    }
    return true;
  }
}
