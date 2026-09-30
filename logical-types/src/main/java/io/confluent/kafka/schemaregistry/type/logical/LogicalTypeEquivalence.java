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
import io.confluent.kafka.schemaregistry.type.logical.provenance.IdentityPolicy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Whether two logical types describe the same data:
 * {@link LogicalType#equivalent(LogicalType, IdentityPolicy)}.
 */
final class LogicalTypeEquivalence {

  // The params naming what a location continues; any other param only documents. Protobuf
  // numbers are compared as each member's effective number instead, recorded or implied.
  private static final List<String> IDENTITY_PARAMS = Arrays.asList(
      Schema.AVRO_ALIASES, ProtoToLogicalTypeConverter.MULTI_MESSAGE_ROOT_PARAM);

  private final IdentityPolicy policy;
  private final Map<String, Schema> mine;
  private final Map<String, Schema> theirs;
  // Named type pairs already being compared: a recursive type is equivalent where it recurs.
  private final Set<List<String>> comparing = new HashSet<>();

  private LogicalTypeEquivalence(IdentityPolicy policy, Map<String, Schema> mine,
      Map<String, Schema> theirs) {
    this.policy = policy;
    this.mine = mine;
    this.theirs = theirs;
  }

  // The root's own name and namespace (a JSON title, a Protobuf package) are never read by
  // provenance; named types are still compared by qualified name.
  static boolean equivalent(LogicalType a, LogicalType b, IdentityPolicy policy) {
    return new LogicalTypeEquivalence(policy, a.getNamedTypes(), b.getNamedTypes())
        .schemas(a.getRootSchema(), b.getRootSchema());
  }

  private boolean schemas(Schema a, Schema b) {
    if (a == null || b == null) {
      return a == b;
    }
    if (a.isNullable() != b.isNullable() || !nativeSteps(a).equals(nativeSteps(b))
        || !identityParams(a.getParams(), b.getParams())) {
      return false;
    }
    boolean refA = a.getType() == Schema.Type.NAMED_TYPE_REF;
    if (refA != (b.getType() == Schema.Type.NAMED_TYPE_REF)) {
      // A JSON $ref and the body it names inline are one location to provenance: the body is
      // compared, at the reference's nullability.
      Schema x = refA ? mine.get(a.getQualifiedName()) : a;
      Schema y = refA ? b : theirs.get(b.getQualifiedName());
      return x != null && y != null && x.getType() == y.getType() && bodies(x, y);
    }
    return a.getType() == b.getType() && bodies(a, b);
  }

  private boolean bodies(Schema a, Schema b) {
    switch (a.getType()) {
      case STRUCT:
        return fields(a, b);
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

  // Fields are paired by name, as every format finds them; a Protobuf field's number, recorded
  // or implied by its position, must match too, as must each oneof member's.
  private boolean fields(Schema a, Schema b) {
    if (a.getFields().size() != b.getFields().size()) {
      return false;
    }
    List<Schema.Field> xs = sorted(a.getFields(), Schema.Field::getName);
    List<Schema.Field> ys = sorted(b.getFields(), Schema.Field::getName);
    Map<Object, Integer> impliedA = impliedNumbers(a);
    Map<Object, Integer> impliedB = impliedNumbers(b);
    for (int i = 0; i < xs.size(); i++) {
      Schema.Field x = xs.get(i);
      Schema.Field y = ys.get(i);
      boolean same = Objects.equals(x.getName(), y.getName())
          && Objects.equals(x.getNativeNames(), y.getNativeNames())
          && Objects.equals(number(x.getFieldNumber(), x, impliedA),
              number(y.getFieldNumber(), y, impliedB));
      if (!same || !identityParams(x.getParams(), y.getParams())
          || !oneofNumbers(x.getSchema(), y.getSchema(), impliedA, impliedB)
          || !schemas(x.getSchema(), y.getSchema())) {
        return false;
      }
    }
    return true;
  }

  // A oneof's members are numbered in the enclosing message's sequence.
  private boolean oneofNumbers(Schema a, Schema b, Map<Object, Integer> impliedA,
      Map<Object, Integer> impliedB) {
    if (policy != IdentityPolicy.PROTOBUF || !isUnion(a) || !isUnion(b)) {
      return true;
    }
    if (a.getBranches().size() != b.getBranches().size()) {
      return false;
    }
    List<Schema.UnionBranch> xs = sorted(a.getBranches(), Schema.UnionBranch::getName);
    List<Schema.UnionBranch> ys = sorted(b.getBranches(), Schema.UnionBranch::getName);
    for (int i = 0; i < xs.size(); i++) {
      if (!Objects.equals(number(xs.get(i).getFieldNumber(), xs.get(i), impliedA),
          number(ys.get(i).getFieldNumber(), ys.get(i), impliedB))) {
        return false;
      }
    }
    return true;
  }

  // Symbols are paired by name; a Protobuf one's number, recorded or its position, must match.
  private boolean enumValues(List<Schema.EnumValue> a, List<Schema.EnumValue> b) {
    if (a.size() != b.size()) {
      return false;
    }
    List<Schema.EnumValue> xs = sorted(a, Schema.EnumValue::getSymbol);
    List<Schema.EnumValue> ys = sorted(b, Schema.EnumValue::getSymbol);
    for (int i = 0; i < xs.size(); i++) {
      if (!xs.get(i).getSymbol().equals(ys.get(i).getSymbol())
          || !Objects.equals(enumNumber(xs.get(i), a), enumNumber(ys.get(i), b))
          || !identityParams(xs.get(i).getParams(), ys.get(i).getParams())) {
        return false;
      }
    }
    return true;
  }

  // Branches are paired by name, except in JSON, where a branch is found by its position.
  private boolean branches(List<Schema.UnionBranch> a, List<Schema.UnionBranch> b) {
    if (a.size() != b.size()) {
      return false;
    }
    List<Schema.UnionBranch> xs = policy == IdentityPolicy.JSON
        ? a : sorted(a, Schema.UnionBranch::getName);
    List<Schema.UnionBranch> ys = policy == IdentityPolicy.JSON
        ? b : sorted(b, Schema.UnionBranch::getName);
    for (int i = 0; i < xs.size(); i++) {
      Schema.UnionBranch x = xs.get(i);
      Schema.UnionBranch y = ys.get(i);
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

  private Map<Object, Integer> impliedNumbers(Schema struct) {
    return policy == IdentityPolicy.PROTOBUF
        ? ProtoToLogicalTypeConverter.impliedFieldNumbers(struct) : Map.of();
  }

  private Integer number(Integer recorded, Object member, Map<Object, Integer> implied) {
    return recorded != null ? recorded : implied.get(member);
  }

  // An enum recording no number is numbered from 0 in declaration order.
  private Integer enumNumber(Schema.EnumValue value, List<Schema.EnumValue> values) {
    if (policy != IdentityPolicy.PROTOBUF) {
      return null;
    }
    boolean recordsAny = values.stream().anyMatch(v -> v.getEnumNumber() != null);
    return recordsAny ? value.getEnumNumber() : values.indexOf(value);
  }

  private static boolean isUnion(Schema schema) {
    return schema != null && schema.getType() == Schema.Type.UNION;
  }

  private static <T> List<T> sorted(List<T> members, Function<T, String> name) {
    List<T> sorted = new ArrayList<>(members);
    sorted.sort(Comparator.comparing(name, Comparator.nullsFirst(Comparator.naturalOrder())));
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
