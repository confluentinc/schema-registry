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

package io.confluent.kafka.schemaregistry.type.logical.statements;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** What depends on the schema format a type is registered in. */
public final class Formats {

  private Formats() {
  }

  /** The format a statement names, whatever its case. */
  public static SchemaType of(String name) {
    try {
      return SchemaType.valueOf(name.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new TypeRefusedException(
          "Unknown format '" + name + "'; must be one of AVRO, PROTOBUF, JSON");
    }
  }

  /** The format of a registered schema, by its schema type: Avro when it gives none. */
  public static SchemaType ofSchemaType(String schemaType) {
    return of(schemaType == null ? "AVRO" : schemaType);
  }

  /**
   * A request to register a type in its format, written as logical types DDL: the registry
   * converts it to the native schema it stores, so that it alone decides what that is.
   */
  public static RegisterSchemaRequest request(SchemaType format, LogicalType lt) {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchemaType(format.name());
    request.setSchema(LogicalTypeToDdlConverter.toDdl(lt));
    request.setReferences(lt.getReferences());
    return request;
  }

  /**
   * The params that pin the native names of the types the root struct's attribute generates,
   * as {@link #toNative} names them now; a rename keeps them by putting them on the attribute.
   */
  public static Map<String, Object> typeNameParams(SchemaType format, LogicalType lt,
      String subject, String attribute) {
    String rowName = LogicalTypeConversion.rowName(subject);
    switch (format) {
      case AVRO:
        return LogicalTypeToAvroConverter.typeNameParams(lt, rowName, attribute);
      case PROTOBUF:
        return LogicalTypeToProtoConverter.typeNameParams(lt, rowName, attribute);
      default:
        return Map.of();
    }
  }

  /**
   * As {@link #typeNameParams}, for the branches of a union attribute, by branch name.
   */
  public static Map<String, Map<String, Object>> branchTypeNameParams(SchemaType format,
      LogicalType lt, String subject, String attribute) {
    return format == SchemaType.PROTOBUF
        ? LogicalTypeToProtoConverter.branchTypeNameParams(lt,
            LogicalTypeConversion.rowName(subject), attribute)
        : Map.of();
  }

  /** Converts a logical type to a native schema of the format, naming an anonymous root. */
  public static ParsedSchema toNative(SchemaType format, LogicalType lt, String subject) {
    String rowName = LogicalTypeConversion.rowName(subject);
    try {
      switch (format) {
        case AVRO:
          return LogicalTypeToAvroConverter.fromLogicalType(lt, rowName);
        case PROTOBUF:
          return LogicalTypeToProtoConverter.fromLogicalType(lt, rowName);
        default:
          return LogicalTypeToJsonConverter.fromLogicalType(lt, rowName);
      }
    } catch (RuntimeException e) {
      throw new TypeRefusedException("Type " + subject + " cannot be represented as " + format
          + ": " + e.getMessage(), e);
    }
  }

  /**
   * Refuses what a type declares that its format cannot write: a DEFAULT outside Avro, a NOT
   * NULL UNION in Protobuf, a Protobuf field or enum number that is none, a Protobuf type that
   * is an ENUM, a JSON UNION whose branches are written as one JSON type.
   */
  public static void refuseUnwritable(SchemaType format, LogicalType lt) {
    refuseUnwritable(format, lt, KnownTypes.none());
  }

  // With the types it refers to, by the names it has for them.
  static void refuseUnwritable(SchemaType format, LogicalType lt, KnownTypes referred) {
    Schema root = lt.getRootSchema();
    if (root.getType() == Schema.Type.NAMED_TYPE_REF) {
      root = lt.getNamedTypes().getOrDefault(root.getQualifiedName(), root);
    }
    if (format == SchemaType.PROTOBUF && root.getType() == Schema.Type.ENUM) {
      throw new TypeRefusedException("A PROTOBUF type cannot be an ENUM: the root of a "
          + "Protobuf schema is a message");
    }
    if (lt.getName() != null) {
      refuseReservedParams(format, "type " + lt.getName(), root.getParams().keySet());
    }
    lt.getNamedTypes().forEach((name, s) ->
        refuseReservedParams(format, "type " + name, s.getParams().keySet()));
    KnownTypes named = KnownTypes.of(lt.getNamedTypes());
    named.adopt(referred, lt.getExternalTypes()::contains);
    refuseUnwritable(format, lt.getRootSchema(), named, null);
    lt.getNamedTypes().values().forEach(s -> refuseUnwritable(format, s, named, null));
  }

  /**
   * Refuses a DEFAULT the format would not apply to records without the attribute: proto3 has
   * no field defaults, and the JSON deserializer does not fill them in.
   */
  public static void refuseUnwritable(SchemaType format, Field f) {
    refuseUnwritable(format, f, KnownTypes.none());
  }

  // With the types its type may name: a name is judged as the type it is of, where known.
  static void refuseUnwritable(SchemaType format, Field f, KnownTypes named) {
    if (format != SchemaType.AVRO && f.hasDefaultValue() && !f.hasDerivedDefault()
        && f.getDefaultValue() != null) {
      throw new TypeRefusedException("Attribute " + f.getName() + " has a DEFAULT, which a "
          + format + " type cannot apply: a record without it reads "
          + (format == SchemaType.PROTOBUF ? "the zero value" : "NULL"));
    }
    refuseBadNumber(format, Schema.PROTOBUF_FIELD_NUMBER, "attribute " + f.getName(),
        f.getParams());
    refuseReservedParams(format, "attribute " + f.getName(), f.getParams().keySet());
    refuseUnwritable(format, f.getSchema(), named, "attribute " + f.getName());
  }

  /**
   * As {@link #refuseUnwritable(SchemaType, LogicalType)}, for one type and those in it.
   */
  public static void refuseUnwritable(SchemaType format, Schema schema) {
    refuseUnwritable(format, schema, KnownTypes.none(), null);
  }

  // within: the attribute the type is the type of, or is in the type of; null if none.
  static void refuseUnwritable(SchemaType format, Schema schema, KnownTypes named,
      String within) {
    refuseReservedParams(format, "a type", schema.getParams().keySet());
    switch (schema.getType()) {
      case STRUCT:
        schema.getFields().forEach(f -> refuseUnwritable(format, f, named));
        break;
      case UNION:
        if (format == SchemaType.PROTOBUF && !schema.isNullable()) {
          throw new TypeRefusedException("A UNION cannot be NOT NULL in a PROTOBUF type: "
              + "a Protobuf UNION is a oneof, which is always nullable");
        }
        if (format == SchemaType.JSON) {
          refuseOverlappingBranches(schema, named, within);
        }
        for (UnionBranch b : schema.getBranches()) {
          refuseBadNumber(format, Schema.PROTOBUF_FIELD_NUMBER, "branch " + b.getName(),
              b.getParams());
          refuseBadFieldName(format, b);
          refuseReservedParams(format, "branch " + b.getName(), b.getParams().keySet());
          refuseUnwritable(format, b.getSchema(), named, within);
        }
        break;
      case ENUM:
        for (EnumValue v : schema.getEnumValues()) {
          refuseBadNumber(format, Schema.PROTOBUF_ENUM_NUMBER, "value " + v.getSymbol(),
              v.getParams());
          refuseReservedParams(format, "value " + v.getSymbol(), v.getParams().keySet());
        }
        break;
      case ARRAY:
      case MULTISET:
        refuseUnwritable(format, schema.getElementType(), named, within);
        break;
      case MAP:
        refuseUnwritable(format, schema.getKeyType(), named, within);
        refuseUnwritable(format, schema.getValueType(), named, within);
        break;
      default:
        break;
    }
  }

  /**
   * Refuses a property under a name the Protobuf format keeps for itself: its reader takes
   * such a property for how a type is encoded, so it would change the type and not be kept.
   */
  static void refuseReservedParams(SchemaType format, String of, Collection<String> names) {
    if (format != SchemaType.PROTOBUF) {
      return;
    }
    for (String name : names) {
      if (ProtoToLogicalTypeConverter.isReservedParam(name)) {
        throw new TypeRefusedException("Property " + name + " of " + of + " cannot be given "
            + "in a PROTOBUF type: the format keeps the names flink.*, connect.*, logical.*, "
            + "precision and scale for how it encodes a type");
      }
    }
  }

  // A member's field name is a Protobuf identifier; whether another field has it is the
  // writer's to say, which names the fields around it.
  private static void refuseBadFieldName(SchemaType format, UnionBranch b) {
    Object name = b.getParams().get(Schema.PROTOBUF_FIELD_NAME);
    if (format != SchemaType.PROTOBUF || name == null) {
      return;
    }
    if (!name.toString().matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new TypeRefusedException("Property " + Schema.PROTOBUF_FIELD_NAME + " of branch "
          + b.getName() + " is not a valid field name: '" + name + "'");
    }
  }

  // Refuses a Protobuf field or enum number that is none, which the registry would refuse
  // without naming it.
  private static void refuseBadNumber(SchemaType format, String property, String of,
      Map<String, Object> params) {
    Object number = params.get(property);
    if (format != SchemaType.PROTOBUF || number == null) {
      return;
    }
    int given;
    try {
      given = Integer.parseInt(number.toString());
    } catch (NumberFormatException e) {
      throw new TypeRefusedException("Property " + property + " of " + of
          + " is not a valid number: '" + number + "'", e);
    }
    // A field's number is 1 or more; an enum value's may be 0 or less.
    if (given < 1 && property.equals(Schema.PROTOBUF_FIELD_NUMBER)) {
      throw new TypeRefusedException("Property " + property + " of " + of
          + " is not a field's number, which is 1 or more: '" + number + "'");
    }
  }

  // Refuses a JSON UNION a value of one of whose branches would match another too, which its
  // oneOf refuses: two written as one scalar JSON type, as BIGINT and DOUBLE both are
  // numbers; two written as arrays, as an empty one is either's; a map written as an object
  // beside a row or another such map; and two rows one of whose values is the other's.
  private static void refuseOverlappingBranches(Schema union, KnownTypes named,
      String within) {
    String[] both = overlappingBranches(union, named);
    if (both != null) {
      throw new TypeRefusedException("A UNION " + (within != null ? "of " + within + " " : "")
          + "in a JSON type cannot have branches " + both[0] + " and " + both[1]
          + OVERLAPPING);
    }
  }

  private static final String OVERLAPPING =
      ", which are written as one JSON type: a value of either would match both";

  // The first two branches of a union a value of one of which would match the other.
  private static String[] overlappingBranches(Schema union, KnownTypes named) {
    List<String[]> pairs = overlappingPairs(union, named);
    return pairs.isEmpty() ? null : pairs.get(0);
  }

  // Every two branches of a union a value of one of which would match the other.
  private static List<String[]> overlappingPairs(Schema union, KnownTypes named) {
    List<UnionBranch> branches = union.getBranches();
    List<List<Written>> written = new ArrayList<>();
    for (UnionBranch b : branches) {
      written.add(written(b.getSchema(), named));
    }
    List<String[]> pairs = new ArrayList<>();
    for (int j = 1; j < branches.size(); j++) {
      for (int i = 0; i < j; i++) {
        if (matchBoth(written.get(i), written.get(j), new HashSet<>())) {
          pairs.add(new String[] {branches.get(i).getName(), branches.get(j).getName()});
        }
      }
    }
    return pairs;
  }

  /**
   * A union in a type two of whose branches a value of one of which would match the other
   * with the types its names are of {@code after}, and did not with those {@code before}: a
   * reference moved to another version of its type gives a branch that type's attributes.
   * Told as the attribute it is of, or null, and the two branches; null if there is none.
   */
  static String[] newlyOverlapping(Schema type, KnownTypes before, KnownTypes after,
      String within) {
    switch (type.getType()) {
      case STRUCT:
        for (Field f : type.getFields()) {
          String[] found = newlyOverlapping(f.getSchema(), before, after, f.getName());
          if (found != null) {
            return found;
          }
        }
        return null;
      case UNION: {
        // Two that did before are left: a type stored so is altered as it was.
        Set<String> were = new HashSet<>();
        overlappingPairs(type, before).forEach(pair -> were.add(pair[0] + " " + pair[1]));
        for (String[] pair : overlappingPairs(type, after)) {
          if (!were.contains(pair[0] + " " + pair[1])) {
            return new String[] {within, pair[0], pair[1]};
          }
        }
        for (UnionBranch b : type.getBranches()) {
          String[] found = newlyOverlapping(b.getSchema(), before, after, within);
          if (found != null) {
            return found;
          }
        }
        return null;
      }
      case ARRAY:
      case MULTISET:
        return newlyOverlapping(type.getElementType(), before, after, within);
      case MAP:
        return newlyOverlapping(type.getValueType(), before, after, within);
      default:
        return null;
    }
  }

  // moved: the references that would be to another version, as told to a reader.
  static TypeRefusedException newlyOverlapping(String refused, String moved, String[] found) {
    return new TypeRefusedException(refused + ": " + moved + ", and with that version's "
        + "attributes branches " + found[1] + " and " + found[2] + " of the UNION"
        + (found[0] != null ? " of attribute " + found[0] : "")
        + " would be written as one JSON type: a value of either would match both");
  }

  /** Whether a type has a union and a name in it: what the types names are of matter to. */
  static boolean namesInAUnion(Schema type) {
    return has(type, Schema.Type.UNION) && has(type, Schema.Type.NAMED_TYPE_REF);
  }

  private static boolean has(Schema type, Schema.Type kind) {
    if (type.getType() == kind) {
      return true;
    }
    switch (type.getType()) {
      case STRUCT:
        return type.getFields().stream().anyMatch(f -> has(f.getSchema(), kind));
      case UNION:
        return type.getBranches().stream().anyMatch(b -> has(b.getSchema(), kind));
      case ARRAY:
      case MULTISET:
        return has(type.getElementType(), kind);
      case MAP:
        return has(type.getKeyType(), kind) || has(type.getValueType(), kind);
      default:
        return false;
    }
  }

  // A type a value may be written as, with the names to follow those in it among.
  private static final class Written {
    final Schema type;
    final KnownTypes named;

    Written(Schema type, KnownTypes named) {
      this.type = type;
      this.named = named;
    }
  }

  // The types a value of a type may be written as: of a union, those of its members, a value
  // of any of which is a value of it; of a name, those of the type it is of, where known.
  private static List<Written> written(Schema type, KnownTypes named) {
    List<Written> types = new ArrayList<>();
    collectWritten(type, named, Collections.newSetFromMap(new IdentityHashMap<>()), types);
    return types;
  }

  private static void collectWritten(Schema type, KnownTypes named, Set<Schema> within,
      List<Written> types) {
    if (type.getType() == Schema.Type.NAMED_TYPE_REF) {
      Schema of = named.get(type.getQualifiedName());
      if (of != null && within.add(of)) {
        collectWritten(of, named.within(of), within, types);
        within.remove(of);
      }
    } else if (type.getType() == Schema.Type.UNION) {
      type.getBranches().forEach(b -> collectWritten(b.getSchema(), named, within, types));
    } else {
      types.add(new Written(type, named));
    }
  }

  // comparing: the rows being compared, by the two types: a row that has an attribute of
  // its own type is come to again while it is compared.
  private static boolean matchBoth(List<Written> one, List<Written> other,
      Set<List<Integer>> comparing) {
    // A VARIANT is written as any JSON value, so a value of any other branch, a struct or
    // a collection as much as a scalar, matches it too.
    if (one.stream().anyMatch(t -> "any".equals(jsonKind(t.type)))
        || other.stream().anyMatch(t -> "any".equals(jsonKind(t.type)))) {
      return true;
    }
    return one.stream().anyMatch(a -> other.stream()
        .anyMatch(b -> matchBoth(a, b, comparing)));
  }

  private static boolean matchBoth(Written one, Written other, Set<List<Integer>> comparing) {
    Schema a = one.type;
    Schema b = other.type;
    if (a.getType() == Schema.Type.ENUM && b.getType() == Schema.Type.ENUM) {
      // Each is one of its symbols: a value of both is a symbol of both.
      Set<String> symbols = new HashSet<>();
      a.getEnumValues().forEach(v -> symbols.add(v.getSymbol()));
      return b.getEnumValues().stream().anyMatch(v -> symbols.contains(v.getSymbol()));
    }
    if (jsonKind(a) != null || jsonKind(b) != null) {
      return jsonKind(a) != null && jsonKind(a).equals(jsonKind(b));
    }
    if (isJsonArray(a) || isJsonArray(b)) {
      // An empty array is a value of any of them.
      return isJsonArray(a) && isJsonArray(b);
    }
    if (!isJsonObject(a) || !isJsonObject(b)) {
      return false;
    }
    boolean rowA = a.getType() == Schema.Type.STRUCT;
    boolean rowB = b.getType() == Schema.Type.STRUCT;
    if (!rowA && !rowB) {
      return true;
    }
    // Two come to while they are compared tell nothing of themselves: the attributes that
    // come after do.
    List<Integer> pair = List.of(System.identityHashCode(a), System.identityHashCode(b));
    if (!comparing.add(pair)) {
      return true;
    }
    try {
      if (rowA && rowB) {
        return !toldApart(one, other, comparing) && (readsAs(a, b) || readsAs(b, a));
      }
      // A map written as an object takes any property whose value is one of its values'.
      return !toldFromMap(rowA ? one : other, rowA ? other : one, comparing);
    } finally {
      comparing.remove(pair);
    }
  }

  // Whether a value of one row, written with each of its attributes, is a value of the
  // other: it has no attribute the other lacks, and each the other requires.
  private static boolean readsAs(Schema row, Schema other) {
    Set<String> names = new HashSet<>();
    row.getFields().forEach(f -> names.add(f.getName()));
    return other.getFields().stream().allMatch(f -> names.contains(f.getName())
        || f.getSchema().isNullable()) && names.stream().allMatch(n -> other.getField(n) != null);
  }

  // Two rows are told apart by an attribute both have whose value in the one is never one
  // in the other, and that one of them requires: a null, which both take where neither
  // does, tells nothing.
  private static boolean toldApart(Written row, Written other, Set<List<Integer>> comparing) {
    for (Field f : row.type.getFields()) {
      Field g = other.type.getField(f.getName());
      if (g != null && (!f.getSchema().isNullable() || !g.getSchema().isNullable())
          && neverMatch(written(f.getSchema(), row.named),
              written(g.getSchema(), other.named), comparing)) {
        return true;
      }
    }
    return false;
  }

  // A row is told from a map written as an object by an attribute it requires whose value
  // is never one of the map's.
  private static boolean toldFromMap(Written row, Written map, Set<List<Integer>> comparing) {
    List<Written> value = written(map.type.getType() == Schema.Type.MAP
        ? map.type.getValueType() : Schema.create(Schema.Type.INT), map.named);
    return row.type.getFields().stream().anyMatch(f -> !f.getSchema().isNullable()
        && neverMatch(written(f.getSchema(), row.named), value, comparing));
  }

  // Whether no value of one type is a value of the other; false where either is not known.
  private static boolean neverMatch(List<Written> one, List<Written> other,
      Set<List<Integer>> comparing) {
    return !one.isEmpty() && !other.isEmpty() && !matchBoth(one, other, comparing);
  }

  // A map or multiset is written as an object where its keys are character strings, and as
  // an array of its entries otherwise.
  private static boolean isJsonObject(Schema type) {
    return type.getType() == Schema.Type.STRUCT || isMapLike(type) && hasStringKeys(type);
  }

  private static boolean isJsonArray(Schema type) {
    return type.getType() == Schema.Type.ARRAY || isMapLike(type) && !hasStringKeys(type);
  }

  private static boolean isMapLike(Schema type) {
    return type.getType() == Schema.Type.MAP || type.getType() == Schema.Type.MULTISET;
  }

  private static boolean hasStringKeys(Schema type) {
    Schema.Type key = (type.getType() == Schema.Type.MAP ? type.getKeyType()
        : type.getElementType()).getType();
    return key == Schema.Type.VARCHAR || key == Schema.Type.CHAR;
  }

  // The JSON type a value of the type is written as, if a scalar or any; null otherwise.
  private static String jsonKind(Schema type) {
    switch (type.getType()) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        return "number";
      case CHAR:
      case VARCHAR:
      case ENUM:
      case BINARY:
      case VARBINARY:
        return "string";
      case BOOLEAN:
        return "boolean";
      case VARIANT:
        return "any";
      default:
        return null;
    }
  }
}
