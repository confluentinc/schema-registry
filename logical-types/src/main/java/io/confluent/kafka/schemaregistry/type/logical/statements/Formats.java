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
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
    refuseUnwritable(format, f, named, null);
  }

  // under: the attribute the attribute is in the type of, by its path; null if none. An
  // attribute in another's type is told with that one before it, as e.x: one of its own
  // name may be the type's own.
  private static void refuseUnwritable(SchemaType format, Field f, KnownTypes named,
      String under) {
    String path = joined(under, f.getName());
    if (format != SchemaType.AVRO && f.hasDefaultValue() && !f.hasDerivedDefault()
        && f.getDefaultValue() != null) {
      throw new TypeRefusedException("Attribute " + path + " has a DEFAULT, which a "
          + format + " type cannot apply: a record without it reads "
          + (format == SchemaType.PROTOBUF ? "the zero value" : "NULL"));
    }
    // A union is a oneof, which has no number: one said for it is not left out, and is
    // told as that whatever the number, which no other would do for.
    if (format == SchemaType.PROTOBUF && f.getSchema().getType() == Schema.Type.UNION
        && f.getParams().containsKey(Schema.PROTOBUF_FIELD_NUMBER)) {
      throw new TypeRefusedException("Attribute " + path + " cannot have a Protobuf "
          + "field number: a UNION is a oneof, which has none; give it none");
    }
    refuseBadNumber(format, Schema.PROTOBUF_FIELD_NUMBER, "attribute " + path, f.getParams());
    refuseReservedParams(format, "attribute " + path, f.getParams().keySet());
    refuseUnwritable(format, f.getSchema(), named, path);
  }

  /**
   * As {@link #refuseUnwritable(SchemaType, LogicalType)}, for one type and those in it.
   */
  public static void refuseUnwritable(SchemaType format, Schema schema) {
    refuseUnwritable(format, schema, KnownTypes.none(), null);
  }

  // within: the attribute the type is the type of, or is in the type of, by its path; null
  // if none.
  static void refuseUnwritable(SchemaType format, Schema schema, KnownTypes named,
      String within) {
    refuseReservedParams(format, "a type", schema.getParams().keySet());
    String of = within != null ? " of attribute " + within : "";
    switch (schema.getType()) {
      case STRUCT:
        refuseSharedNumbers(format, schema, within);
        schema.getFields().forEach(f -> refuseUnwritable(format, f, named, within));
        break;
      case UNION:
        if (format == SchemaType.PROTOBUF && !schema.isNullable()) {
          throw new TypeRefusedException("A UNION" + of + " cannot be NOT NULL in a PROTOBUF "
              + "type: a Protobuf UNION is a oneof, which is always nullable");
        }
        if (format == SchemaType.JSON) {
          refuseOverlappingBranches(schema, named, within);
        }
        for (UnionBranch b : schema.getBranches()) {
          refuseBadNumber(format, Schema.PROTOBUF_FIELD_NUMBER, "branch " + b.getName() + of,
              b.getParams());
          refuseBadFieldName(format, b, of);
          refuseReservedParams(format, "branch " + b.getName() + of, b.getParams().keySet());
          // What is in a member's type is told with the member before it.
          refuseUnwritable(format, b.getSchema(), named,
              within == null ? null : joined(within, b.getName()));
        }
        break;
      case ENUM:
        for (EnumValue v : schema.getEnumValues()) {
          refuseBadNumber(format, Schema.PROTOBUF_ENUM_NUMBER, "value " + v.getSymbol() + of,
              v.getParams());
          refuseReservedParams(format, "value " + v.getSymbol() + of, v.getParams().keySet());
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
   * A name as a part of a path: one with a dot or a backtick in it is quoted as the DDL
   * quotes a name, so that the path of an attribute in it is no other attribute's.
   */
  static String segment(String name) {
    return name.indexOf('.') < 0 && name.indexOf('`') < 0 ? name
        : "`" + name.replace("`", "``") + "`";
  }

  /** The path of an attribute, or of a member, in the type of the attribute of a path. */
  static String joined(String under, String name) {
    return under == null ? segment(name) : under + "." + segment(name);
  }

  /** A value as a statement gives it, quoted as a literal is: a quote in it is doubled. */
  static String quoted(Object value) {
    return "'" + String.valueOf(value).replace("'", "''") + "'";
  }

  // A Protobuf message's fields, and the members of the unions among them, have each a
  // number of its own: one said for two of them is told here by their paths, and not by
  // the writer by a message's name no statement gave.
  private static void refuseSharedNumbers(SchemaType format, Schema row, String within) {
    if (format != SchemaType.PROTOBUF) {
      return;
    }
    Map<Integer, String> said = new HashMap<>();
    for (Field f : row.getFields()) {
      String path = joined(within, f.getName());
      if (f.getSchema().getType() != Schema.Type.UNION) {
        refuseSharedNumber(said, f.getParams(), "attribute " + path);
        continue;
      }
      for (UnionBranch b : f.getSchema().getBranches()) {
        refuseSharedNumber(said, b.getParams(), "branch " + b.getName() + " of attribute "
            + path);
      }
    }
  }

  private static void refuseSharedNumber(Map<Integer, String> said,
      Map<String, Object> params, String of) {
    Object number = params.get(Schema.PROTOBUF_FIELD_NUMBER);
    int given;
    try {
      given = number == null ? 0 : Integer.parseInt(number.toString());
    } catch (NumberFormatException e) {
      given = 0;
    }
    // One that is no number, or none a field can have, is refused as that: by its value,
    // however it is spelt.
    if (given < 1 || given > MAX_FIELD_NUMBER || given >= KEPT_FIRST && given <= KEPT_LAST) {
      return;
    }
    String has = said.putIfAbsent(given, of);
    if (has != null) {
      throw new TypeRefusedException(Character.toUpperCase(of.charAt(0)) + of.substring(1)
          + " cannot have the Protobuf field number " + given + ": " + has
          + " has it; give it another, or none");
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
  private static void refuseBadFieldName(SchemaType format, UnionBranch b, String of) {
    Object name = b.getParams().get(Schema.PROTOBUF_FIELD_NAME);
    if (format != SchemaType.PROTOBUF || name == null) {
      return;
    }
    if (!name.toString().matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new TypeRefusedException("Property " + Schema.PROTOBUF_FIELD_NAME + " of branch "
          + b.getName() + of + " is not a valid field name: " + quoted(name));
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
          + " is not a valid number: " + quoted(number), e);
    }
    // A field's number is 1 or more; an enum value's may be 0 or less.
    if (given < 1 && property.equals(Schema.PROTOBUF_FIELD_NUMBER)) {
      throw new TypeRefusedException("Property " + property + " of " + of
          + " is not a field's number, which is 1 or more: " + quoted(number));
    }
    // Nor is it above the last there is, or one of those Protobuf keeps: told here by the
    // attribute's path, and not by the writer by a message's name no statement gave.
    if (property.equals(Schema.PROTOBUF_FIELD_NUMBER)
        && (given > MAX_FIELD_NUMBER || given >= KEPT_FIRST && given <= KEPT_LAST)) {
      throw new TypeRefusedException(Character.toUpperCase(of.charAt(0)) + of.substring(1)
          + " cannot have the Protobuf field number " + given + ": "
          + (given > MAX_FIELD_NUMBER ? "a field's number is " + MAX_FIELD_NUMBER + " at most"
            : "Protobuf keeps it for itself") + "; give it another, or none");
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
      throw new TypeRefusedException("A UNION "
          + (within != null ? "of attribute " + within + " " : "")
          + "in a JSON type cannot have branches " + both[0] + " and " + both[1]
          + OVERLAPPING);
    }
  }

  private static final int MAX_FIELD_NUMBER = 536_870_911;
  private static final int KEPT_FIRST = 19_000;
  private static final int KEPT_LAST = 19_999;

  private static final String OVERLAPPING =
      ", which are written as one JSON type: a value of either would match both";

  // The first two branches of a union a value of one of which would match the other.
  private static String[] overlappingBranches(Schema union, KnownTypes named) {
    List<String[]> pairs = overlappingPairs(union, named, true);
    return pairs.isEmpty() ? null : pairs.get(0);
  }

  // Every two branches of a union a value of one of which would match the other; or the
  // first two alone, where no more is asked: a union of thousands of branches of one kind
  // is told by its first two.
  private static List<String[]> overlappingPairs(Schema union, KnownTypes named,
      boolean first) {
    List<UnionBranch> branches = union.getBranches();
    List<List<Written>> written = new ArrayList<>();
    for (UnionBranch b : branches) {
      written.add(written(b.getSchema(), named));
    }
    List<String[]> pairs = new ArrayList<>();
    Pairs comparing = new Pairs();
    for (int j = 1; j < branches.size(); j++) {
      for (int i = 0; i < j; i++) {
        // Rows met while the two are judged are judged at once, and the two again by what
        // those are found to be, until no more are met: what the two are is then told.
        boolean both;
        int met;
        do {
          met = comparing.met.size();
          both = matchBoth(written.get(i), written.get(j), comparing);
          comparing.settle();
        } while (comparing.met.size() > met);
        if (both) {
          pairs.add(new String[] {branches.get(i).getName(), branches.get(j).getName()});
          if (first) {
            return pairs;
          }
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
          String[] found = newlyOverlapping(f.getSchema(), before, after,
              joined(within, f.getName()));
          if (found != null) {
            return found;
          }
        }
        return null;
      case UNION: {
        // Two that did before are left: a type stored so is altered as it was.
        Set<String> were = new HashSet<>();
        overlappingPairs(type, before, false)
            .forEach(pair -> were.add(pair[0] + " " + pair[1]));
        for (String[] pair : overlappingPairs(type, after, false)) {
          if (!were.contains(pair[0] + " " + pair[1])) {
            return new String[] {within, pair[0], pair[1]};
          }
        }
        for (UnionBranch b : type.getBranches()) {
          String[] found = newlyOverlapping(b.getSchema(), before, after,
              within == null ? null : joined(within, b.getName()));
          if (found != null) {
            return found;
          }
        }
        return null;
      }
      case ARRAY:
      case MULTISET:
        return newlyOverlapping(type.getElementType(), before, after, within);
      case MAP: {
        // A key that is not a string is written as a value is, and may be a union.
        String[] found = newlyOverlapping(type.getKeyType(), before, after, within);
        return found != null ? found
            : newlyOverlapping(type.getValueType(), before, after, within);
      }
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

  // Two rows, or a row and a map written as an object, by what they are and not by what
  // they say.
  private static final class Pair {
    final Written one;
    final Written other;

    Pair(Written one, Written other) {
      this.one = one;
      this.other = other;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Pair && ((Pair) o).one.type == one.type
          && ((Pair) o).other.type == other.type;
    }

    @Override
    public int hashCode() {
      return 31 * System.identityHashCode(one.type) + System.identityHashCode(other.type);
    }
  }

  // The rows met while two branches are judged, two by two, and whether a value of the one
  // would match the other. Two are taken to match until an attribute tells them apart, as
  // two that name each other tell nothing of themselves; each is judged by what the others
  // are taken to be, and again when one of those is told apart. Rows that name each other
  // are so judged once each for each that changes, and not by every way they are come to.
  private static final class Pairs {
    final Map<Pair, Boolean> match = new HashMap<>();
    final List<Pair> met = new ArrayList<>();
    // The pairs judged by what a pair was taken to be: they are judged again once it is
    // told apart, and no others, however long the chain of them.
    private final Map<Pair, Set<Pair>> judgedBy = new HashMap<>();
    private Pair judging;
    private int settled;

    boolean of(Written one, Written other) {
      Pair pair = new Pair(one, other);
      if (judging != null) {
        judgedBy.computeIfAbsent(pair, p -> new HashSet<>()).add(judging);
      }
      Boolean known = match.putIfAbsent(pair, true);
      if (known == null) {
        met.add(pair);
        return true;
      }
      return known;
    }

    void settle() {
      Set<Pair> again = new LinkedHashSet<>();
      // Those met on the way are judged in their turn, and before any is judged again: one
      // judged by many is then judged again once for them all.
      while (!again.isEmpty() || settled < met.size()) {
        Pair pair;
        if (settled < met.size()) {
          pair = met.get(settled++);
        } else {
          pair = again.iterator().next();
          again.remove(pair);
        }
        if (!match.get(pair)) {
          continue;
        }
        judging = pair;
        boolean both = judge(pair.one, pair.other, this);
        judging = null;
        if (!both) {
          match.put(pair, false);
          again.addAll(judgedBy.getOrDefault(pair, Set.of()));
        }
      }
    }
  }

  // comparing: the rows met, and what two of them are taken to be.
  private static boolean matchBoth(List<Written> one, List<Written> other,
      Pairs comparing) {
    // A VARIANT is written as any JSON value, so a value of any other branch, a struct or
    // a collection as much as a scalar, matches it too.
    if (one.stream().anyMatch(t -> "any".equals(jsonKind(t.type)))
        || other.stream().anyMatch(t -> "any".equals(jsonKind(t.type)))) {
      return true;
    }
    // Every two are asked, and so met at once: asked until one matches, as many are met
    // in as many turns, each of which asks again all that came before.
    boolean any = false;
    for (Written a : one) {
      for (Written b : other) {
        if (matchBoth(a, b, comparing)) {
          any = true;
        }
      }
    }
    return any;
  }

  private static boolean matchBoth(Written one, Written other, Pairs comparing) {
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
    return comparing.of(one, other);
  }

  // Whether a value of one of two rows, or of a row and a map written as an object, would
  // match the other, by what the rows they name are taken to be.
  private static boolean judge(Written one, Written other, Pairs comparing) {
    boolean rowA = one.type.getType() == Schema.Type.STRUCT;
    boolean rowB = other.type.getType() == Schema.Type.STRUCT;
    // A map written as an object takes any property whose value is one of its values'.
    return rowA && rowB
        ? !toldApart(one, other, comparing)
            && (readsAs(one.type, other.type) || readsAs(other.type, one.type))
        : !toldFromMap(rowA ? one : other, rowA ? other : one, comparing);
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
  private static boolean toldApart(Written row, Written other, Pairs comparing) {
    for (Field f : row.type.getFields()) {
      Field g = other.type.getField(f.getName());
      if (g != null && (!takesNull(f.getSchema(), row.named)
            || !takesNull(g.getSchema(), other.named))
          && neverMatch(written(f.getSchema(), row.named),
              written(g.getSchema(), other.named), comparing)) {
        return true;
      }
    }
    return false;
  }

  // A row is told from a map written as an object by an attribute it requires whose value
  // is never one of the map's: a null is, where both take one. A multiset's is a count.
  private static boolean toldFromMap(Written row, Written map, Pairs comparing) {
    Schema of = map.type.getType() == Schema.Type.MAP
        ? map.type.getValueType() : Schema.create(Schema.Type.INT).setNullable(false);
    List<Written> value = written(of, map.named);
    boolean nulls = takesNull(of, map.named);
    return row.type.getFields().stream().anyMatch(f -> !f.getSchema().isNullable()
        && !(nulls && takesNull(f.getSchema(), row.named))
        && neverMatch(written(f.getSchema(), row.named), value, comparing));
  }

  // Whether a null is a value of an attribute's type: one that may be null, or a name of a
  // type referred to whose own root may be, whatever the attribute says of itself.
  private static boolean takesNull(Schema type, KnownTypes named) {
    return type.isNullable() || nullWithin(type, named, 0);
  }

  // Whether a null is a value of a type that does not say so of itself: a name of a type
  // referred to whose root may be null, or a union a member of which is such a name. A
  // union's members, and a type of the document's own, are written with no null of theirs.
  private static boolean nullWithin(Schema type, KnownTypes named, int depth) {
    if (depth > 16) {
      return true;
    }
    if (type.getType() == Schema.Type.UNION) {
      return type.getBranches().stream()
          .anyMatch(b -> nullWithin(b.getSchema(), named, depth + 1));
    }
    Schema of = type.getType() == Schema.Type.NAMED_TYPE_REF
        ? named.get(type.getQualifiedName()) : null;
    return of != null && (named.isReferred(of) && of.isNullable()
        || nullWithin(of, named.within(of), depth + 1));
  }

  // Whether no value of one type is a value of the other; false where either is not known.
  private static boolean neverMatch(List<Written> one, List<Written> other,
      Pairs comparing) {
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
