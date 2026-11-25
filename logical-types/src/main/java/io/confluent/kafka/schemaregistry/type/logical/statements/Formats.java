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
import java.util.HashMap;
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
    refuseUnwritable(format, lt, Map.of());
  }

  /**
   * As {@link #refuseUnwritable(SchemaType, LogicalType)}, with the types it refers to, by
   * the names it has for them.
   */
  public static void refuseUnwritable(SchemaType format, LogicalType lt,
      Map<String, Schema> referred) {
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
    Map<String, Schema> named = new HashMap<>(referred);
    named.putAll(lt.getNamedTypes());
    refuseUnwritable(format, lt.getRootSchema(), named);
    lt.getNamedTypes().values().forEach(s -> refuseUnwritable(format, s, named));
  }

  /**
   * Refuses a DEFAULT the format would not apply to records without the attribute: proto3 has
   * no field defaults, and the JSON deserializer does not fill them in.
   */
  public static void refuseUnwritable(SchemaType format, Field f) {
    refuseUnwritable(format, f, Map.of());
  }

  /**
   * As {@link #refuseUnwritable(SchemaType, Field)}, with the types its type may name: a
   * name is judged as the type it is of where that is one of them.
   */
  public static void refuseUnwritable(SchemaType format, Field f, Map<String, Schema> named) {
    if (format != SchemaType.AVRO && f.hasDefaultValue() && !f.hasDerivedDefault()
        && f.getDefaultValue() != null) {
      throw new TypeRefusedException("Attribute " + f.getName() + " has a DEFAULT, which a "
          + format + " type cannot apply: a record without it reads "
          + (format == SchemaType.PROTOBUF ? "the zero value" : "NULL"));
    }
    refuseBadNumber(format, Schema.PROTOBUF_FIELD_NUMBER, "attribute " + f.getName(),
        f.getParams());
    refuseReservedParams(format, "attribute " + f.getName(), f.getParams().keySet());
    refuseUnwritable(format, f.getSchema(), named);
  }

  /**
   * As {@link #refuseUnwritable(SchemaType, LogicalType)}, for one type and those in it.
   */
  public static void refuseUnwritable(SchemaType format, Schema schema) {
    refuseUnwritable(format, schema, Map.of());
  }

  /**
   * As {@link #refuseUnwritable(SchemaType, Schema)}, with the types it may name.
   */
  public static void refuseUnwritable(SchemaType format, Schema schema,
      Map<String, Schema> named) {
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
          refuseOverlappingBranches(schema, named);
        }
        for (UnionBranch b : schema.getBranches()) {
          refuseBadNumber(format, Schema.PROTOBUF_FIELD_NUMBER, "branch " + b.getName(),
              b.getParams());
          refuseBadFieldName(format, b);
          refuseReservedParams(format, "branch " + b.getName(), b.getParams().keySet());
          refuseUnwritable(format, b.getSchema(), named);
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
        refuseUnwritable(format, schema.getElementType(), named);
        break;
      case MAP:
        refuseUnwritable(format, schema.getKeyType(), named);
        refuseUnwritable(format, schema.getValueType(), named);
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
    try {
      Integer.parseInt(number.toString());
    } catch (NumberFormatException e) {
      throw new TypeRefusedException("Property " + property + " of " + of
          + " is not a valid number: '" + number + "'", e);
    }
  }

  // Refuses a JSON UNION a value of one of whose branches would match another too, which its
  // oneOf refuses: two written as one scalar JSON type, as BIGINT and DOUBLE both are
  // numbers; two written as arrays, as an empty one is either's; a map written as an object
  // beside a row or another such map; and two rows one of whose values is the other's.
  private static void refuseOverlappingBranches(Schema union, Map<String, Schema> named) {
    List<UnionBranch> branches = union.getBranches();
    List<List<Schema>> written = new ArrayList<>();
    for (UnionBranch b : branches) {
      List<Schema> types = new ArrayList<>();
      collectWritten(b.getSchema(), named, new HashSet<>(), types);
      written.add(types);
    }
    for (int j = 1; j < branches.size(); j++) {
      for (int i = 0; i < j; i++) {
        if (matchBoth(written.get(i), written.get(j))) {
          throw overlapping(branches.get(i).getName(), branches.get(j).getName());
        }
      }
    }
  }

  // The types a value of a branch may be written as: of a branch that is a union, those of
  // its members, a value of any of which is a value of the branch; of one that is a name,
  // those of the type it is of, where that is known.
  private static void collectWritten(Schema type, Map<String, Schema> named,
      Set<String> within, List<Schema> types) {
    if (type.getType() == Schema.Type.NAMED_TYPE_REF) {
      String name = type.getQualifiedName();
      Schema of = named.get(name);
      if (of != null && within.add(name)) {
        collectWritten(of, named, within, types);
        within.remove(name);
      }
    } else if (type.getType() == Schema.Type.UNION) {
      type.getBranches().forEach(b -> collectWritten(b.getSchema(), named, within, types));
    } else {
      types.add(type);
    }
  }

  private static boolean matchBoth(List<Schema> one, List<Schema> other) {
    // A VARIANT is written as any JSON value, so a value of any other branch, a struct or
    // a collection as much as a scalar, matches it too.
    if (one.stream().anyMatch(t -> "any".equals(jsonKind(t)))
        || other.stream().anyMatch(t -> "any".equals(jsonKind(t)))) {
      return true;
    }
    return one.stream().anyMatch(a -> other.stream().anyMatch(b -> matchBoth(a, b)));
  }

  private static boolean matchBoth(Schema a, Schema b) {
    if (jsonKind(a) != null || jsonKind(b) != null) {
      return jsonKind(a) != null && jsonKind(a).equals(jsonKind(b));
    }
    if (isJsonArray(a) || isJsonArray(b)) {
      // An empty array is a value of any of them.
      return isJsonArray(a) && isJsonArray(b);
    }
    boolean rows = a.getType() == Schema.Type.STRUCT && b.getType() == Schema.Type.STRUCT;
    // A map written as an object takes any property, a row's among them.
    return rows ? readsAs(a, b) || readsAs(b, a) : isJsonObject(a) && isJsonObject(b);
  }

  // Whether a value of one row, written with each of its attributes, is a value of the
  // other: it has no attribute the other lacks, and each the other requires.
  private static boolean readsAs(Schema row, Schema other) {
    Set<String> names = new HashSet<>();
    row.getFields().forEach(f -> names.add(f.getName()));
    return other.getFields().stream().allMatch(f -> names.contains(f.getName())
        || f.getSchema().isNullable()) && names.stream().allMatch(n -> other.getField(n) != null);
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

  private static TypeRefusedException overlapping(String one, String other) {
    return new TypeRefusedException("A UNION in a JSON type cannot have branches " + one
        + " and " + other + ", which are written as one JSON type: a value of either would "
        + "match both");
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
