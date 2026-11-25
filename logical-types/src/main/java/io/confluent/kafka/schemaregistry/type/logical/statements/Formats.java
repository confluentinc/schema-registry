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
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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
    Schema root = lt.getRootSchema();
    if (root.getType() == Schema.Type.NAMED_TYPE_REF) {
      root = lt.getNamedTypes().getOrDefault(root.getQualifiedName(), root);
    }
    if (format == SchemaType.PROTOBUF && root.getType() == Schema.Type.ENUM) {
      throw new TypeRefusedException("A PROTOBUF type cannot be an ENUM: the root of a "
          + "Protobuf schema is a message");
    }
    refuseUnwritable(format, lt.getRootSchema());
    lt.getNamedTypes().values().forEach(s -> refuseUnwritable(format, s));
  }

  /**
   * Refuses a DEFAULT the format would not apply to records without the attribute: proto3 has
   * no field defaults, and the JSON deserializer does not fill them in.
   */
  public static void refuseUnwritable(SchemaType format, Field f) {
    if (format != SchemaType.AVRO && f.hasDefaultValue() && !f.hasDerivedDefault()
        && f.getDefaultValue() != null) {
      throw new TypeRefusedException("Attribute " + f.getName() + " has a DEFAULT, which a "
          + format + " type cannot apply: a record without it reads "
          + (format == SchemaType.PROTOBUF ? "the zero value" : "NULL"));
    }
    refuseBadNumber(format, Schema.PROTOBUF_FIELD_NUMBER, "attribute " + f.getName(),
        f.getParams());
    refuseUnwritable(format, f.getSchema());
  }

  /**
   * As {@link #refuseUnwritable(SchemaType, LogicalType)}, for one type and those in it.
   */
  public static void refuseUnwritable(SchemaType format, Schema schema) {
    switch (schema.getType()) {
      case STRUCT:
        schema.getFields().forEach(f -> refuseUnwritable(format, f));
        break;
      case UNION:
        if (format == SchemaType.PROTOBUF && !schema.isNullable()) {
          throw new TypeRefusedException("A UNION cannot be NOT NULL in a PROTOBUF type: "
              + "a Protobuf UNION is a oneof, which is always nullable");
        }
        if (format == SchemaType.JSON) {
          refuseOverlappingBranches(schema);
        }
        for (UnionBranch b : schema.getBranches()) {
          refuseBadNumber(format, Schema.PROTOBUF_FIELD_NUMBER, "branch " + b.getName(),
              b.getParams());
          refuseBadFieldName(format, b);
          refuseUnwritable(format, b.getSchema());
        }
        break;
      case ENUM:
        for (EnumValue v : schema.getEnumValues()) {
          refuseBadNumber(format, Schema.PROTOBUF_ENUM_NUMBER, "value " + v.getSymbol(),
              v.getParams());
        }
        break;
      case ARRAY:
      case MULTISET:
        refuseUnwritable(format, schema.getElementType());
        break;
      case MAP:
        refuseUnwritable(format, schema.getKeyType());
        refuseUnwritable(format, schema.getValueType());
        break;
      default:
        break;
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

  // Refuses a JSON UNION two of whose branches are written as one JSON type, as BIGINT and
  // DOUBLE both are numbers: a value of either would match both, which its oneOf refuses.
  private static void refuseOverlappingBranches(Schema union) {
    Map<String, String> kinds = new HashMap<>();
    String first = null;
    String variant = null;
    for (UnionBranch b : union.getBranches()) {
      String kind = jsonKind(b.getSchema());
      // A VARIANT is written as any JSON value, so a value of any other branch, a struct or
      // a collection as much as a scalar, matches it too.
      String other = "any".equals(kind) ? first : variant;
      if (other != null) {
        throw overlapping(other, b.getName());
      }
      if (first == null) {
        first = b.getName();
      }
      if ("any".equals(kind)) {
        variant = b.getName();
      } else if (kind != null) {
        for (Map.Entry<String, String> e : kinds.entrySet()) {
          if (kind.equals(e.getValue())) {
            throw overlapping(e.getKey(), b.getName());
          }
        }
        kinds.put(b.getName(), kind);
      }
    }
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
