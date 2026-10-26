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

package io.confluent.kafka.schemaregistry.type.logical.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.avro.type.LogicalMap;
import io.confluent.avro.type.VariantConversion;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroTypeNames.Kind;
import io.confluent.kafka.schemaregistry.type.logical.common.FromLogicalContext;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BytesBuilder;
import org.apache.avro.SchemaBuilder.LongBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts logical type {@link Schema} to Avro {@link org.apache.avro.Schema}.
 */
public class LogicalTypeToAvroConverter {

  private static final Logger log = LoggerFactory.getLogger(LogicalTypeToAvroConverter.class);

  private static final int MAX_LENGTH = Integer.MAX_VALUE;

  private static final String CONFLUENT_UNION_PROP = "confluent:union";
  private static final String CONFLUENT_NAMESPACE_PROP = "confluent:namespace";
  private static final String CONFLUENT_EDITION_PROP = "confluent:edition";

  /**
   * Converts a logical type schema into an Avro schema using the canonical
   * (V2) emission. Prefer the explicit-version overload for new callers.
   */
  public static AvroSchema fromLogicalType(LogicalType logicalType, String rowName) {
    return fromLogicalType(logicalType, rowName, LogicalTypeVersion.V2);
  }

  /**
   * Converts a logical type schema into an Avro schema in the requested
   * emission edition (V1 = Flink-compatible; V2 = canonical LT).
   */
  public static AvroSchema fromLogicalType(
      LogicalType logicalType, String rowName, LogicalTypeVersion version) {
    if (!logicalType.getExternalImports().isEmpty()) {
      throw new ValidationException(
          "Cannot emit to Avro: LogicalType carries JSON-specific synthetic "
              + "external imports " + logicalType.getExternalImports().keySet()
              + ". Re-construct the LT against Avro-compatible externals.");
    }
    Set<String> orphans = logicalType.findOrphanNamedTypes();
    if (!orphans.isEmpty()) {
      log.warn("Avro encoding drops named type(s) not reachable from the registered "
          + "root: {}. Reference them from the root or REGISTER one of them instead.",
          orphans);
    }
    AvroTypeNames names = new AvroTypeNames();
    Emitted emitted = emit(logicalType, rowName, version, names);
    AvroTypeNames suffixing = names.forSuffixing();
    if (suffixing != null) {
      emitted = emit(logicalType, rowName, version, suffixing);
    }
    checkDistinctNames(emitted.schema, emitted.externalTypes);
    return applyMetadata(emitted.schema, emitted.metadataProps, logicalType,
        version == LogicalTypeVersion.V1);
  }

  /**
   * The params that pin the names of the types generated under the root struct's field
   * {@code fieldName}, as {@link #fromLogicalType(LogicalType, String)} names them now: put on the
   * field before renaming it, they keep the types' names, which Avro resolves records by. Empty
   * for a field that generates no named type.
   */
  public static Map<String, Object> typeNameParams(
      LogicalType logicalType, String rowName, String fieldName) {
    org.apache.avro.Schema root = fromLogicalType(logicalType, rowName).rawSchema();
    if (root.getType() == org.apache.avro.Schema.Type.UNION && root.getTypes().size() == 2) {
      root = root.getTypes().get(root.getTypes().get(0).getType()
          == org.apache.avro.Schema.Type.NULL ? 1 : 0);
    }
    org.apache.avro.Schema.Field field = root.getType() == org.apache.avro.Schema.Type.RECORD
        ? root.getField(fieldName) : null;
    if (field == null) {
      throw new ValidationException("The root struct has no field " + fieldName);
    }
    return AvroToLogicalTypeConverter.pinNameParams(root, field);
  }

  // The root schema and its metadata, before the checks and normalization of the whole.
  private static final class Emitted {
    private final org.apache.avro.Schema schema;
    private final Map<String, String> metadataProps;
    private final Map<String, org.apache.avro.Schema> externalTypes;

    private Emitted(org.apache.avro.Schema schema, Map<String, String> metadataProps,
        Map<String, org.apache.avro.Schema> externalTypes) {
      this.schema = schema;
      this.metadataProps = metadataProps;
      this.externalTypes = externalTypes;
    }
  }

  private static Emitted emit(LogicalType logicalType, String rowName,
      LogicalTypeVersion version, AvroTypeNames names) {
    final Schema schema = logicalType.getRootSchema();
    FromLogicalContext<org.apache.avro.Schema> ctx =
        new FromLogicalContext<>(logicalType, version);
    ctx.setTypeNames(names);

    // Pre-populate cache with all named types (records and enums) from each
    // external schema, so any nested type is reachable as a NAMED_TYPE_REF.
    // Visited set is shared across externals: when external A transitively
    // references external B, B's types appear inside both parsed schemas, but
    // should only be registered once.
    Set<String> visited = new HashSet<>();
    // Every named type of the externals, fixed ones too, which no generated type may take.
    Map<String, org.apache.avro.Schema> externalTypes = new HashMap<>();
    for (Map.Entry<String, String> entry :
            logicalType.getResolvedReferences().entrySet()) {
      AvroSchema parsed = new AvroSchema(entry.getValue(),
          logicalType.getReferences(), logicalType.getResolvedReferences(),
          null, null, null, false);
      collectNamedSchemas(parsed.rawSchema(), visited, ctx);
      collectFixedSchemas(parsed.rawSchema(), new HashSet<>(), externalTypes);
    }
    for (String name : visited) {
      externalTypes.put(name, ctx.getConverted(name));
    }
    names.reserve(externalTypes.keySet());

    Map<String, String> metadataProps = new LinkedHashMap<>();
    metadataProps.put(CONFLUENT_EDITION_PROP, version == LogicalTypeVersion.V1 ? "1" : "2");
    if (logicalType.getNamespace() != null && !ctx.isV1()) {
      metadataProps.put(CONFLUENT_NAMESPACE_PROP, logicalType.getNamespace());
    }

    if (schema.getType() == Schema.Type.UNION) {
      if (ctx.isV1()) {
        throw new ValidationException("UNION not supported in V1 emission mode");
      }
      // Special handling: build Avro union directly, prepend null if nullable
      List<org.apache.avro.Schema> avroMembers = new ArrayList<>();
      if (schema.isNullable()) {
        avroMembers.add(SchemaBuilder.builder().nullType());
      }
      List<Map<String, Object>> unionMeta = new ArrayList<>();
      for (UnionBranch branch : schema.getBranches()) {
        avroMembers.add(fromBranch(branch, rowName, ctx));
        unionMeta.add(buildBranchMeta(branch));
      }
      org.apache.avro.Schema avroSchema =
          org.apache.avro.Schema.createUnion(avroMembers);
      try {
        metadataProps.put(CONFLUENT_UNION_PROP,
            JacksonMapper.INSTANCE.writeValueAsString(unionMeta));
      } catch (JsonProcessingException e) {
        throw new ValidationException("Failed to serialize union metadata", e);
      }
      return new Emitted(avroSchema, metadataProps, externalTypes);
    }
    // A named root (bare STRUCT/ENUM carrying LogicalType.name) emits under its own name, taking
    // precedence over the caller's rowName. The reader unwraps only when the root's namespace
    // matches, so namespace + name reconstructs the original Avro full name.
    String rootName = rowName;
    if (logicalType.getName() != null) {
      rootName = logicalType.getNamespace() != null
          ? logicalType.getNamespace() + "." + logicalType.getName()
          : logicalType.getName();
    }
    org.apache.avro.Schema notNullSchema =
        fromLogicalTypeIgnoreNullable(schema, rootName, ctx);
    org.apache.avro.Schema result = schema.isNullable()
        ? nullableSchema(notNullSchema) : notNullSchema;
    return new Emitted(result, metadataProps, externalTypes);
  }

  /**
   * True when {@code rowName} is the name of the LogicalType's named root — i.e. the root was
   * unwrapped to a bare inline STRUCT/ENUM carrying {@link LogicalType#getName()}, and is being
   * emitted under that name. Such a root is a genuine named record/enum (not anonymous), so its
   * name survives the round-trip; anonymous roots ({@code getName() == null}) and nested inline
   * structs remain anonymous.
   */
  private static boolean isNamedRoot(
      String rowName, FromLogicalContext<org.apache.avro.Schema> ctx) {
    LogicalType lt = ctx.getLogicalType();
    if (lt.getName() == null) {
      return false;
    }
    String rootName = lt.getNamespace() != null
        ? lt.getNamespace() + "." + lt.getName()
        : lt.getName();
    return rootName.equals(rowName);
  }

  private static AvroSchema applyMetadata(
      org.apache.avro.Schema schema, Map<String, String> metadataProps,
      LogicalType logicalType, boolean isV1) {
    Metadata metadata = metadataProps.isEmpty() ? null : new Metadata(null, metadataProps, null);
    AvroSchema result = new AvroSchema(
        schema, logicalType.getReferences(), logicalType.getResolvedReferences(), null)
        .copy(metadata, null);
    // V1 must stay byte-equivalent to Flink's writer output, which does not
    // normalize. Normalize would sort props alphabetically — visible deviation
    // from Flink's insertion-order emission.
    return isV1 ? result : result.normalize();
  }

  /**
   * The name for a union branch's record, enum or fixed: the union's own name plus the branch's,
   * as a struct field's record is named after its parent. A branch name alone repeats across
   * unions, and Avro would make every type of that name the first one written.
   */
  private static String branchRowName(String unionRowName, UnionBranch branch) {
    return unionRowName + "_" + branch.getName();
  }

  private static org.apache.avro.Schema fromBranch(
      UnionBranch branch, String unionRowName, FromLogicalContext<org.apache.avro.Schema> ctx) {
    AvroTypeNames names = names(ctx);
    String base = names.enterBranch(branchRowName(unionRowName, branch), branch.getParams());
    try {
      return fromLogicalTypeIgnoreNullable(branch.getSchema(), base, ctx);
    } finally {
      names.exitBranch(branch.getParams());
    }
  }

  private static AvroTypeNames names(FromLogicalContext<org.apache.avro.Schema> ctx) {
    return (AvroTypeNames) ctx.getTypeNames();
  }

  // A field's Avro type, its types named under the field's base.
  private static org.apache.avro.Schema fromField(
      Field field, String rowName, FromLogicalContext<org.apache.avro.Schema> ctx) {
    AvroTypeNames names = names(ctx);
    String base = names.enterField(rowName, field.getName(), field.getParams());
    try {
      return fromLogicalTypeIgnoreNullable(field.getSchema(), base, ctx);
    } finally {
      names.exitField(field.getParams());
    }
  }

  private static org.apache.avro.Schema fromLogicalTypeRaw(
      Schema schema, String rowName, FromLogicalContext<org.apache.avro.Schema> ctx) {
    org.apache.avro.Schema notNullSchema = fromLogicalTypeIgnoreNullable(schema, rowName, ctx);
    return schema.isNullable() ? nullableSchema(notNullSchema) : notNullSchema;
  }

  private static org.apache.avro.Schema fromLogicalTypeIgnoreNullable(
      Schema schema, String rowName, FromLogicalContext<org.apache.avro.Schema> ctx) {
    switch (schema.getType()) {
      case BOOLEAN:
        return SchemaBuilder.builder().booleanType();
      case TINYINT: {
        org.apache.avro.Schema integer = SchemaBuilder.builder().intType();
        integer.addProp(CommonConstants.CONNECT_TYPE_PROP, "int8");
        return integer;
      }
      case SMALLINT: {
        org.apache.avro.Schema integer = SchemaBuilder.builder().intType();
        integer.addProp(CommonConstants.CONNECT_TYPE_PROP, "int16");
        return integer;
      }
      case INT:
        return SchemaBuilder.builder().intType();
      case BIGINT:
        return SchemaBuilder.builder().longType();
      case FLOAT:
        return SchemaBuilder.builder().floatType();
      case DOUBLE:
        return SchemaBuilder.builder().doubleType();
      case CHAR: {
        int length = schema.getLength();
        final org.apache.avro.Schema stringType = SchemaBuilder.builder().stringType();
        stringType.addProp(
            CommonConstants.FLINK_PROPERTY_VERSION,
            CommonConstants.FLINK_PROPERTY_CURRENT_VERSION);
        stringType.addProp(CommonConstants.FLINK_MIN_LENGTH, length);
        stringType.addProp(CommonConstants.FLINK_MAX_LENGTH, length);
        return stringType;
      }
      case VARCHAR: {
        int length = schema.getLength();
        final org.apache.avro.Schema stringType = SchemaBuilder.builder().stringType();
        if (length < MAX_LENGTH) {
          stringType.addProp(
              CommonConstants.FLINK_PROPERTY_VERSION,
              CommonConstants.FLINK_PROPERTY_CURRENT_VERSION);
          stringType.addProp(CommonConstants.FLINK_MAX_LENGTH, length);
        }
        return stringType;
      }
      case BINARY: {
        AvroTypeNames names = names(ctx);
        org.apache.avro.Schema fixed = SchemaBuilder.fixed(
            names.allocate(rowName, names.atRoot() ? Kind.FIXED : Kind.DERIVED))
            .size(schema.getLength());
        names.attach(fixed);
        return fixed;
      }
      case VARBINARY: {
        int length = schema.getLength();
        final BytesBuilder<org.apache.avro.Schema> bytesBuilder =
            SchemaBuilder.builder().bytesBuilder();
        if (length < MAX_LENGTH) {
          bytesBuilder
              .prop(CommonConstants.FLINK_PROPERTY_VERSION,
                  CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
              .prop(CommonConstants.FLINK_MAX_LENGTH, length);
        }
        return bytesBuilder.endBytes();
      }
      case TIMESTAMP_LTZ:
        if (ctx.isV1() && schema.getPrecision() > 6) {
          throw new ValidationException(
              "TIMESTAMP_LTZ precision > 6 not supported in V1 emission mode");
        }
        return convertLocalTimestamp(schema);
      case TIMESTAMP:
        if (ctx.isV1() && schema.getPrecision() > 6) {
          throw new ValidationException(
              "TIMESTAMP precision > 6 not supported in V1 emission mode");
        }
        return convertTimestamp(schema);
      case DATE:
        return LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
      case TIME:
        if (ctx.isV1() && schema.getPrecision() > 3) {
          throw new ValidationException(
              "TIME precision > 3 not supported in V1 emission mode");
        }
        return convertTime(schema);
      case DECIMAL:
        return LogicalTypes.decimal(schema.getPrecision(), schema.getScale())
            .addToSchema(SchemaBuilder.builder().bytesType());
      case VARIANT:
        // Emitted in both V1 and V2: Flink has a native VariantType (LogicalTypeRoot.VARIANT),
        // so VARIANT is not an LT-only construct.
        return new VariantConversion().getRecommendedSchema();
      case NAMED_TYPE_REF: {
        String name = schema.getQualifiedName();
        if (ctx.hasNamedType(name)) {
          if (!ctx.isAlreadyConverted(name)) {
            Schema typeDef = ctx.getNamedType(name);
            AvroTypeNames.Saved saved = names(ctx).enterNamedType();
            try {
              if (typeDef.getType() == Schema.Type.STRUCT) {
                // Use the placeholder pattern so a recursive self-reference can
                // resolve via the cache before the body is fully built.
                buildNamedRecord(name, typeDef, ctx);
              } else {
                org.apache.avro.Schema converted =
                    fromLogicalTypeIgnoreNullable(typeDef, name, ctx);
                ctx.putConverted(name, converted);
              }
            } finally {
              names(ctx).exitNamedType(saved);
            }
          }
          return ctx.getConverted(name);
        }
        if (ctx.isAlreadyConverted(name)) {
          return ctx.getConverted(name);
        }
        throw new ValidationException("Unknown named type reference: " + name);
      }
      case STRUCT: {
        // Build the record manually so we can control union ordering for
        // nullable fields (Avro requires the default's type to match the
        // first union branch).
        AvroTypeNames names = names(ctx);
        String recordName = names.allocate(rowName,
            names.atRoot() || isNamedRoot(rowName, ctx) ? Kind.FIXED : Kind.DERIVED);
        org.apache.avro.Schema recordSchema = org.apache.avro.Schema.createRecord(
            recordName, schema.getDoc(), null, false);
        names.attach(recordSchema);
        List<org.apache.avro.Schema.Field> avroFields = new ArrayList<>();
        for (Field field : schema.getFields()) {
          String fieldName = field.getName();
          if (!validateName(fieldName)) {
            throw getInvalidFieldNameException(fieldName);
          }
          Schema fieldType = field.getSchema();
          org.apache.avro.Schema notNullSchema = fromField(field, recordName, ctx);
          boolean hasNonNullDefault =
              field.hasDefaultValue() && !field.hasDerivedDefault()
              && field.getDefaultValue() != null;
          org.apache.avro.Schema fieldAvroSchema = maybeMakeNullable(
              fieldType, notNullSchema, hasNonNullDefault);
          Object defaultVal = computeAvroDefault(fieldType, field, ctx);
          avroFields.add(new org.apache.avro.Schema.Field(
              fieldName, fieldAvroSchema, field.getDoc(), defaultVal));
        }
        recordSchema.setFields(avroFields);
        // Add schema-level tags and params as record props. Inline structs are
        // anonymous, EXCEPT the named root: since Option A a named root is a bare
        // inline STRUCT carrying LogicalType.name, and it must stay a named record
        // (no logical.anonymous marker) so the reader recovers its name.
        if (!ctx.isV1()) {
          addSchemaTags(recordSchema, schema);
          addNamedTypeAliases(recordSchema, schema);
          addSchemaParams(recordSchema, schema, !isNamedRoot(rowName, ctx));
          addSchemaRules(recordSchema, schema);
          // Add field-level and union branch metadata as field props
          for (Field field : schema.getFields()) {
            org.apache.avro.Schema.Field avroField = recordSchema.getField(field.getName());
            if (!field.getTags().isEmpty()) {
              avroField.addProp("confluent:tags", field.getTags());
            }
            addFieldAliasesAndParams(avroField, field);
            addFieldRules(avroField, field);
            if (field.getSchema().getType() == Schema.Type.UNION) {
              List<Map<String, Object>> unionMeta = new ArrayList<>();
              for (UnionBranch branch : field.getSchema().getBranches()) {
                unionMeta.add(buildBranchMeta(branch));
              }
              avroField.addProp("confluent:union", unionMeta);
            }
          }
        }
        return recordSchema;
      }
      case ENUM: {
        List<String> symbols = schema.getEnumValues().stream()
            .map(EnumValue::getSymbol)
            .collect(Collectors.toList());
        AvroTypeNames names = names(ctx);
        org.apache.avro.Schema enumSchema = SchemaBuilder.enumeration(names.allocate(rowName,
                ctx.hasNamedType(rowName) || isNamedRoot(rowName, ctx) || names.atRoot()
                    ? Kind.FIXED : Kind.DERIVED))
            .doc(schema.getDoc())
            .symbols(symbols.toArray(new String[0]));
        names.attach(enumSchema);
        if (!ctx.isV1()) {
          addSchemaTags(enumSchema, schema);
          // Anonymous unless this enum was reached as a named-type reference
          // (rowName equals the qualified name of a registered named type), or it
          // is the named root (a bare inline ENUM carrying LogicalType.name).
          addNamedTypeAliases(enumSchema, schema);
          addSchemaParams(enumSchema, schema,
              !ctx.hasNamedType(rowName) && !isNamedRoot(rowName, ctx));
          addEnumValueMeta(enumSchema, schema);
        }
        return enumSchema;
      }
      case UNION: {
        if (ctx.isV1()) {
          throw new ValidationException("UNION not supported in V1 emission mode");
        }
        List<org.apache.avro.Schema> avroMembers = new ArrayList<>();
        for (UnionBranch branch : schema.getBranches()) {
          // Union members must not be nullable (Avro doesn't allow nested unions)
          org.apache.avro.Schema memberSchema = fromBranch(branch, rowName, ctx);
          avroMembers.add(memberSchema);
        }
        return org.apache.avro.Schema.createUnion(avroMembers);
      }
      case MAP:
        return convertMap(schema, rowName, ctx);
      case ARRAY: {
        org.apache.avro.Schema items = fromLogicalTypeRaw(schema.getElementType(), rowName, ctx);
        refuseMapEntryName(items, rowName);
        org.apache.avro.Schema array = SchemaBuilder.builder().array().items(items);
        addNestedUnionMeta(array, schema.getElementType(), ctx);
        return array;
      }
      case MULTISET:
        return convertMultiset(schema, rowName, ctx);
      default:
        throw new ValidationException(
            "Unsupported to derive an Avro Schema for type " + schema);
    }
  }

  // The reader takes an array of records with Connect's map entry name for a map's entries.
  private static void refuseMapEntryName(org.apache.avro.Schema items, String rowName) {
    if (items.getType() == org.apache.avro.Schema.Type.RECORD
        && CommonConstants.CONNECT_AVRO_NAMESPACE.equals(items.getNamespace())
        && CommonConstants.MAP_ENTRY_TYPE_NAME.equals(items.getName())) {
      throw new ValidationException("The elements of array " + rowName + " cannot be named "
          + items.getFullName() + ", which would read back as a map's entries");
    }
  }

  private static ValidationException getInvalidFieldNameException(String fieldName) {
    return new ValidationException(
        String.format(
            "Illegal field name for AVRO format "
                + "`%s`. AVRO expects field"
                + " names to start with [A-Za-z_] subsequently contain only [A-Za-z0-9_].",
            fieldName));
  }

  private static org.apache.avro.Schema convertMap(
      Schema schema, String rowName, FromLogicalContext<org.apache.avro.Schema> ctx) {
    final Schema keyType = schema.getKeyType();
    final Schema valueType = schema.getValueType();
    return convertMapLikeType(rowName, keyType, valueType, ctx);
  }

  private static org.apache.avro.Schema convertMultiset(
      Schema schema, String rowName, FromLogicalContext<org.apache.avro.Schema> ctx) {
    final Schema keyType = schema.getElementType();
    final Schema valueType = Schema.create(Schema.Type.INT).setNullable(false);
    final org.apache.avro.Schema avroSchema =
        convertMapLikeType(rowName, keyType, valueType, ctx);
    avroSchema.addProp(
        CommonConstants.FLINK_PROPERTY_VERSION,
        CommonConstants.FLINK_PROPERTY_CURRENT_VERSION);
    avroSchema.addProp(CommonConstants.FLINK_TYPE, CommonConstants.FLINK_MULTISET_TYPE);
    return avroSchema;
  }

  private static org.apache.avro.Schema convertMapLikeType(
      String rowName, Schema keyType, Schema valueType,
      FromLogicalContext<org.apache.avro.Schema> ctx) {
    boolean isCharacterString =
        keyType.getType() == Schema.Type.VARCHAR || keyType.getType() == Schema.Type.CHAR;
    if (isCharacterString) {
      org.apache.avro.Schema mapSchema =
          SchemaBuilder.builder().map().values(
              fromLogicalTypeRaw(valueType, rowName, ctx));
      addNestedUnionMeta(mapSchema, valueType, ctx);
      int keyLength = keyType.getLength();
      if (keyLength < Schema.MAX_LENGTH && !ctx.isV1()) {
        // V1 (Flink-compat) drops key-length / CHAR-vs-VARCHAR info — Flink
        // doesn't preserve it for string-keyed maps. logical.* keys are
        // emitted as top-level schema properties, not nested in confluent:params.
        mapSchema.addProp(CommonConstants.LOGICAL_KEY_LENGTH_PROP, keyLength);
        if (keyType.getType() == Schema.Type.CHAR) {
          mapSchema.addProp(CommonConstants.LOGICAL_KEY_TYPE_PROP, "CHAR");
        }
      }
      return mapSchema;
    } else {
      return connectCustomMap(rowName, keyType, valueType, ctx);
    }
  }

  private static org.apache.avro.Schema fromMapPart(Schema schema, String rowName, String suffix,
      FromLogicalContext<org.apache.avro.Schema> ctx) {
    AvroTypeNames names = names(ctx);
    String name = names.enterMapPart(rowName, suffix);
    try {
      return fromLogicalTypeRaw(schema, name, ctx);
    } finally {
      names.exitMapPart(rowName, suffix);
    }
  }

  private static org.apache.avro.Schema connectCustomMap(
      String rowName, Schema keyType, Schema valueType,
      FromLogicalContext<org.apache.avro.Schema> ctx) {
    org.apache.avro.Schema keySchema = fromMapPart(keyType, rowName, AvroTypeNames.KEY_SUFFIX, ctx);
    org.apache.avro.Schema valueSchema =
        fromMapPart(valueType, rowName, AvroTypeNames.VALUE_SUFFIX, ctx);
    if (ctx.isV1()) {
      // Flink-compat form: canonical record name `io.confluent.connect.avro.MapEntry`
      // with .noDefault() on key and value. Inherits Flink's known multi-map
      // collision risk by design (V1 = wire-equivalent to Flink output).
      return SchemaBuilder.array().items(
          SchemaBuilder.record(CommonConstants.MAP_ENTRY_TYPE_NAME)
              .namespace(CommonConstants.CONNECT_AVRO_NAMESPACE)
              .fields()
              .name(CommonConstants.KEY_FIELD).type(keySchema).noDefault()
              .name(CommonConstants.VALUE_FIELD).type(valueSchema).noDefault()
              .endRecord());
    }
    // V2: per-map unique name + LogicalMap on the array + connect.internal.type
    // prop on the entry record (latter for AvroData compat).
    AvroTypeNames names = names(ctx);
    String entryName = names.allocate(names.entryName(rowName),
        names.isPinnedEntry(rowName) ? Kind.PINNED : Kind.DERIVED);
    org.apache.avro.Schema arr = LogicalMap.createMap(entryName, keySchema, valueSchema);
    names.attach(arr.getElementType());
    arr.getElementType().addProp(
        CommonConstants.CONNECT_INTERNAL_TYPE_PROP, CommonConstants.MAP_ENTRY_TYPE_NAME);
    addNestedUnionMeta(arr.getElementType().getField(CommonConstants.KEY_FIELD), keyType, ctx);
    addNestedUnionMeta(arr.getElementType().getField(CommonConstants.VALUE_FIELD), valueType,
        ctx);
    return arr;
  }

  /**
   * Build a named Avro record using a placeholder so a recursive self-reference
   * can resolve via the cache while the body is still being constructed.
   */
  private static void buildNamedRecord(
      String name, Schema typeDef, FromLogicalContext<org.apache.avro.Schema> ctx) {
    int dot = name.lastIndexOf('.');
    String simpleName = dot >= 0 ? name.substring(dot + 1) : name;
    String namespace = dot >= 0 ? name.substring(0, dot) : null;
    if (!validateName(simpleName)) {
      throw getInvalidFieldNameException(simpleName);
    }
    org.apache.avro.Schema placeholder = org.apache.avro.Schema.createRecord(
        simpleName, typeDef.getDoc(), namespace, false);
    names(ctx).allocate(name, Kind.FIXED);
    names(ctx).attach(placeholder);
    ctx.putConverted(name, placeholder);

    List<org.apache.avro.Schema.Field> avroFields = new ArrayList<>();
    for (Field field : typeDef.getFields()) {
      String fieldName = field.getName();
      if (!validateName(fieldName)) {
        throw getInvalidFieldNameException(fieldName);
      }
      Schema fieldType = field.getSchema();
      org.apache.avro.Schema notNullSchema = fromField(field, name, ctx);
      boolean hasNonNullDefault =
          field.hasDefaultValue() && !field.hasDerivedDefault()
              && field.getDefaultValue() != null;
      org.apache.avro.Schema fieldAvroSchema = maybeMakeNullable(
          fieldType, notNullSchema, hasNonNullDefault);
      Object defaultValue = computeAvroDefault(fieldType, field, ctx);
      org.apache.avro.Schema.Field avroField = new org.apache.avro.Schema.Field(
          fieldName, fieldAvroSchema, field.getDoc(), defaultValue);
      if (!ctx.isV1()) {
        if (!field.getTags().isEmpty()) {
          avroField.addProp("confluent:tags", field.getTags());
        }
        addFieldAliasesAndParams(avroField, field);
        addFieldRules(avroField, field);
        if (fieldType.getType() == Schema.Type.UNION) {
          List<Map<String, Object>> unionMeta = new ArrayList<>();
          for (UnionBranch branch : fieldType.getBranches()) {
            unionMeta.add(buildBranchMeta(branch));
          }
          avroField.addProp("confluent:union", unionMeta);
        }
      }
      avroFields.add(avroField);
    }
    placeholder.setFields(avroFields);
    if (!ctx.isV1()) {
      addSchemaTags(placeholder, typeDef);
      addNamedTypeAliases(placeholder, typeDef);
      addSchemaParams(placeholder, typeDef);
      addSchemaRules(placeholder, typeDef);
    }
  }

  private static org.apache.avro.Schema convertTime(Schema schema) {
    final int precision = schema.getPrecision();
    if (precision < 0 || precision > 9) {
      throw new ValidationException(
          "TIME precision must be in [0, 9], got " + precision);
    }
    // Avro has no time-nanos; precision 7-9 stored as time-micros with overlay.
    final boolean useMillis = precision <= 3;
    final int naturalPrecision = useMillis ? 3 : 6;
    final org.apache.avro.LogicalType avroLogicalType =
        useMillis ? LogicalTypes.timeMillis() : LogicalTypes.timeMicros();
    if (useMillis) {
      if (precision == naturalPrecision) {
        return avroLogicalType.addToSchema(SchemaBuilder.builder().intType());
      }
      return avroLogicalType.addToSchema(
          SchemaBuilder.builder().intBuilder()
              .prop(CommonConstants.FLINK_PROPERTY_VERSION,
                  CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
              .prop(CommonConstants.FLINK_PRECISION, precision)
              .endInt());
    }
    LongBuilder<org.apache.avro.Schema> longBuilder = SchemaBuilder.builder().longBuilder();
    if (precision != naturalPrecision) {
      longBuilder = longBuilder
          .prop(CommonConstants.FLINK_PROPERTY_VERSION,
              CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
          .prop(CommonConstants.FLINK_PRECISION, precision);
    }
    return avroLogicalType.addToSchema(longBuilder.endLong());
  }

  private static org.apache.avro.Schema convertTimestamp(Schema schema) {
    return convertTimestampOfFlavor(schema, /*localTimestamp=*/ true);
  }

  private static org.apache.avro.Schema convertLocalTimestamp(Schema schema) {
    return convertTimestampOfFlavor(schema, /*localTimestamp=*/ false);
  }

  private static org.apache.avro.Schema convertTimestampOfFlavor(
      Schema schema, boolean localTimestamp) {
    final int precision = schema.getPrecision();
    if (precision < 0 || precision > 9) {
      throw new ValidationException(
          (localTimestamp ? "TIMESTAMP" : "TIMESTAMP_LTZ")
              + " precision must be in [0, 9], got " + precision);
    }
    final org.apache.avro.LogicalType avroLogicalType;
    final int naturalPrecision;
    if (precision <= 3) {
      avroLogicalType = localTimestamp
          ? LogicalTypes.localTimestampMillis() : LogicalTypes.timestampMillis();
      naturalPrecision = 3;
    } else if (precision <= 6) {
      avroLogicalType = localTimestamp
          ? LogicalTypes.localTimestampMicros() : LogicalTypes.timestampMicros();
      naturalPrecision = 6;
    } else {
      avroLogicalType = localTimestamp
          ? LogicalTypes.localTimestampNanos() : LogicalTypes.timestampNanos();
      naturalPrecision = 9;
    }
    LongBuilder<org.apache.avro.Schema> longBuilder = SchemaBuilder.builder().longBuilder();
    if (precision != naturalPrecision) {
      longBuilder = longBuilder
          .prop(CommonConstants.FLINK_PROPERTY_VERSION,
              CommonConstants.FLINK_PROPERTY_CURRENT_VERSION)
          .prop(CommonConstants.FLINK_PRECISION, precision);
    }
    return avroLogicalType.addToSchema(longBuilder.endLong());
  }

  private static void addSchemaTags(org.apache.avro.Schema avroSchema, Schema schema) {
    if (!schema.getTags().isEmpty()) {
      avroSchema.addProp("confluent:tags", schema.getTags());
    }
  }

  /**
   * Emit struct-level CHECK rules as a {@code confluent:rules} array property.
   * Each rule is a {@code {name, doc, expr, sql}} object — only non-null
   * fields are included so the wire form stays compact.
   */
  private static void addSchemaRules(org.apache.avro.Schema avroSchema, Schema schema) {
    addRulesProp(avroSchema::addProp, schema.getRules());
  }

  /**
   * Emit field-level CHECK rules as a {@code confluent:rules} array property
   * on the Avro field.
   */
  private static void addFieldRules(
      org.apache.avro.Schema.Field avroField, Field field) {
    addRulesProp(avroField::addProp, field.getRules());
  }

  /**
   * Emit the field aliases carried in {@link Schema#AVRO_ALIASES} through the
   * native Avro alias slot, and write the remaining user params as
   * {@code confluent:params}. All format-native params ({@code avro.*} /
   * {@code protobuf.*}) are stripped from the container: the Avro-owned aliases
   * because they are emitted natively, and any foreign {@code protobuf.*} because
   * a format-native param never belongs in a generic container.
   */
  private static void addFieldAliasesAndParams(
      org.apache.avro.Schema.Field avroField, Field field) {
    for (String alias : field.getAliases()) {
      avroField.addAlias(alias);
    }
    Map<String, Object> userParams = Schema.stripFormatNativeParams(field.getParams());
    if (!userParams.isEmpty()) {
      avroField.addProp("confluent:params", userParams);
    }
  }

  private static void addRulesProp(
      java.util.function.BiConsumer<String, Object> propSetter,
      List<io.confluent.kafka.schemaregistry.type.logical.Rule> rules) {
    if (rules.isEmpty()) {
      return;
    }
    List<Map<String, Object>> wire = new ArrayList<>(rules.size());
    for (io.confluent.kafka.schemaregistry.type.logical.Rule r : rules) {
      Map<String, Object> obj = new LinkedHashMap<>();
      if (r.getName() != null) {
        obj.put("name", r.getName());
      }
      if (r.getDoc() != null) {
        obj.put("doc", r.getDoc());
      }
      obj.put("expr", r.getExpr());
      obj.put("sql", r.getSql());
      wire.add(obj);
    }
    propSetter.accept("confluent:rules", wire);
  }

  private static void addEnumValueMeta(org.apache.avro.Schema avroSchema, Schema schema) {
    List<Map<String, Object>> enumMeta = new ArrayList<>();
    for (EnumValue ev : schema.getEnumValues()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("name", ev.getSymbol());
      if (ev.getDoc() != null) {
        entry.put("doc", ev.getDoc());
      }
      // Strip first: a format-native param alone must not manufacture a confluent:enum property.
      Map<String, Object> userParams = Schema.stripFormatNativeParams(ev.getParams());
      if (!userParams.isEmpty()) {
        entry.put("params", userParams);
      }
      enumMeta.add(entry);
    }
    if (enumMeta.stream().anyMatch(e -> e.size() > 1)) {
      avroSchema.addProp("confluent:enum", enumMeta);
    }
  }

  /**
   * A union in a collection has no field of its own for {@code confluent:union}, so the branch
   * names go on what holds it: the array, the map, or the entry record's key or value field.
   */
  private static void addNestedUnionMeta(
      JsonProperties holder, Schema type, FromLogicalContext<org.apache.avro.Schema> ctx) {
    if (ctx.isV1() || type.getType() != Schema.Type.UNION) {
      return;
    }
    List<Map<String, Object>> unionMeta = new ArrayList<>();
    for (UnionBranch branch : type.getBranches()) {
      unionMeta.add(buildBranchMeta(branch));
    }
    holder.addProp(CONFLUENT_UNION_PROP, unionMeta);
  }

  private static Map<String, Object> buildBranchMeta(UnionBranch branch) {
    Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("name", branch.getName());
    if (branch.getDoc() != null) {
      entry.put("doc", branch.getDoc());
    }
    // A oneof member's protobuf.field.number is format-native and has no Avro home, so keep it out
    // of confluent:union just as regular field numbers are kept out of confluent:params.
    Map<String, Object> branchParams = Schema.stripFormatNativeParams(branch.getParams());
    if (!branchParams.isEmpty()) {
      entry.put("params", branchParams);
    }
    return entry;
  }

  /**
   * Re-emit a named type's aliases through Avro's native {@code aliases} slot, the counterpart of
   * {@code readSchemaParams} injecting them from it. Fullnames, so a namespace change is harmless.
   */
  private static void addNamedTypeAliases(org.apache.avro.Schema avroSchema, Schema schema) {
    for (String alias : schema.getAliases()) {
      avroSchema.addAlias(alias);
    }
  }

  private static void addSchemaParams(org.apache.avro.Schema avroSchema, Schema schema) {
    addSchemaParams(avroSchema, schema, false);
  }

  /**
   * Sets the {@code confluent:params} prop with user-supplied params (if any), and
   * separately emits a top-level {@code logical.anonymous=true} marker when the
   * record/enum should be recovered by the reader as an inline LT STRUCT/ENUM
   * rather than as a named type.
   */
  private static void addSchemaParams(
      org.apache.avro.Schema avroSchema, Schema schema, boolean isAnonymous) {
    // Aliases ride Avro's native slot; strip so one alone can't manufacture confluent:params.
    Map<String, Object> userParams = Schema.stripFormatNativeParams(schema.getParams());
    if (!userParams.isEmpty()) {
      avroSchema.addProp("confluent:params", new LinkedHashMap<>(userParams));
    }
    if (isAnonymous) {
      avroSchema.addProp(CommonConstants.LOGICAL_ANONYMOUS_PROP, "true");
    }
  }

  /**
   * Recursively walks an Avro schema and registers every RECORD and ENUM
   * named type in the converter cache, keyed by full name. FIXED is skipped.
   */
  /**
   * Refuses two different named types with the same full name, e.g. a type a param pins and a
   * schema reference's type: Avro would write the second as a reference to the first, replacing
   * it. Identical JSON bodies are allowed; {@code equals} would miss docs and aliases.
   */
  private static void checkDistinctNames(
      org.apache.avro.Schema schema, Map<String, org.apache.avro.Schema> externalTypes) {
    Map<String, String> seenAt = new HashMap<>();
    externalTypes.keySet().forEach(name -> seenAt.put(name, null));
    // A reference's types are seeded, so the walk stops at one it uses rather than enter it.
    checkDistinctNames(schema, "", new HashMap<>(externalTypes), seenAt);
  }

  private static void checkDistinctNames(
      org.apache.avro.Schema schema, String path, Map<String, org.apache.avro.Schema> seen,
      Map<String, String> seenAt) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED: {
        String name = schema.getFullName();
        org.apache.avro.Schema prior = seen.putIfAbsent(name, schema);
        if (prior != null) {
          if (prior != schema && !prior.toString().equals(schema.toString())) {
            throw new ValidationException("Avro type name '" + name + "' is used for two "
                + "different types, at " + describePath(seenAt.get(name)) + " and "
                + describePath(path) + "; rename one of them");
          }
          return;
        }
        seenAt.put(name, path);
        if (schema.getType() == org.apache.avro.Schema.Type.RECORD) {
          for (org.apache.avro.Schema.Field field : schema.getFields()) {
            checkDistinctNames(field.schema(),
                path.isEmpty() ? field.name() : path + "." + field.name(), seen, seenAt);
          }
        }
        break;
      }
      case UNION:
        for (org.apache.avro.Schema member : schema.getTypes()) {
          checkDistinctNames(member, path, seen, seenAt);
        }
        break;
      case ARRAY:
        checkDistinctNames(schema.getElementType(), path, seen, seenAt);
        break;
      case MAP:
        checkDistinctNames(schema.getValueType(), path, seen, seenAt);
        break;
      default:
        break;
    }
  }

  private static String describePath(String path) {
    return path == null ? "a schema reference" : path.isEmpty() ? "the root" : path;
  }

  // The fixed types of an external, which no named type reference reaches.
  private static void collectFixedSchemas(org.apache.avro.Schema schema, Set<String> visited,
      Map<String, org.apache.avro.Schema> fixed) {
    switch (schema.getType()) {
      case RECORD:
        if (visited.add(schema.getFullName())) {
          for (org.apache.avro.Schema.Field field : schema.getFields()) {
            collectFixedSchemas(field.schema(), visited, fixed);
          }
        }
        break;
      case FIXED:
        fixed.putIfAbsent(schema.getFullName(), schema);
        break;
      case UNION:
        for (org.apache.avro.Schema member : schema.getTypes()) {
          collectFixedSchemas(member, visited, fixed);
        }
        break;
      case ARRAY:
        collectFixedSchemas(schema.getElementType(), visited, fixed);
        break;
      case MAP:
        collectFixedSchemas(schema.getValueType(), visited, fixed);
        break;
      default:
        break;
    }
  }

  private static void collectNamedSchemas(
      org.apache.avro.Schema schema,
      java.util.Set<String> visited,
      FromLogicalContext<org.apache.avro.Schema> ctx) {
    switch (schema.getType()) {
      case RECORD: {
        String fullName = schema.getFullName();
        if (visited.add(fullName)) {
          ctx.putConverted(fullName, schema);
          for (org.apache.avro.Schema.Field field : schema.getFields()) {
            collectNamedSchemas(field.schema(), visited, ctx);
          }
        }
        break;
      }
      case ENUM: {
        String fullName = schema.getFullName();
        if (visited.add(fullName)) {
          ctx.putConverted(fullName, schema);
        }
        break;
      }
      case UNION:
        for (org.apache.avro.Schema member : schema.getTypes()) {
          collectNamedSchemas(member, visited, ctx);
        }
        break;
      case ARRAY:
        collectNamedSchemas(schema.getElementType(), visited, ctx);
        break;
      case MAP:
        collectNamedSchemas(schema.getValueType(), visited, ctx);
        break;
      default:
        // primitives and FIXED — no named-type recursion
        break;
    }
  }

  // Avro forbids a union in a union, so a nullable UNION, as an array element or map value
  // say, gets null among its members, as a nullable field's does.
  private static org.apache.avro.Schema nullableSchema(org.apache.avro.Schema schema) {
    if (schema.isNullable()) {
      return schema;
    }
    if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
      List<org.apache.avro.Schema> members = new ArrayList<>();
      members.add(SchemaBuilder.builder().nullType());
      members.addAll(schema.getTypes());
      return org.apache.avro.Schema.createUnion(members);
    }
    return org.apache.avro.Schema.createUnion(SchemaBuilder.builder().nullType(), schema);
  }

  /**
   * Wrap the not-null Avro schema as a nullable union if the LogicalType is
   * nullable. Union ordering matches AvroData's convention: when the field has
   * a non-null default, the default's branch comes first ([T, null]); otherwise
   * null comes first ([null, T]). Required by Avro's rule that the default's
   * type matches the first union branch.
   */
  private static org.apache.avro.Schema maybeMakeNullable(
      Schema fieldType,
      org.apache.avro.Schema notNullSchema,
      boolean hasNonNullDefault) {
    if (!fieldType.isNullable()) {
      return notNullSchema;
    }
    org.apache.avro.Schema nullSchema = SchemaBuilder.builder().nullType();
    // Avro forbids nested unions. If notNullSchema is already a union (e.g. a
    // proper UNION<INT, STRING>), splice null into its members rather than
    // wrap, producing [null, int, string] instead of [null, [int, string]].
    if (notNullSchema.getType() == org.apache.avro.Schema.Type.UNION) {
      List<org.apache.avro.Schema> members = new ArrayList<>(notNullSchema.getTypes());
      if (hasNonNullDefault) {
        members.add(nullSchema);
      } else {
        members.add(0, nullSchema);
      }
      return org.apache.avro.Schema.createUnion(members);
    }
    if (hasNonNullDefault) {
      return org.apache.avro.Schema.createUnion(notNullSchema, nullSchema);
    }
    return org.apache.avro.Schema.createUnion(nullSchema, notNullSchema);
  }

  /**
   * Compute the Avro default-value object for a field:
   *  - explicit non-null default → encoded value
   *  - explicit null default     → JsonProperties.NULL_VALUE (emits "default": null)
   *  - no default + nullable + V1 → JsonProperties.NULL_VALUE (Flink-compat:
   *    Flink emits {@code default: null} for every nullable field without an
   *    explicit default)
   *  - no default                → null (no "default" key in the emitted JSON)
   */
  private static Object computeAvroDefault(
      Schema fieldType, Field field, FromLogicalContext<?> ctx) {
    if (field.hasDefaultValue() && !field.hasDerivedDefault()) {
      Object value = field.getDefaultValue();
      if (value == null) {
        return JsonProperties.NULL_VALUE;
      }
      return AvroDefaultValueConverter.toAvroData(fieldType, value);
    }
    if (ctx.isV1() && fieldType.isNullable()) {
      return JsonProperties.NULL_VALUE;
    }
    return null;
  }

  private static boolean validateName(String name) {
    if (name == null || name.isEmpty()) {
      return false;
    }
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      return false;
    }
    for (int i = 1; i < name.length(); i++) {
      char c = name.charAt(i);
      if (!(Character.isLetterOrDigit(c) || c == '_')) {
        return false;
      }
    }
    return true;
  }
}
