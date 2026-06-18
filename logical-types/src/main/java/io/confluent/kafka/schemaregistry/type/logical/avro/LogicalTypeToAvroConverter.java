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
    final Schema schema = logicalType.getRootSchema();
    Set<String> orphans = logicalType.findOrphanNamedTypes();
    if (!orphans.isEmpty()) {
      log.warn("Avro encoding drops named type(s) not reachable from the registered "
          + "root: {}. Reference them from the root or REGISTER one of them instead.",
          orphans);
    }
    FromLogicalContext<org.apache.avro.Schema> ctx =
        new FromLogicalContext<>(logicalType, version);

    // Pre-populate cache with all named types (records and enums) from each
    // external schema, so any nested type is reachable as a NAMED_TYPE_REF.
    // Visited set is shared across externals: when external A transitively
    // references external B, B's types appear inside both parsed schemas, but
    // should only be registered once.
    Set<String> visited = new HashSet<>();
    for (Map.Entry<String, String> entry :
            logicalType.getResolvedReferences().entrySet()) {
      AvroSchema parsed = new AvroSchema(entry.getValue(),
          logicalType.getReferences(), logicalType.getResolvedReferences(),
          null, null, null, false);
      collectNamedSchemas(parsed.rawSchema(), visited, ctx);
    }

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
        avroMembers.add(fromLogicalTypeIgnoreNullable(
            branch.getSchema(), branch.getName(), ctx));
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

      return applyMetadata(avroSchema, metadataProps, logicalType, ctx.isV1());
    }
    org.apache.avro.Schema notNullSchema =
        fromLogicalTypeIgnoreNullable(schema, rowName, ctx);
    org.apache.avro.Schema result = schema.isNullable()
        ? nullableSchema(notNullSchema) : notNullSchema;
    return applyMetadata(result, metadataProps, logicalType, ctx.isV1());
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
      case BINARY:
        return SchemaBuilder.fixed(rowName).size(schema.getLength());
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
        if (ctx.isV1()) {
          throw new ValidationException("VARIANT not supported in V1 emission mode");
        }
        return new VariantConversion().getRecommendedSchema();
      case NAMED_TYPE_REF: {
        String name = schema.getQualifiedName();
        if (ctx.hasNamedType(name)) {
          if (!ctx.isAlreadyConverted(name)) {
            Schema typeDef = ctx.getNamedType(name);
            if (typeDef.getType() == Schema.Type.STRUCT) {
              // Use the placeholder pattern so a recursive self-reference can
              // resolve via the cache before the body is fully built.
              buildNamedRecord(name, typeDef, ctx);
            } else {
              org.apache.avro.Schema converted =
                  fromLogicalTypeIgnoreNullable(typeDef, name, ctx);
              ctx.putConverted(name, converted);
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
        org.apache.avro.Schema recordSchema = org.apache.avro.Schema.createRecord(
            rowName, schema.getDoc(), null, false);
        List<org.apache.avro.Schema.Field> avroFields = new ArrayList<>();
        for (Field field : schema.getFields()) {
          String fieldName = field.getName();
          if (!validateName(fieldName)) {
            throw getInvalidFieldNameException(fieldName);
          }
          Schema fieldType = field.getSchema();
          org.apache.avro.Schema notNullSchema = fromLogicalTypeIgnoreNullable(
              fieldType, rowName + "_" + fieldName, ctx);
          boolean hasNonNullDefault =
              field.hasDefaultValue() && field.getDefaultValue() != null;
          org.apache.avro.Schema fieldAvroSchema = maybeMakeNullable(
              fieldType, notNullSchema, hasNonNullDefault);
          Object defaultVal = computeAvroDefault(fieldType, field, ctx);
          avroFields.add(new org.apache.avro.Schema.Field(
              fieldName, fieldAvroSchema, field.getDoc(), defaultVal));
        }
        recordSchema.setFields(avroFields);
        // Add schema-level tags and params as record props. Mark as anonymous:
        // case STRUCT is only reached for inline LT structs (named types go
        // through buildNamedRecord, not this case).
        if (!ctx.isV1()) {
          addSchemaTags(recordSchema, schema);
          addSchemaParams(recordSchema, schema, true);
          addSchemaRules(recordSchema, schema);
          // Add field-level and union branch metadata as field props
          for (Field field : schema.getFields()) {
            org.apache.avro.Schema.Field avroField = recordSchema.getField(field.getName());
            if (!field.getTags().isEmpty()) {
              avroField.addProp("confluent:tags", field.getTags());
            }
            if (!field.getParams().isEmpty()) {
              avroField.addProp("confluent:params", field.getParams());
            }
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
        org.apache.avro.Schema enumSchema =
            SchemaBuilder.enumeration(rowName).doc(schema.getDoc())
                .symbols(symbols.toArray(new String[0]));
        if (!ctx.isV1()) {
          addSchemaTags(enumSchema, schema);
          // Anonymous unless this enum was reached as a named-type reference
          // (rowName equals the qualified name of a registered named type).
          addSchemaParams(enumSchema, schema, !ctx.hasNamedType(rowName));
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
          org.apache.avro.Schema memberSchema =
              fromLogicalTypeIgnoreNullable(
                  branch.getSchema(), branch.getName(), ctx);
          avroMembers.add(memberSchema);
        }
        return org.apache.avro.Schema.createUnion(avroMembers);
      }
      case MAP:
        return convertMap(schema, rowName, ctx);
      case ARRAY:
        return SchemaBuilder.builder()
            .array()
            .items(fromLogicalTypeRaw(
                schema.getElementType(), rowName, ctx));
      case MULTISET:
        return convertMultiset(schema, rowName, ctx);
      default:
        throw new ValidationException(
            "Unsupported to derive an Avro Schema for type " + schema);
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

  private static org.apache.avro.Schema connectCustomMap(
      String rowName, Schema keyType, Schema valueType,
      FromLogicalContext<org.apache.avro.Schema> ctx) {
    org.apache.avro.Schema keySchema = fromLogicalTypeRaw(keyType, rowName + "_key", ctx);
    org.apache.avro.Schema valueSchema = fromLogicalTypeRaw(valueType, rowName + "_value", ctx);
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
    org.apache.avro.Schema arr =
        LogicalMap.createMap(rowName + "_Entry", keySchema, valueSchema);
    arr.getElementType().addProp(
        CommonConstants.CONNECT_INTERNAL_TYPE_PROP, CommonConstants.MAP_ENTRY_TYPE_NAME);
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
    ctx.putConverted(name, placeholder);

    List<org.apache.avro.Schema.Field> avroFields = new ArrayList<>();
    for (Field field : typeDef.getFields()) {
      String fieldName = field.getName();
      if (!validateName(fieldName)) {
        throw getInvalidFieldNameException(fieldName);
      }
      Schema fieldType = field.getSchema();
      org.apache.avro.Schema notNullSchema = fromLogicalTypeIgnoreNullable(
          fieldType, name + "_" + fieldName, ctx);
      boolean hasNonNullDefault =
          field.hasDefaultValue() && field.getDefaultValue() != null;
      org.apache.avro.Schema fieldAvroSchema = maybeMakeNullable(
          fieldType, notNullSchema, hasNonNullDefault);
      Object defaultValue = computeAvroDefault(fieldType, field, ctx);
      org.apache.avro.Schema.Field avroField = new org.apache.avro.Schema.Field(
          fieldName, fieldAvroSchema, field.getDoc(), defaultValue);
      if (!ctx.isV1()) {
        if (!field.getTags().isEmpty()) {
          avroField.addProp("confluent:tags", field.getTags());
        }
        if (!field.getParams().isEmpty()) {
          avroField.addProp("confluent:params", field.getParams());
        }
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
      if (!ev.getParams().isEmpty()) {
        entry.put("params", ev.getParams());
      }
      enumMeta.add(entry);
    }
    if (enumMeta.stream().anyMatch(e -> e.size() > 1)) {
      avroSchema.addProp("confluent:enum", enumMeta);
    }
  }

  private static Map<String, Object> buildBranchMeta(UnionBranch branch) {
    Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("name", branch.getName());
    if (branch.getDoc() != null) {
      entry.put("doc", branch.getDoc());
    }
    if (!branch.getParams().isEmpty()) {
      entry.put("params", branch.getParams());
    }
    return entry;
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
    if (!schema.getParams().isEmpty()) {
      avroSchema.addProp("confluent:params", new LinkedHashMap<>(schema.getParams()));
    }
    if (isAnonymous) {
      avroSchema.addProp(CommonConstants.LOGICAL_ANONYMOUS_PROP, "true");
    }
  }

  /**
   * Recursively walks an Avro schema and registers every RECORD and ENUM
   * named type in the converter cache, keyed by full name. FIXED is skipped.
   */
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

  private static org.apache.avro.Schema nullableSchema(org.apache.avro.Schema schema) {
    return schema.isNullable()
        ? schema
        : org.apache.avro.Schema.createUnion(SchemaBuilder.builder().nullType(), schema);
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
    if (field.hasDefaultValue()) {
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
