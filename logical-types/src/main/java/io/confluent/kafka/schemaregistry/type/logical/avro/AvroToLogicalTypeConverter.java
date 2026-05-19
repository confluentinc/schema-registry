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
import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.avro.type.LogicalMap;
import io.confluent.avro.type.VariantLogicalType;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.common.ToLogicalContext;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

import org.apache.avro.JsonProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.NULL;

/**
 * Converts Avro {@link org.apache.avro.Schema} to logical type {@link Schema}.
 *
 * <p><b>Default-value path emission.</b> ARRAY contributes index {@code 0} when descending into
 * its element type, so a default at {@code arr.element.fieldA} appears at path
 * {@code [arrFieldIndex, 0, fieldAIndex]}. All other container conventions follow
 * {@link io.confluent.kafka.schemaregistry.type.logical.LogicalType#getDefaultValues()}.
 */
public class AvroToLogicalTypeConverter {

  private static final String CONFLUENT_UNION_PROP = "confluent:union";
  private static final String CONFLUENT_NAMESPACE_PROP = "confluent:namespace";
  private static final int DEFAULT_DECIMAL_PRECISION = 10;
  private static final int DEFAULT_DECIMAL_SCALE = 0;
  private static final int MAX_LENGTH = Integer.MAX_VALUE;

  public static Schema toRootSchema(final AvroSchema avroSchema) {
    return toLogicalType(avroSchema).getRootSchema();
  }

  public static LogicalType toLogicalType(final AvroSchema avroSchema) {
    Map<String, Object> unionMetadata = extractUnionMetadata(avroSchema);
    final ToLogicalContext<org.apache.avro.Schema> ctx =
        new ToLogicalContext<>(avroSchema, unionMetadata);
    // Extend reference names with all RECORD/ENUM full names from each
    // resolved external schema, so nested external types are recognized.
    for (Map.Entry<String, String> entry :
        avroSchema.resolvedReferences().entrySet()) {
      AvroSchema parsedRef = new AvroSchema(entry.getValue(),
          avroSchema.references(), avroSchema.resolvedReferences(),
          null, null, null, false);
      collectExternalTypeNames(parsedRef.rawSchema(), new HashSet<>(), ctx);
    }
    final Schema schema =
        convertWithCycleDetection(avroSchema.rawSchema(), false, ctx, new ArrayList<>());
    return new LogicalType(
        extractNamespace(avroSchema),
        schema,
        ctx.getNamedTypes(),
        ctx.getExternalTypes(),
        avroSchema.references(),
        avroSchema.resolvedReferences(),
        ctx.getDefaultValues());
  }

  private static String extractNamespace(AvroSchema avroSchema) {
    Metadata metadata = avroSchema.metadata();
    if (metadata == null || metadata.getProperties() == null) {
      return null;
    }
    return metadata.getProperties().get(CONFLUENT_NAMESPACE_PROP);
  }

  private static Map<String, Object> extractUnionMetadata(AvroSchema avroSchema) {
    Metadata metadata = avroSchema.metadata();
    if (metadata == null || metadata.getProperties() == null) {
      return Map.of();
    }
    String unionMetaJson = metadata.getProperties().get(CONFLUENT_UNION_PROP);
    if (unionMetaJson == null) {
      return Map.of();
    }
    try {
      List<Map<String, Object>> unionMeta = JacksonMapper.INSTANCE.readValue(
          unionMetaJson, new TypeReference<List<Map<String, Object>>>() {});
      return Map.of(CONFLUENT_UNION_PROP, unionMeta);
    } catch (JsonProcessingException e) {
      throw new ValidationException("Failed to deserialize union metadata", e);
    }
  }

  private static Map<String, Object> extractUnionMetadata(
      org.apache.avro.Schema.Field field) {
    Map<String, Object> metadata = new LinkedHashMap<>();
    Object unionMeta = field.getObjectProp("confluent:union");
    if (unionMeta != null) {
      metadata.put("confluent:union", unionMeta);
    }
    return metadata;
  }

  /**
   * Recursively walks an Avro schema and registers every RECORD and ENUM
   * full name as an external reference. Cycle guard via visited set.
   */
  private static void collectExternalTypeNames(
      org.apache.avro.Schema schema,
      Set<String> visited,
      ToLogicalContext<org.apache.avro.Schema> ctx) {
    switch (schema.getType()) {
      case RECORD: {
        String fullName = schema.getFullName();
        if (visited.add(fullName)) {
          ctx.addExternalType(fullName);
          for (org.apache.avro.Schema.Field field : schema.getFields()) {
            collectExternalTypeNames(field.schema(), visited, ctx);
          }
        }
        break;
      }
      case ENUM: {
        String fullName = schema.getFullName();
        if (visited.add(fullName)) {
          ctx.addExternalType(fullName);
        }
        break;
      }
      case UNION:
        for (org.apache.avro.Schema member : schema.getTypes()) {
          collectExternalTypeNames(member, visited, ctx);
        }
        break;
      case ARRAY:
        collectExternalTypeNames(schema.getElementType(), visited, ctx);
        break;
      case MAP:
        collectExternalTypeNames(schema.getValueType(), visited, ctx);
        break;
      default:
        break;
    }
  }

  private static Schema convertWithCycleDetection(
      final org.apache.avro.Schema avroSchema,
      boolean isNullable,
      ToLogicalContext<org.apache.avro.Schema> ctx,
      List<Integer> indexPath) {

    // Short-circuit recursive back-references to a named type currently being
    // recovered (placeholder is in localNamedTypes); avoids the cycle-detection
    // throw and lets recursion terminate cleanly. Anonymous records can't
    // appear in a back-reference (synthesized names are field-path-unique), so
    // they're never in localNamedTypes anyway.
    if (avroSchema.getType() == org.apache.avro.Schema.Type.RECORD
        && !isAnonymous(avroSchema)
        && ctx.hasNamedType(avroSchema.getFullName())) {
      return Schema.createNamedTypeRef(avroSchema.getFullName())
          .setNullable(isNullable);
    }

    final boolean isNamedType =
        avroSchema.getType() == org.apache.avro.Schema.Type.UNION
            || avroSchema.getType() == org.apache.avro.Schema.Type.RECORD;
    if (isNamedType) {
      if (!ctx.addSeenSchema(avroSchema)) {
        throw new ValidationException(ctx.getCyclicSchemaErrorMessage());
      }
    }
    final Schema result = convert(avroSchema, isNullable, ctx, indexPath);
    if (isNamedType) {
      ctx.removeSeenSchema(avroSchema);
    }
    return result;
  }

  private static Schema convert(
      final org.apache.avro.Schema avroSchema,
      boolean isNullable,
      ToLogicalContext<org.apache.avro.Schema> ctx,
      List<Integer> indexPath) {
    final String connectType = avroSchema.getProp(CommonConstants.CONNECT_TYPE_PROP);
    final String logicalType = avroSchema.getProp(CommonConstants.AVRO_LOGICAL_TYPE_PROP);

    switch (avroSchema.getType()) {
      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN).setNullable(isNullable);

      case BYTES:
      case FIXED:
        if (CommonConstants.AVRO_LOGICAL_DECIMAL.equalsIgnoreCase(logicalType)) {
          final Object scaleNode =
              avroSchema.getObjectProp(CommonConstants.AVRO_LOGICAL_DECIMAL_SCALE_PROP);
          final int scale =
              scaleNode instanceof Number
                  ? ((Number) scaleNode).intValue()
                  : DEFAULT_DECIMAL_SCALE;
          Object precisionNode =
              avroSchema.getObjectProp(CommonConstants.AVRO_LOGICAL_DECIMAL_PRECISION_PROP);
          final int precision;
          if (precisionNode != null) {
            if (!(precisionNode instanceof Number)) {
              throw new ValidationException(
                  CommonConstants.AVRO_LOGICAL_DECIMAL_PRECISION_PROP
                      + " property must be a JSON Integer.");
            }
            precision = ((Number) precisionNode).intValue();
          } else {
            precision = DEFAULT_DECIMAL_PRECISION;
          }
          return Schema.createDecimal(precision, scale).setNullable(isNullable);
        } else if (avroSchema.getType() == org.apache.avro.Schema.Type.FIXED) {
          return Schema.createBinary(avroSchema.getFixedSize()).setNullable(isNullable);
        } else {
          final int maxLength =
              Optional.ofNullable(avroSchema.getObjectProp(CommonConstants.FLINK_MAX_LENGTH))
                  .map(i -> (Integer) i)
                  .orElse(MAX_LENGTH);
          if (maxLength == MAX_LENGTH) {
            return Schema.createBytes().setNullable(isNullable);
          }
          return Schema.createVarbinary(maxLength).setNullable(isNullable);
        }

      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE).setNullable(isNullable);

      case FLOAT:
        return Schema.create(Schema.Type.FLOAT).setNullable(isNullable);

      case INT:
        if (connectType == null && logicalType == null) {
          return Schema.create(Schema.Type.INT).setNullable(isNullable);
        } else if (logicalType != null) {
          if (CommonConstants.AVRO_LOGICAL_DATE.equalsIgnoreCase(logicalType)) {
            return Schema.create(Schema.Type.DATE).setNullable(isNullable);
          } else if (CommonConstants.AVRO_LOGICAL_TIME_MILLIS.equalsIgnoreCase(logicalType)) {
            return createTimeType(avroSchema, isNullable);
          } else {
            return Schema.create(Schema.Type.INT).setNullable(isNullable);
          }
        } else if (connectType.equalsIgnoreCase("int8")) {
          return Schema.create(Schema.Type.TINYINT).setNullable(isNullable);
        } else if (connectType.equalsIgnoreCase("int16")) {
          return Schema.create(Schema.Type.SMALLINT).setNullable(isNullable);
        } else {
          throw new ValidationException(
              "Connect type annotation for Avro int field is null");
        }

      case LONG:
        return createTimestampType(avroSchema, isNullable)
            .orElseGet(() -> Schema.create(Schema.Type.BIGINT).setNullable(isNullable));

      case STRING:
        final int maxLength =
            Optional.ofNullable(avroSchema.getObjectProp(CommonConstants.FLINK_MAX_LENGTH))
                .map(i -> (Integer) i)
                .orElse(MAX_LENGTH);
        return Optional.ofNullable(avroSchema.getObjectProp(CommonConstants.FLINK_MIN_LENGTH))
            .filter(minLength -> (int) minLength == maxLength)
            .map(minLength -> Schema.createChar((int) minLength).setNullable(isNullable))
            .orElseGet(() -> {
              if (maxLength == MAX_LENGTH) {
                return Schema.createString().setNullable(isNullable);
              }
              return Schema.createVarchar(maxLength).setNullable(isNullable);
            });

      case ENUM: {
        // Anything not explicitly marked anonymous is treated as a named type.
        // Externally-authored Avro (which never carries our marker) becomes a
        // proper NAMED_TYPE_REF in the recovered LogicalType — matching Avro's
        // own model where every record/enum has a stable identity.
        // Externals (pre-walked into the external set via collectExternalTypeNames)
        // get the SAME body-promotion treatment, so SrLtToFlinkShim and other
        // namedTypes consumers can resolve the body — `addExternalType` is still
        // called (in the pre-walk) so the LT's externalTypes set carries the
        // external-vs-local distinction.
        if (!isAnonymous(avroSchema)) {
          String name = avroSchema.getFullName();
          if (!ctx.hasNamedType(name)) {
            ctx.putNamedType(name, convertEnum(avroSchema));
          }
          return Schema.createNamedTypeRef(name).setNullable(isNullable);
        }
        List<String> symbols = avroSchema.getEnumSymbols();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> enumMeta =
            (List<Map<String, Object>>) avroSchema.getObjectProp("confluent:enum");
        List<EnumValue> enumValues = new ArrayList<>();
        for (int i = 0; i < symbols.size(); i++) {
          String doc = null;
          Map<String, Object> evParams = null;
          if (enumMeta != null && i < enumMeta.size()) {
            Map<String, Object> entry = enumMeta.get(i);
            doc = (String) entry.get("doc");
            evParams = (Map<String, Object>) entry.get("params");
          }
          enumValues.add(new EnumValue(symbols.get(i), doc, evParams));
        }
        Schema enumSchema = Schema.createEnum(enumValues).setNullable(isNullable);
        enumSchema.setDoc(avroSchema.getDoc());
        readSchemaTags(avroSchema, enumSchema);
        readSchemaParams(avroSchema, enumSchema);
        return enumSchema;
      }

      case ARRAY:
        org.apache.avro.Schema elemSchema = avroSchema.getElementType();
        if (isLogicalMap(avroSchema)) {
          if (elemSchema.getFields().size() != 2
              || elemSchema.getField(CommonConstants.KEY_FIELD) == null
              || elemSchema.getField(CommonConstants.VALUE_FIELD) == null) {
            throw new ValidationException(
                "Found map encoded as array of key-value pairs, but array "
                    + "elements do not match the expected format.");
          }
          final boolean isMultisetType =
              Objects.equals(
                  CommonConstants.FLINK_MULTISET_TYPE,
                  avroSchema.getProp(CommonConstants.FLINK_TYPE));
          final Schema keyType =
              convertWithCycleDetection(
                  elemSchema.getField(CommonConstants.KEY_FIELD).schema(),
                  false, ctx, appendToList(indexPath, 0));
          final Schema valueType =
              convertWithCycleDetection(
                  elemSchema.getField(CommonConstants.VALUE_FIELD).schema(),
                  false, ctx, appendToList(indexPath, 1));
          return createMapLikeType(isNullable, keyType, valueType, isMultisetType);
        } else {
          return Schema.createArray(
              convertWithCycleDetection(
                  avroSchema.getElementType(), false, ctx, appendToList(indexPath, 0)))
              .setNullable(isNullable);
        }

      case MAP:
        final boolean isMultisetType2 =
            Objects.equals(
                CommonConstants.FLINK_MULTISET_TYPE,
                avroSchema.getProp(CommonConstants.FLINK_TYPE));
        return createMapLikeType(
            isNullable,
            readMapKeyType(avroSchema),
            convertWithCycleDetection(
                avroSchema.getValueType(), false, ctx, appendToList(indexPath, 1)),
            isMultisetType2);

      case RECORD: {
        if (avroSchema.getLogicalType() != null
            && VariantLogicalType.NAME.equals(avroSchema.getLogicalType().getName())) {
          return Schema.create(Schema.Type.VARIANT).setNullable(isNullable);
        }
        // Anything not explicitly marked anonymous is a named record. Insert a
        // placeholder first so a recursive back-reference inside the body can
        // short-circuit via the namedTypes-membership check.
        // Externals (pre-walked into ctx via collectExternalTypeNames) get
        // body-promoted into namedTypes too — the LT's externalTypes set
        // carries the external-vs-local distinction so writers can re-emit
        // them as imports rather than inline.
        if (!isAnonymous(avroSchema)) {
          String name = avroSchema.getFullName();
          if (!ctx.hasNamedType(name)) {
            ctx.putNamedType(name, Schema.createStruct(new ArrayList<>()));
            ctx.putNamedType(name,
                convertRecord(avroSchema, ctx, indexPath));
          }
          return Schema.createNamedTypeRef(name).setNullable(isNullable);
        }
        final List<Field> fields = new ArrayList<>();
        int pos = 0;
        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
          final List<Integer> fieldIndex = appendToList(indexPath, field.pos());
          ctx.pushFieldPath(field.name());
          // Temporarily set field-level union branch metadata
          Map<String, Object> savedMetadata = ctx.getUnionMetadata();
          ctx.setUnionMetadata(extractUnionMetadata(field));
          final Schema fieldType =
              convertWithCycleDetection(
                  field.schema(), false, ctx, fieldIndex);
          ctx.setUnionMetadata(savedMetadata);
          ctx.popFieldPath();

          Object defaultValue = field.defaultVal();
          boolean hasDefault = defaultValue != null;
          if (defaultValue instanceof JsonProperties.Null) {
            defaultValue = null;
          } else if (hasDefault) {
            defaultValue = AvroDefaultValueConverter.toJavaData(fieldType, defaultValue);
            // Mirror Flink: only register an explicit non-null default in the
            // path-keyed map. JsonProperties.Null is intentionally skipped to
            // avoid round-trip ambiguity (can't distinguish "" from null from
            // the literal string "null" once serialized).
            ctx.putDefaultValue(fieldIndex, defaultValue);
          }

          List<String> fieldTags = readFieldTags(field);
          Map<String, Object> fieldParams = readFieldParams(field);
          List<io.confluent.kafka.schemaregistry.type.logical.Rule> fieldRules =
              readFieldRules(field);
          fields.add(new Field(field.name(), fieldType, pos++,
              defaultValue, hasDefault, field.doc(), fieldTags, fieldParams, fieldRules));
        }
        Schema structSchema = Schema.createStruct(fields).setNullable(isNullable);
        structSchema.setDoc(avroSchema.getDoc());
        readSchemaTags(avroSchema, structSchema);
        readSchemaParams(avroSchema, structSchema);
        readSchemaRules(avroSchema, structSchema);
        return structSchema;
      }

      case UNION: {
        List<org.apache.avro.Schema> unionTypes = avroSchema.getTypes();
        List<org.apache.avro.Schema> memberSchemas =
            unionTypes.stream()
                .filter(s -> s.getType() != NULL)
                .collect(Collectors.toList());
        boolean hasNull = unionTypes.size() != memberSchemas.size();

        // Nullable type: union of null and one other type
        if (memberSchemas.size() == 1) {
          return convertWithCycleDetection(
              memberSchemas.get(0), hasNull, ctx, indexPath);
        }

        // Proper union with multiple non-null types
        List<UnionMember> unionMembers = new ArrayList<>();
        int memberPos = 0;
        for (org.apache.avro.Schema memberSchema : memberSchemas) {
          final List<Integer> fieldIndex = appendToList(indexPath, memberPos++);
          Schema memberType =
              convertWithCycleDetection(
                  memberSchema, false, ctx, fieldIndex);
          unionMembers.add(new UnionMember(
              memberSchema.getName(),
              memberSchema.getFullName(),
              memberType));
        }

        // Use branch metadata if available, otherwise fall back to type-derived names
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> unionMeta =
            (List<Map<String, Object>>) ctx.getUnionMetadata().get("confluent:union");
        final Map<String, Long> simpleNameFreq =
            unionMembers.stream()
                .collect(Collectors.groupingBy(
                    UnionMember::getSimpleName, Collectors.counting()));
        List<UnionBranch> branches = new ArrayList<>();
        for (int i = 0; i < unionMembers.size(); i++) {
          UnionMember member = unionMembers.get(i);
          Map<String, Object> hint = unionMeta != null && i < unionMeta.size()
              ? unionMeta.get(i) : null;
          final String branchName;
          if (hint != null && hint.get("name") != null) {
            branchName = (String) hint.get("name");
          } else {
            branchName = simpleNameFreq.get(member.getSimpleName()) == 1
                ? member.getSimpleName()
                : member.getFullName();
          }
          String hintDoc = hint != null ? (String) hint.get("doc") : null;
          @SuppressWarnings("unchecked")
          Map<String, Object> hintParams = hint != null
              ? (Map<String, Object>) hint.get("params") : null;
          branches.add(new UnionBranch(branchName, member.getSchema(), hintDoc, hintParams));
        }
        return Schema.createUnion(branches).setNullable(hasNull);
      }

      case NULL:
        throw new ValidationException("Standalone NULL type is not supported");

      default:
        throw new ValidationException(
            "Couldn't translate unsupported Avro schema type "
                + avroSchema.getType().getName() + ".");
    }
  }

  private static Schema convertRecord(
      org.apache.avro.Schema avroSchema,
      ToLogicalContext<org.apache.avro.Schema> ctx,
      List<Integer> indexPath) {
    final List<Field> fields = new ArrayList<>();
    int pos = 0;
    for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
      final List<Integer> fieldIndex = appendToList(indexPath, field.pos());
      ctx.pushFieldPath(field.name());
      Map<String, Object> savedMetadata = ctx.getUnionMetadata();
      ctx.setUnionMetadata(extractUnionMetadata(field));
      final Schema fieldType =
          convertWithCycleDetection(field.schema(), false, ctx, fieldIndex);
      ctx.setUnionMetadata(savedMetadata);
      ctx.popFieldPath();

      Object defaultValue = field.defaultVal();
      boolean hasDefault = defaultValue != null;
      if (defaultValue instanceof JsonProperties.Null) {
        defaultValue = null;
      } else if (hasDefault) {
        defaultValue = AvroDefaultValueConverter.toJavaData(fieldType, defaultValue);
        ctx.putDefaultValue(fieldIndex, defaultValue);
      }

      List<String> fieldTags = readFieldTags(field);
      Map<String, Object> fieldParams = readFieldParams(field);
      List<io.confluent.kafka.schemaregistry.type.logical.Rule> fieldRules =
          readFieldRules(field);
      fields.add(new Field(field.name(), fieldType, pos++,
          defaultValue, hasDefault, field.doc(), fieldTags, fieldParams, fieldRules));
    }
    Schema structSchema = Schema.createStruct(fields).setNullable(false);
    structSchema.setDoc(avroSchema.getDoc());
    readSchemaTags(avroSchema, structSchema);
    readSchemaParams(avroSchema, structSchema);
    readSchemaRules(avroSchema, structSchema);
    return structSchema;
  }

  @SuppressWarnings("unchecked")
  private static Schema convertEnum(org.apache.avro.Schema avroSchema) {
    List<String> symbols = avroSchema.getEnumSymbols();
    List<Map<String, Object>> enumMeta =
        (List<Map<String, Object>>) avroSchema.getObjectProp("confluent:enum");
    List<EnumValue> enumValues = new ArrayList<>();
    for (int i = 0; i < symbols.size(); i++) {
      String doc = null;
      Map<String, Object> evParams = null;
      if (enumMeta != null && i < enumMeta.size()) {
        Map<String, Object> entry = enumMeta.get(i);
        doc = (String) entry.get("doc");
        evParams = (Map<String, Object>) entry.get("params");
      }
      enumValues.add(new EnumValue(symbols.get(i), doc, evParams));
    }
    Schema enumSchema = Schema.createEnum(enumValues).setNullable(false);
    enumSchema.setDoc(avroSchema.getDoc());
    readSchemaTags(avroSchema, enumSchema);
    readSchemaParams(avroSchema, enumSchema);
    return enumSchema;
  }

  private static <V> List<V> appendToList(final List<V> list, final V value) {
    final List<V> newList = new ArrayList<>(list);
    newList.add(value);
    return newList;
  }

  private static Schema createMapLikeType(
      boolean isNullable, Schema keyType, Schema valueType, boolean isMultisetType) {
    if (isMultisetType) {
      if (valueType.getType() != Schema.Type.INT) {
        throw new ValidationException(
            "Unexpected value type for a MULTISET type: " + valueType);
      }
      return Schema.createMultiset(keyType).setNullable(isNullable);
    } else {
      return Schema.createMap(keyType, valueType).setNullable(isNullable);
    }
  }

  private static Schema createTimeType(org.apache.avro.Schema avroSchema, boolean isNullable) {
    final int precision =
        Optional.ofNullable(avroSchema.getObjectProp(CommonConstants.FLINK_PRECISION))
            .map(i -> (Integer) i)
            .orElse(3);
    return Schema.createTime(precision).setNullable(isNullable);
  }

  private static Schema createTimeMicrosType(
      org.apache.avro.Schema avroSchema, boolean isNullable) {
    final int precision =
        Optional.ofNullable(avroSchema.getObjectProp(CommonConstants.FLINK_PRECISION))
            .map(i -> (Integer) i)
            .orElse(6);
    return Schema.createTime(precision).setNullable(isNullable);
  }

  private static Optional<Schema> createTimestampType(
      org.apache.avro.Schema avroSchema, boolean isNullable) {
    final Optional<Integer> propertyPrecision =
        Optional.ofNullable(avroSchema.getObjectProp(CommonConstants.FLINK_PRECISION))
            .map(i -> (Integer) i);
    final String logicalType = avroSchema.getProp(CommonConstants.AVRO_LOGICAL_TYPE_PROP);

    if (CommonConstants.AVRO_LOGICAL_TIME_MICROS.equalsIgnoreCase(logicalType)) {
      return Optional.of(createTimeMicrosType(avroSchema, isNullable));
    } else if (CommonConstants.AVRO_LOGICAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
      return Optional.of(Schema.createTimestampLtz(propertyPrecision.orElse(3))
          .setNullable(isNullable));
    } else if (CommonConstants.AVRO_LOGICAL_TIMESTAMP_MICROS.equalsIgnoreCase(logicalType)) {
      return Optional.of(Schema.createTimestampLtz(propertyPrecision.orElse(6))
          .setNullable(isNullable));
    } else if (CommonConstants.AVRO_LOGICAL_TIMESTAMP_NANOS.equalsIgnoreCase(logicalType)) {
      return Optional.of(Schema.createTimestampLtz(propertyPrecision.orElse(9))
          .setNullable(isNullable));
    } else if (CommonConstants.AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
      return Optional.of(Schema.createTimestamp(propertyPrecision.orElse(3))
          .setNullable(isNullable));
    } else if (CommonConstants.AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS.equalsIgnoreCase(logicalType)) {
      return Optional.of(Schema.createTimestamp(propertyPrecision.orElse(6))
          .setNullable(isNullable));
    } else if (CommonConstants.AVRO_LOGICAL_LOCAL_TIMESTAMP_NANOS.equalsIgnoreCase(logicalType)) {
      return Optional.of(Schema.createTimestamp(propertyPrecision.orElse(9))
          .setNullable(isNullable));
    }

    return Optional.empty();
  }

  private static boolean isLogicalMap(org.apache.avro.Schema arraySchema) {
    // Primary: LogicalMap on the array (the form LT-Avro emits).
    if (arraySchema.getLogicalType() instanceof LogicalMap) {
      return true;
    }
    // Legacy fallbacks for Flink-emitted and AvroData-emitted schemas.
    org.apache.avro.Schema elem = arraySchema.getElementType();
    if (elem.getType() != org.apache.avro.Schema.Type.RECORD) {
      return false;
    }
    // Canonical form: io.confluent.connect.avro.MapEntry. Used by the Flink
    // converter and by AvroData when the Connect schema has no name.
    if (CommonConstants.CONNECT_AVRO_NAMESPACE.equals(elem.getNamespace())
        && CommonConstants.MAP_ENTRY_TYPE_NAME.equals(elem.getName())) {
      return true;
    }
    // Named form: any record name + connect.internal.type=MapEntry prop.
    // Used by AvroData when the Connect schema has a user-given name.
    return CommonConstants.MAP_ENTRY_TYPE_NAME.equals(
        elem.getProp(CommonConstants.CONNECT_INTERNAL_TYPE_PROP));
  }

  private static Schema readMapKeyType(org.apache.avro.Schema avroSchema) {
    Object keyLength = avroSchema.getObjectProp(CommonConstants.LOGICAL_KEY_LENGTH_PROP);
    if (keyLength instanceof Integer) {
      String keyTypeName = (String) avroSchema.getObjectProp(
          CommonConstants.LOGICAL_KEY_TYPE_PROP);
      if ("CHAR".equals(keyTypeName)) {
        return Schema.createChar((int) keyLength).setNullable(false);
      }
      return Schema.createVarchar((int) keyLength).setNullable(false);
    }
    return Schema.createString().setNullable(false);
  }

  @SuppressWarnings("unchecked")
  private static List<String> readFieldTags(org.apache.avro.Schema.Field field) {
    Object tags = field.getObjectProp("confluent:tags");
    if (tags instanceof List) {
      List<String> tagList = new ArrayList<>();
      for (Object tag : (List<?>) tags) {
        tagList.add(tag.toString());
      }
      return tagList;
    }
    return Collections.emptyList();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> readFieldParams(org.apache.avro.Schema.Field field) {
    Object params = field.getObjectProp("confluent:params");
    if (params instanceof Map) {
      return new LinkedHashMap<>((Map<String, Object>) params);
    }
    return Collections.emptyMap();
  }

  @SuppressWarnings("unchecked")
  private static void readSchemaTags(org.apache.avro.Schema avroSchema, Schema schema) {
    Object tags = avroSchema.getObjectProp("confluent:tags");
    if (tags instanceof List) {
      List<String> tagList = new ArrayList<>();
      for (Object tag : (List<?>) tags) {
        tagList.add(tag.toString());
      }
      schema.setTags(tagList);
    }
  }

  @SuppressWarnings("unchecked")
  private static void readSchemaParams(org.apache.avro.Schema avroSchema, Schema schema) {
    Object params = avroSchema.getObjectProp("confluent:params");
    if (params instanceof Map) {
      Map<String, Object> userParams = (Map<String, Object>) params;
      if (!userParams.isEmpty()) {
        schema.setParams(new LinkedHashMap<>(userParams));
      }
    }
  }

  /**
   * Read CHECK rules from a {@code confluent:rules} array property on an
   * Avro field or schema. Returns empty list when the property is absent or
   * malformed (rule lists are forward-compatible — an unrecognized rule
   * should not fail the read).
   */
  @SuppressWarnings("unchecked")
  private static List<io.confluent.kafka.schemaregistry.type.logical.Rule> readRules(
      Object propValue) {
    if (!(propValue instanceof List)) {
      return Collections.emptyList();
    }
    List<io.confluent.kafka.schemaregistry.type.logical.Rule> out = new ArrayList<>();
    for (Object item : (List<?>) propValue) {
      if (!(item instanceof Map)) {
        continue;
      }
      Map<String, Object> obj = (Map<String, Object>) item;
      Object expr = obj.get("expr");
      Object sql = obj.get("sql");
      // Skip rules with null OR empty expr/sql. Empty strings round-trip
      // into nonsensical `CHECK ()` DDL. Matches the proto reader's
      // policy after the proto3-default-empty fix.
      if (expr == null || sql == null) {
        continue;
      }
      String exprStr = expr.toString();
      String sqlStr = sql.toString();
      if (exprStr.isEmpty() || sqlStr.isEmpty()) {
        continue;
      }
      out.add(new io.confluent.kafka.schemaregistry.type.logical.Rule(
          nullIfEmpty(obj.get("name")),
          nullIfEmpty(obj.get("doc")),
          exprStr,
          sqlStr));
    }
    return out;
  }

  private static String nullIfEmpty(Object o) {
    if (!(o instanceof String)) {
      return null;
    }
    String s = (String) o;
    return s.isEmpty() ? null : s;
  }

  private static List<io.confluent.kafka.schemaregistry.type.logical.Rule> readFieldRules(
      org.apache.avro.Schema.Field field) {
    return readRules(field.getObjectProp("confluent:rules"));
  }

  private static void readSchemaRules(
      org.apache.avro.Schema avroSchema, Schema schema) {
    List<io.confluent.kafka.schemaregistry.type.logical.Rule> rules =
        readRules(avroSchema.getObjectProp("confluent:rules"));
    if (!rules.isEmpty()) {
      schema.setRules(rules);
    }
  }

  private static boolean isAnonymous(org.apache.avro.Schema avroSchema) {
    return "true".equals(avroSchema.getObjectProp(CommonConstants.LOGICAL_ANONYMOUS_PROP));
  }

  private static final class UnionMember {
    private final String simpleName;
    private final String fullName;
    private final Schema schema;

    private UnionMember(String simpleName, String fullName, Schema schema) {
      this.simpleName = simpleName;
      this.fullName = fullName.replace('.', '_');
      this.schema = schema;
    }

    public String getSimpleName() {
      return simpleName;
    }

    public String getFullName() {
      return fullName;
    }

    public Schema getSchema() {
      return schema;
    }
  }
}
