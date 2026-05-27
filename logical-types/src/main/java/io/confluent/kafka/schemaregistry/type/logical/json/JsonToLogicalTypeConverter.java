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

package io.confluent.kafka.schemaregistry.type.logical.json;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.common.ToLogicalContext;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.StringSchema;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_INDEX_PROP;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_PARAMETERS;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_BYTES;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_DATE;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_DECIMAL;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_DECIMAL_PRECISION;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_DECIMAL_SCALE;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_FLOAT32;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_FLOAT64;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_INT16;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_INT32;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_INT64;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_INT8;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_MAP;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_PROP;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_TIME;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.CONNECT_TYPE_TIMESTAMP;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_MAX_LENGTH;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_MIN_LENGTH;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_PRECISION;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_TYPE_MULTISET;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_TYPE_PROP;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_TYPE_TIMESTAMP;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.GENERALIZED_TYPE_UNION_PREFIX;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.KEY_FIELD;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.VALUE_FIELD;


/**
 * Converts JSON Schema to logical type {@link Schema}.
 *
 * <p><b>Default-value path emission.</b> ARRAY appends {@code [0]} for the element type
 * (matching the Avro convention); MAP appends {@code [0]} for key and {@code [1]} for value;
 * UNION appends the branch index; STRUCT appends the field position. See
 * {@link io.confluent.kafka.schemaregistry.type.logical.LogicalType#getDefaultValues()}.
 */
public class JsonToLogicalTypeConverter {

  private static final Logger LOG =
      LoggerFactory.getLogger(JsonToLogicalTypeConverter.class);

  /**
   * JSON Schema {@code default} keyword.
   */
  private static final String DEFAULT_KEYWORD = "default";

  private static final int MAX_LENGTH = Integer.MAX_VALUE;
  private static final int DEFAULT_DECIMAL_PRECISION = 38;
  private static final int DEFAULT_DECIMAL_SCALE = 18;

  public static Schema toRootSchema(final JsonSchema schema) {
    return toLogicalType(schema).getRootSchema();
  }

  public static LogicalType toLogicalType(final JsonSchema schema) {
    final ToLogicalContext<String> ctx = new ToLogicalContext<>(schema);
    // Recover named types from $defs (entries are keyed by qualified name).
    recoverNamedTypesFromDefs(schema, ctx);
    final Schema logicalType =
        convertWithCycleDetection(schema.rawSchema(), false, ctx,
            Collections.emptyList());
    Object ns = schema.rawSchema().getUnprocessedProperties()
        .get("confluent:namespace");
    return new LogicalType(
        ns instanceof String ? (String) ns : null,
        logicalType,
        ctx.getNamedTypes(),
        ctx.getExternalTypes(),
        ctx.getExternalImports(),
        schema.references(),
        schema.resolvedReferences(),
        ctx.getDefaultValues());
  }

  private static List<Integer> appendToList(final List<Integer> list, final int value) {
    List<Integer> result = new ArrayList<>(list);
    result.add(value);
    return result;
  }

  /**
   * Capture the JSON Schema {@code default} keyword on an object property and
   * record it in {@code ctx}'s path-keyed defaults map. Returns the converted
   * default value for use as the LT {@code Field.defaultValue} (or null when
   * no default applies).
   *
   * <p>Read order: {@code unprocessedProperties.get("default")} first (where
   * SchemaLoader-loaded schemas put it — the loader consumes the keyword for
   * its own ref-resolution pipeline), then the typed everit API (populated
   * when the schema was constructed via the builder, e.g. by our V1 writer).
   * Resolves {@link ReferenceSchema} chains first so a {@code default}
   * declared on a {@code $ref}'d schema is reachable.
   *
   * <p>Skips null defaults to avoid the "no default vs default null"
   * ambiguity (per Avro convention). everit represents JSON {@code null} as
   * {@link JSONObject#NULL}, a non-null Java sentinel — treat it the same as
   * Java null for the null-skip.
   *
   * <p>Converter failures (e.g. a non-ISO timestamp string for a TIMESTAMP
   * field) are logged and the default is dropped — failing the whole schema
   * walk would be a regression for in-the-wild schemas that previously had
   * malformed {@code default} keywords silently ignored. The log line is
   * deliberately structural (indexPath + type root + exception class) — no
   * field name, no value, no message text, since any of those may carry
   * customer data.
   *
   * <p>The path-keyed map drives downstream consumers (Flink shim,
   * schema-evolution checker) that want unambiguous typed defaults. Multi-
   * non-null UNION defaults are skipped from the path-keyed map — the value
   * can't be tagged with a branch, so propagating it would mislead consumers.
   * The returned {@link Field#getDefaultValue()} still carries it for DDL
   * round-trip / V1 writer purposes.
   */
  private static Object captureDefaultValue(
      final ToLogicalContext<String> ctx,
      final org.everit.json.schema.Schema fieldSchema,
      final Schema fieldType,
      final List<Integer> fieldIndex) {
    final org.everit.json.schema.Schema resolved = resolveReference(fieldSchema);
    final Object rawDefault = extractDefault(resolved);
    if (rawDefault == null || rawDefault == JSONObject.NULL) {
      return null;
    }
    Object defaultValue;
    try {
      defaultValue = JsonDefaultValueConverter.toJavaData(fieldType, rawDefault);
    } catch (RuntimeException e) {
      LOG.warn(
          "Skipping unconvertible JSON Schema default at field index path {} "
              + "(target type: {}, exception: {})",
          fieldIndex, fieldType.getType(), e.getClass().getSimpleName());
      return null;
    }
    if (defaultValue == null) {
      return null;
    }
    if (!isMultiNonNullUnion(fieldType)) {
      ctx.putDefaultValue(fieldIndex, defaultValue);
    }
    return defaultValue;
  }

  private static Object extractDefault(final org.everit.json.schema.Schema schema) {
    final Map<String, Object> unprocessed = schema.getUnprocessedProperties();
    if (unprocessed != null && unprocessed.containsKey(DEFAULT_KEYWORD)) {
      return unprocessed.get(DEFAULT_KEYWORD);
    }
    if (schema.hasDefaultValue()) {
      return schema.getDefaultValue();
    }
    return null;
  }

  /**
   * Unwrap chained {@link ReferenceSchema}s to reach the referred body. Used
   * before reading the {@code default} keyword so that defaults declared on a
   * {@code $ref}'d schema (rather than on the ref-using property itself) are
   * not missed.
   */
  private static org.everit.json.schema.Schema resolveReference(
      org.everit.json.schema.Schema schema) {
    org.everit.json.schema.Schema current = schema;
    while (current instanceof ReferenceSchema) {
      org.everit.json.schema.Schema referred =
          ((ReferenceSchema) current).getReferredSchema();
      if (referred == null) {
        return current;
      }
      current = referred;
    }
    return current;
  }

  /**
   * True iff {@code type} is a UNION with more than one (necessarily non-null)
   * branch. The singleton-collapse path in {@code convertCombinedSchema}
   * collapses single-branch oneOfs to the branch type, so any UNION that
   * survives that path carries ≥ 2 non-null branches — and is ambiguous for
   * a property-level default value.
   */
  private static boolean isMultiNonNullUnion(Schema type) {
    return type != null
        && type.getType() == Schema.Type.UNION
        && type.getBranches().size() > 1;
  }

  /**
   * Type-name extraction policy:
   * <ul>
   *   <li>Canonical refs whose URI ends in {@code /$defs/<key>} or
   *       {@code /definitions/<key>} (local or cross-doc): extract
   *       {@code <key>}. Collision-safe under the LT-canonical convention
   *       that $defs keys are unique across all referenced docs.</li>
   *   <li>Everything else (whole-doc refs, arbitrary JSON-Pointer refs,
   *       non-$defs sub-schema refs): synthesize a {@code Ref<N>} typeName
   *       and record {@code (Ref<N> → original URI)} in the LT's
   *       {@code externalImports} map. The writer emits these as a local
   *       {@code #/$defs/Ref<N>} passthrough whose body re-emits the
   *       original URI verbatim.</li>
   * </ul>
   * Returns {@code null} if the URI is non-canonical — caller invokes
   * {@link ToLogicalContext#synthesizeRefName} to get the synthesized name.
   */
  private static String extractCanonicalDefsKey(String refValue) {
    String[] markers = {"/$defs/", "/definitions/"};
    for (String marker : markers) {
      int idx = refValue.indexOf(marker);
      if (idx < 0) {
        continue;
      }
      String tail = refValue.substring(idx + marker.length());
      // If the URI continues with another JSON-pointer segment after the
      // $defs key, it's pointing INTO a $defs entry's body, not at the
      // entry itself — non-canonical, must synthesize.
      if (tail.indexOf('/') >= 0) {
        return null;
      }
      return tail;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static void recoverNamedTypesFromDefs(
      JsonSchema schema, ToLogicalContext<String> ctx) {
    Object defsObj = schema.rawSchema().getUnprocessedProperties().get("$defs");
    if (!(defsObj instanceof Map)) {
      return;
    }
    // Walk the raw JsonNode in parallel so we can detect synthetic-wrapper
    // entries via the `logical.ref` marker (a sibling of `$ref` that everit's
    // parser drops when constructing a ReferenceSchema).
    JsonNode rawDefs = null;
    JsonNode rawRoot = schema.toJsonNode();
    if (rawRoot != null && rawRoot.has("$defs")) {
      rawDefs = rawRoot.get("$defs");
    }
    // The $defs entry's KEY is the named-type identifier — refs use JSON
    // Pointer form `#/$defs/<key>`, so the key is what callers reference.
    Map<String, Object> defs = (Map<String, Object>) defsObj;
    for (Map.Entry<String, Object> entry : defs.entrySet()) {
      if (!(entry.getValue() instanceof org.everit.json.schema.Schema)) {
        continue;
      }
      org.everit.json.schema.Schema defSchema =
          (org.everit.json.schema.Schema) entry.getValue();
      // Synthetic-wrapper detection: the raw JSON entry carries
      // `"logical.ref": true` alongside `$ref`. Register as a synthetic
      // external and lazy-promote the body via the referred schema, so
      // downstream consumers see actual content under namedTypes.
      if (rawDefs != null && rawDefs.has(entry.getKey())) {
        JsonNode rawEntry = rawDefs.get(entry.getKey());
        if (rawEntry.isObject()
            && rawEntry.has("logical.ref")
            && rawEntry.get("logical.ref").asBoolean()
            && rawEntry.has("$ref")) {
          ctx.registerSyntheticExternal(
              entry.getKey(), rawEntry.get("$ref").asText());
          if (defSchema instanceof ReferenceSchema) {
            org.everit.json.schema.Schema referredSchema =
                ((ReferenceSchema) defSchema).getReferredSchema();
            if (referredSchema != null) {
              ctx.putNamedType(entry.getKey(),
                  Schema.createStruct(new ArrayList<>()));
              // Named-type body walks start with an empty indexPath; defaults
              // inside named types aren't part of the root schema's positional
              // path-keyed map.
              Schema body = convertWithCycleDetection(
                  referredSchema, false, ctx, Collections.emptyList());
              ctx.putNamedType(entry.getKey(), body);
            }
          }
          continue;
        }
      }
      Schema converted = convertWithCycleDetection(
          defSchema, false, ctx, Collections.emptyList());
      ctx.putNamedType(entry.getKey(), converted);
    }
  }

  private static Schema convertWithCycleDetection(
      final org.everit.json.schema.Schema schema, boolean isNullable,
      ToLogicalContext<String> ctx, final List<Integer> indexPath) {
    if (schema instanceof ReferenceSchema) {
      ReferenceSchema refSchema = (ReferenceSchema) schema;
      // Prefer NAMED_TYPE_REF emission via convert() — every $ref from our
      // writer has a refValue of the form `[<doc>]#/$defs/<name>` and convert()
      // extracts the name as the addressable identifier without recursing
      // into the referred body.
      if (refSchema.getReferenceValue() != null) {
        return convert(schema, isNullable, ctx, indexPath);
      }
      // Defensive fallback for unusual inputs (e.g. an in-memory ReferenceSchema
      // with no refValue set): follow the referred body with cycle detection.
      org.everit.json.schema.Schema referredSchema = refSchema.getReferredSchema();
      if (referredSchema == null) {
        return convert(schema, isNullable, ctx, indexPath);
      }
      if (!ctx.addSeenSchema(referredSchema.toString())) {
        throw new ValidationException(ctx.getCyclicSchemaErrorMessage());
      }
      final Schema result = convert(referredSchema, isNullable, ctx, indexPath);
      ctx.removeSeenSchema(referredSchema.toString());
      return result;
    }
    return convert(schema, isNullable, ctx, indexPath);
  }

  private static Schema convert(
      final org.everit.json.schema.Schema schema, boolean isNullable,
      ToLogicalContext<String> ctx, final List<Integer> indexPath) {
    final String title = schema.getTitle();

    if (schema instanceof BooleanSchema) {
      return Schema.create(Schema.Type.BOOLEAN).setNullable(isNullable);
    } else if (schema instanceof NumberSchema) {
      return convertNumberSchema((NumberSchema) schema, isNullable, title);
    } else if (schema instanceof StringSchema) {
      return convertStringSchema((StringSchema) schema, isNullable);
    } else if (schema instanceof EnumSchema) {
      EnumSchema enumSchema = (EnumSchema) schema;
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> enumMeta =
          (List<Map<String, Object>>) schema.getUnprocessedProperties().get("confluent:enum");
      List<?> possibleValues = enumSchema.getPossibleValuesAsList();
      List<EnumValue> values = new ArrayList<>();
      Set<String> seenSymbols = new HashSet<>();
      for (int i = 0; i < possibleValues.size(); i++) {
        String symbol = possibleValues.get(i).toString();
        // JSON Schema only "SHOULD" require unique enum values; tolerate
        // duplicates by keeping the first occurrence's metadata.
        if (!seenSymbols.add(symbol)) {
          continue;
        }
        String doc = null;
        Map<String, Object> evParams = null;
        if (enumMeta != null && i < enumMeta.size()) {
          @SuppressWarnings("unchecked")
          Map<String, Object> entry = enumMeta.get(i);
          doc = (String) entry.get("doc");
          evParams = (Map<String, Object>) entry.get("params");
        }
        values.add(new EnumValue(symbol, doc, evParams));
      }
      Schema result = Schema.createEnum(values).setNullable(isNullable);
      result.setDoc(schema.getDescription());
      readSchemaTags(schema, result);
      readSchemaParams(schema, result);
      return result;
    } else if (schema instanceof CombinedSchema) {
      return convertCombinedSchema(
          (CombinedSchema) schema, isNullable, ctx, indexPath);
    } else if (schema instanceof ArraySchema) {
      return convertArraySchema(
          (ArraySchema) schema, isNullable, ctx, indexPath);
    } else if (schema instanceof ObjectSchema) {
      return convertObjectSchema(
          (ObjectSchema) schema, isNullable, ctx, indexPath);
    } else if (schema instanceof EmptySchema) {
      return Schema.create(Schema.Type.VARIANT).setNullable(isNullable);
    } else if (schema instanceof ReferenceSchema) {
      ReferenceSchema refSchema = (ReferenceSchema) schema;
      String refValue = refSchema.getReferenceValue();
      if (refValue != null) {
        // Canonical refs (URI ends in /$defs/<key> or /definitions/<key>)
        // extract <key> directly. Non-canonical refs (whole-doc, arbitrary
        // pointer, local non-$defs) synthesize a Ref<N> typeName with the
        // original URI stashed in externalImports — the writer materializes
        // those as a local #/$defs/Ref<N> passthrough at emit time.
        String typeName = extractCanonicalDefsKey(refValue);
        boolean synthetic = (typeName == null);
        if (synthetic) {
          typeName = ctx.synthesizeRefName(refValue);
        }
        // Populate the named-types map on demand: schemas loaded via
        // SchemaLoader (rather than constructed by our V1 writer) typically
        // don't carry $defs in unprocessedProperties, so recoverNamedTypesFromDefs
        // wouldn't have seen this body. Walk the referred body now, with a
        // placeholder first to short-circuit recursion if the body refers back
        // to its own name.
        boolean wasKnown = ctx.hasNamedType(typeName);
        if (!wasKnown && refSchema.getReferredSchema() != null) {
          ctx.putNamedType(typeName, Schema.createStruct(new ArrayList<>()));
          // Named-type body walks start with an empty indexPath; defaults
          // inside named types aren't part of the root schema's positional
          // path-keyed map.
          Schema body = convertWithCycleDetection(
              refSchema.getReferredSchema(), false, ctx,
              Collections.emptyList());
          ctx.putNamedType(typeName, body);
        }
        // Mark as external only on first-time-seen canonical cross-doc refs.
        // Names already in namedTypes from the $defs walk are either real
        // local types (preserve) or synthetic externals (already marked).
        // Synthetic refs were marked by synthesizeRefName.
        // The everit parser may rewrite local "#/..." refs to "<base>#/..."
        // (often "UNKNOWN" when the active schema has no $id), so a prefix
        // check on refValue isn't reliable — rely on first-time-seen instead.
        if (!synthetic && !wasKnown && !refValue.startsWith("#")) {
          ctx.addExternalType(typeName);
        }
        return Schema.createNamedTypeRef(typeName).setNullable(isNullable);
      }
      if (refSchema.getReferredSchema() == null) {
        throw new ValidationException(
            "Unresolved $ref with no reference value");
      }
      return convertWithCycleDetection(
          refSchema.getReferredSchema(), isNullable, ctx, indexPath);
    } else {
      throw new ValidationException(
          "Unsupported JSON schema type " + schema.getClass().getName());
    }
  }

  private static Schema convertNumberSchema(
      NumberSchema numberSchema, boolean isNullable, String title) {
    String type = (String) numberSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
    if (type == null) {
      return numberSchema.requiresInteger()
          ? Schema.create(Schema.Type.BIGINT).setNullable(isNullable)
          : Schema.create(Schema.Type.DOUBLE).setNullable(isNullable);
    }
    switch (type) {
      case CONNECT_TYPE_INT8:
        return Schema.create(Schema.Type.TINYINT).setNullable(isNullable);
      case CONNECT_TYPE_INT16:
        return Schema.create(Schema.Type.SMALLINT).setNullable(isNullable);
      case CONNECT_TYPE_INT32:
        return fromInt32Type(isNullable, numberSchema);
      case CONNECT_TYPE_INT64:
        return fromInt64Type(isNullable, numberSchema);
      case CONNECT_TYPE_FLOAT32:
        return Schema.create(Schema.Type.FLOAT).setNullable(isNullable);
      case CONNECT_TYPE_FLOAT64:
        return Schema.create(Schema.Type.DOUBLE).setNullable(isNullable);
      case CONNECT_TYPE_BYTES:
        if (!CONNECT_TYPE_DECIMAL.equals(title)) {
          throw new ValidationException("Expected decimal type");
        }
        @SuppressWarnings("unchecked")
        final Map<String, Object> properties =
            (Map<String, Object>) numberSchema.getUnprocessedProperties()
                .getOrDefault(CONNECT_PARAMETERS, Collections.emptyMap());
        final int scale = Optional.ofNullable(properties.get(CONNECT_TYPE_DECIMAL_SCALE))
            .map(prop -> Integer.parseInt((String) prop))
            .orElse(DEFAULT_DECIMAL_SCALE);
        final int precision = Optional.ofNullable(properties.get(CONNECT_TYPE_DECIMAL_PRECISION))
            .map(prop -> Integer.parseInt((String) prop))
            .orElse(DEFAULT_DECIMAL_PRECISION);
        return Schema.createDecimal(precision, scale).setNullable(isNullable);
      default:
        throw new ValidationException("Unsupported type " + type);
    }
  }

  private static Schema fromInt32Type(boolean isNullable, org.everit.json.schema.Schema schema) {
    final String title = schema.getTitle();
    if (Objects.equals(title, CONNECT_TYPE_TIME)) {
      final int precision = getTimestampPrecision(schema);
      return Schema.createTime(precision).setNullable(isNullable);
    } else if (Objects.equals(title, CONNECT_TYPE_DATE)) {
      return Schema.create(Schema.Type.DATE).setNullable(isNullable);
    } else {
      return Schema.create(Schema.Type.INT).setNullable(isNullable);
    }
  }

  private static Schema fromInt64Type(boolean isNullable, org.everit.json.schema.Schema schema) {
    if (Objects.equals(schema.getTitle(), CONNECT_TYPE_TIMESTAMP)) {
      final int precision = getTimestampPrecision(schema);
      return Schema.createTimestampLtz(precision).setNullable(isNullable);
    } else if (FLINK_TYPE_TIMESTAMP.equals(
        schema.getUnprocessedProperties().getOrDefault(FLINK_TYPE_PROP, null))) {
      final int precision = getTimestampPrecision(schema);
      return Schema.createTimestamp(precision).setNullable(isNullable);
    }
    return Schema.create(Schema.Type.BIGINT).setNullable(isNullable);
  }

  @SuppressWarnings("unchecked")
  private static int getTimestampPrecision(org.everit.json.schema.Schema schema) {
    return (int) schema.getUnprocessedProperties().getOrDefault(FLINK_PRECISION, 3);
  }

  private static Schema convertStringSchema(StringSchema schema, boolean isNullable) {
    final Map<String, Object> props = schema.getUnprocessedProperties();
    String type = (String) props.get(CONNECT_TYPE_PROP);
    if (CONNECT_TYPE_BYTES.equals(type)) {
      final Integer minLength = (Integer) props.getOrDefault(FLINK_MIN_LENGTH, null);
      final Integer maxLength = (Integer) props.getOrDefault(FLINK_MAX_LENGTH, null);
      if (minLength != null && Objects.equals(minLength, maxLength)) {
        return Schema.createBinary(maxLength).setNullable(isNullable);
      } else if (maxLength != null) {
        return Schema.createVarbinary(maxLength).setNullable(isNullable);
      } else {
        return Schema.createBytes().setNullable(isNullable);
      }
    } else {
      final Integer minLength = schema.getMinLength();
      final Integer maxLength = schema.getMaxLength();
      if (minLength != null && Objects.equals(minLength, maxLength)) {
        return Schema.createChar(maxLength).setNullable(isNullable);
      } else if (maxLength != null) {
        return Schema.createVarchar(maxLength).setNullable(isNullable);
      } else {
        return Schema.createString().setNullable(isNullable);
      }
    }
  }

  private static Schema convertCombinedSchema(
      CombinedSchema combinedSchema, boolean isNullable, ToLogicalContext<String> ctx,
      final List<Integer> indexPath) {
    CombinedSchema.ValidationCriterion criterion = combinedSchema.getCriterion();
    if (criterion == CombinedSchema.ALL_CRITERION) {
      return convertWithCycleDetection(
          CombinedSchemaUtils.simplifyAllOfSchema(combinedSchema), isNullable,
          ctx, indexPath);
    } else if (criterion != CombinedSchema.ONE_CRITERION
        && criterion != CombinedSchema.ANY_CRITERION) {
      throw new ValidationException("Unsupported criterion " + criterion);
    }

    // Singleton-collapse: a oneOf with exactly one non-null member is
    // semantically equivalent to that member type. Mirrors Avro's
    // AvroToLogicalTypeConverter behavior — in content-variant systems
    // (Avro/JSON), UNION<T> conveys no information beyond T. Catches both
    // bare oneOf:[T] and the wrapper shape oneOf:[null, T].
    List<org.everit.json.schema.Schema> nonNullSubschemas = new ArrayList<>();
    boolean hasNullMember = false;
    for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
      if (subSchema instanceof NullSchema) {
        hasNullMember = true;
      } else {
        nonNullSubschemas.add(subSchema);
      }
    }
    if (nonNullSubschemas.size() == 1) {
      return convertWithCycleDetection(
          nonNullSubschemas.get(0), isNullable || hasNullMember, ctx, indexPath);
    }

    // Proper union: oneOf/anyOf with multiple non-null types
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> unionMeta = (List<Map<String, Object>>)
        combinedSchema.getUnprocessedProperties().get("confluent:union");
    int index = 0;
    boolean isNullableUnion = isNullable;
    final List<UnionBranch> branches = new ArrayList<>();

    for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
      if (subSchema instanceof NullSchema) {
        isNullableUnion = true;
      } else {
        Map<String, Object> hint = unionMeta != null && index < unionMeta.size()
            ? unionMeta.get(index) : null;
        String branchName;
        if (hint != null && hint.get("name") != null) {
          branchName = (String) hint.get("name");
        } else {
          branchName = subSchema.getTitle() != null
              ? subSchema.getTitle()
              : GENERALIZED_TYPE_UNION_PREFIX + index;
        }
        String branchDoc = hint != null ? (String) hint.get("doc") : null;
        @SuppressWarnings("unchecked")
        Map<String, Object> branchParams = hint != null
            ? (Map<String, Object>) hint.get("params") : null;
        Schema branchType = convertWithCycleDetection(
            subSchema, true, ctx, appendToList(indexPath, index));
        branches.add(new UnionBranch(branchName, branchType, branchDoc, branchParams));
        index++;
      }
    }
    return Schema.createUnion(branches).setNullable(isNullableUnion);
  }

  private static Schema convertArraySchema(
      ArraySchema arraySchema, boolean isNullable, ToLogicalContext<String> ctx,
      final List<Integer> indexPath) {
    org.everit.json.schema.Schema allItemSchema = arraySchema.getAllItemSchema();
    if (allItemSchema == null) {
      if (arraySchema.getItemSchemas() != null) {
        throw new ValidationException(
            "JSON tuples (i.e. arrays that contain items of different types) are not supported");
      }
      throw new ValidationException("Array schema did not specify the items type");
    }
    String type = (String) arraySchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
    if (CONNECT_TYPE_MAP.equals(type) && allItemSchema instanceof ObjectSchema) {
      ObjectSchema objectSchema = (ObjectSchema) allItemSchema;
      final boolean isMultiset = Objects.equals(
          FLINK_TYPE_MULTISET,
          arraySchema.getUnprocessedProperties().get(FLINK_TYPE_PROP));
      // MAP: key at appendToList(indexPath, 0), value at appendToList(indexPath, 1).
      final Schema keyType = convertWithCycleDetection(
          objectSchema.getPropertySchemas().get(KEY_FIELD), false,
          ctx, appendToList(indexPath, 0));
      final Schema valueType = convertWithCycleDetection(
          objectSchema.getPropertySchemas().get(VALUE_FIELD), false,
          ctx, appendToList(indexPath, 1));
      return createMapLikeType(isNullable, keyType, valueType, isMultiset);
    } else {
      // ARRAY appends [0] for the element type, matching upstream Flink's
      // JSON convention (Avro-style). Proto's ARRAY adds no index, which is
      // why a single LT doc can carry both — the writers each pick the path
      // shape their own consumers expect.
      return Schema.createArray(
          convertWithCycleDetection(
              allItemSchema, false, ctx, appendToList(indexPath, 0)))
          .setNullable(isNullable);
    }
  }

  private static Schema convertObjectSchema(
      ObjectSchema objectSchema, boolean isNullable, ToLogicalContext<String> ctx,
      final List<Integer> indexPath) {
    String type = (String) objectSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
    if (CONNECT_TYPE_MAP.equals(type)) {
      final boolean isMultiset = Objects.equals(
          FLINK_TYPE_MULTISET,
          objectSchema.getUnprocessedProperties().get(FLINK_TYPE_PROP));
      // MAP value at appendToList(indexPath, 1); key type read from
      // unprocessedProperties (no schema body to walk for default capture).
      final Schema valueType = convertWithCycleDetection(
          objectSchema.getSchemaOfAdditionalProperties(), false,
          ctx, appendToList(indexPath, 1));
      final Schema keyType = readMapKeyType(objectSchema);
      return createMapLikeType(isNullable, keyType, valueType, isMultiset);
    } else {
      return convertRowType(isNullable, ctx, objectSchema, indexPath);
    }
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

  private static Schema convertRowType(
      boolean isNullable, ToLogicalContext<String> ctx, ObjectSchema objectSchema,
      final List<Integer> indexPath) {
    Map<String, org.everit.json.schema.Schema> properties = objectSchema.getPropertySchemas();
    final Comparator<Entry<String, org.everit.json.schema.Schema>> indexComparator =
        Comparator.comparing(
            e -> {
              final Object index =
                  e.getValue().getUnprocessedProperties().get(CONNECT_INDEX_PROP);
              return index != null ? (Integer) index : null;
            },
            Comparator.nullsLast(Comparator.comparing(Function.identity())));
    final Stream<Entry<String, org.everit.json.schema.Schema>> sortedFields =
        properties.entrySet().stream().sorted(indexComparator.thenComparing(Entry::getKey));
    final List<Field> fields = new ArrayList<>();
    int pos = 0;
    for (Entry<String, org.everit.json.schema.Schema> property :
        sortedFields.collect(Collectors.toList())) {
      String subFieldName = property.getKey();
      org.everit.json.schema.Schema subSchema = property.getValue();
      boolean isFieldNullable =
          !objectSchema.getRequiredProperties().contains(subFieldName);
      ctx.pushFieldPath(subFieldName);
      final List<Integer> fieldIndex = appendToList(indexPath, pos);
      final Schema fieldType;
      try {
        fieldType = convertWithCycleDetection(subSchema, isFieldNullable, ctx, fieldIndex);
      } finally {
        ctx.popFieldPath();
      }
      Object defaultValue = captureDefaultValue(ctx, subSchema, fieldType, fieldIndex);
      boolean hasDefault = defaultValue != null;
      List<String> fieldTags = readFieldTags(subSchema);
      Map<String, Object> fieldParams = readFieldParams(subSchema);
      List<io.confluent.kafka.schemaregistry.type.logical.Rule> fieldRules =
          readRules(subSchema);
      fields.add(new Field(subFieldName, fieldType, pos++,
          defaultValue, hasDefault, subSchema.getDescription(),
          fieldTags, fieldParams, fieldRules));
    }
    Schema structSchema = Schema.createStruct(fields).setNullable(isNullable);
    structSchema.setDoc(objectSchema.getDescription());
    readSchemaTags(objectSchema, structSchema);
    readSchemaParams(objectSchema, structSchema);
    readSchemaRules(objectSchema, structSchema);
    return structSchema;
  }

  private static Schema readMapKeyType(ObjectSchema objectSchema) {
    Map<String, Object> unprocessed = objectSchema.getUnprocessedProperties();
    Object keyLength = unprocessed.get(CommonConstants.LOGICAL_KEY_LENGTH_PROP);
    if (keyLength instanceof Integer) {
      String keyTypeName = (String) unprocessed.get(CommonConstants.LOGICAL_KEY_TYPE_PROP);
      if ("CHAR".equals(keyTypeName)) {
        return Schema.createChar((int) keyLength).setNullable(false);
      }
      return Schema.createVarchar((int) keyLength).setNullable(false);
    }
    return Schema.createString().setNullable(false);
  }

  @SuppressWarnings("unchecked")
  private static List<String> readFieldTags(org.everit.json.schema.Schema jsonSchema) {
    Object tags = jsonSchema.getUnprocessedProperties().get("confluent:tags");
    if (tags instanceof List) {
      List<String> tagList = new ArrayList<>();
      for (Object tag : (List<?>) tags) {
        tagList.add(tag.toString());
      }
      return tagList;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> readFieldParams(org.everit.json.schema.Schema jsonSchema) {
    Object params = jsonSchema.getUnprocessedProperties().get("confluent:params");
    if (params instanceof Map) {
      Map<String, Object> userParams = (Map<String, Object>) params;
      return userParams.isEmpty() ? null : new LinkedHashMap<>(userParams);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static void readSchemaTags(org.everit.json.schema.Schema jsonSchema, Schema schema) {
    Object tags = jsonSchema.getUnprocessedProperties().get("confluent:tags");
    if (tags instanceof List) {
      List<String> tagList = new ArrayList<>();
      for (Object tag : (List<?>) tags) {
        tagList.add(tag.toString());
      }
      schema.setTags(tagList);
    }
  }

  @SuppressWarnings("unchecked")
  private static void readSchemaParams(org.everit.json.schema.Schema jsonSchema, Schema schema) {
    Object params = jsonSchema.getUnprocessedProperties().get("confluent:params");
    if (params instanceof Map) {
      Map<String, Object> userParams = (Map<String, Object>) params;
      if (!userParams.isEmpty()) {
        schema.setParams(new LinkedHashMap<>(userParams));
      }
    }
  }

  /**
   * Read CHECK rules from a {@code confluent:rules} array property in the
   * JSON Schema's unprocessedProperties. Returns null when absent so the
   * Field constructor's default-empty applies.
   */
  @SuppressWarnings("unchecked")
  private static List<io.confluent.kafka.schemaregistry.type.logical.Rule> readRules(
      org.everit.json.schema.Schema jsonSchema) {
    Object value = jsonSchema.getUnprocessedProperties().get("confluent:rules");
    if (!(value instanceof List)) {
      return null;
    }
    List<io.confluent.kafka.schemaregistry.type.logical.Rule> out = new ArrayList<>();
    for (Object item : (List<?>) value) {
      if (!(item instanceof Map)) {
        continue;
      }
      Map<String, Object> obj = (Map<String, Object>) item;
      Object expr = obj.get("expr");
      Object sql = obj.get("sql");
      // Skip rules with null OR empty expr/sql. Empty strings round-trip
      // into nonsensical `CHECK ()` DDL. Matches the proto/Avro readers'
      // policy.
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
    return out.isEmpty() ? null : out;
  }

  private static String nullIfEmpty(Object o) {
    if (!(o instanceof String)) {
      return null;
    }
    String s = (String) o;
    return s.isEmpty() ? null : s;
  }

  private static void readSchemaRules(
      org.everit.json.schema.Schema jsonSchema, Schema schema) {
    List<io.confluent.kafka.schemaregistry.type.logical.Rule> rules = readRules(jsonSchema);
    if (rules != null) {
      schema.setRules(rules);
    }
  }
}
