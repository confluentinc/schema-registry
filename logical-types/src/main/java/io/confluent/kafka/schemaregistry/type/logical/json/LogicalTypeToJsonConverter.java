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
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.common.FromLogicalContext;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_PROPERTY_CURRENT_VERSION;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_PROPERTY_VERSION;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_TYPE_MULTISET;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_TYPE_PROP;
import static io.confluent.kafka.schemaregistry.type.logical.json.CommonConstants.FLINK_TYPE_TIMESTAMP;

/**
 * Converts logical type {@link Schema} to JSON Schema.
 */
public class LogicalTypeToJsonConverter {

  private static final int MAX_LENGTH = Integer.MAX_VALUE;

  public static JsonSchema fromLogicalType(LogicalType logicalType, String rowName) {
    return fromLogicalType(logicalType, rowName, LogicalTypeVersion.V2);
  }

  public static JsonSchema fromLogicalType(
      LogicalType logicalType, String rowName, LogicalTypeVersion version) {
    Schema schema = logicalType.getRootSchema();
    FromLogicalContext<String> ctx =
        new FromLogicalContext<>(logicalType, version);

    // Register each external type name → source doc URI. The NAMED_TYPE_REF
    // case uses this mapping to emit `<doc>#/$defs/<name>` refs. Throws on
    // duplicate-name collisions across resolvedReferences.
    for (Map.Entry<String, String> entry :
        logicalType.getResolvedReferences().entrySet()) {
      collectExternalNames(entry.getKey(), entry.getValue(), ctx);
    }

    org.everit.json.schema.Schema.Builder<?> rootBuilder =
        fromLogicalTypeBuilder(schema, rowName, ctx);

    // Some everit primitive builders initialize unprocessedProperties as an
    // immutable singleton map (e.g., {connect.type=int32}). Build a fresh
    // mutable copy before adding root-level extras.
    Map<String, Object> rootExtras = new LinkedHashMap<>(
        rootBuilder.unprocessedProperties);

    // Build each LOCAL named-type's everit Schema; we render them under $defs
    // in the JsonNode below. The $defs key IS the addressable identifier;
    // refs use the JSON Pointer form `#/$defs/<name>`. Externals (whose
    // bodies were lazy-promoted into namedTypes by the reader) are emitted
    // via the existing collectExternalNames + $ref-with-doc-URI path; we
    // must NOT inline them as $defs here.
    Map<String, Schema> localNamedTypes = logicalType.getLocalNamedTypes();
    Map<String, org.everit.json.schema.Schema> defs = null;
    if (!localNamedTypes.isEmpty()) {
      defs = new LinkedHashMap<>();
      for (Map.Entry<String, Schema> entry : localNamedTypes.entrySet()) {
        String typeName = entry.getKey();
        org.everit.json.schema.Schema.Builder<?> defBuilder =
            fromLogicalTypeIgnoreNullable(entry.getValue(), typeName, ctx);
        defs.put(typeName, defBuilder.build());
      }
    }

    if (logicalType.getNamespace() != null && !ctx.isV1()) {
      rootExtras.put("confluent:namespace", logicalType.getNamespace());
    }

    // Tag the dialect we emit. Our use of $defs and JSON Pointer $refs is
    // standard in JSON Schema draft 2020-12. Skipped in V1 (Flink-compat)
    // because Flink's writer doesn't emit a $schema declaration.
    if (!ctx.isV1()) {
      rootExtras.put("$schema", "https://json-schema.org/draft/2020-12/schema");
    }

    // Render the outer schema (without $defs). Its unprocessedProperties
    // contain only primitives/lists/maps of primitives, so everit's toString
    // produces clean JSON. We splice $defs in at the JsonNode layer rather
    // than via unprocessedProperties — putting Schema instances there would
    // be bean-introspected by everit's printer.
    rootBuilder.unprocessedProperties(rootExtras);
    ObjectNode rootNode;
    try {
      rootNode = (ObjectNode) JacksonMapper.INSTANCE
          .readTree(rootBuilder.build().toString());
    } catch (java.io.IOException e) {
      throw new ValidationException(
          "Failed to render outer JSON schema", e);
    }

    Map<String, String> externalImports = logicalType.getExternalImports();
    if (defs != null || !externalImports.isEmpty()) {
      ObjectNode defsNode = JacksonMapper.INSTANCE.createObjectNode();
      if (defs != null) {
        for (Map.Entry<String, org.everit.json.schema.Schema> entry :
            defs.entrySet()) {
          try {
            defsNode.set(entry.getKey(),
                JacksonMapper.INSTANCE.readTree(entry.getValue().toString()));
          } catch (java.io.IOException e) {
            throw new ValidationException(
                "Failed to render def JSON schema: " + entry.getKey(), e);
          }
        }
      }
      // Synthetic-wrapper $defs entries: each externalImports entry becomes a
      // local passthrough `{"$ref": <uri>, "logical.ref": true}`. The marker
      // tells future readers to treat this $defs entry as a synthetic
      // external rather than a degenerate local type.
      for (Map.Entry<String, String> entry : externalImports.entrySet()) {
        ObjectNode synthetic = JacksonMapper.INSTANCE.createObjectNode();
        synthetic.put("$ref", entry.getValue());
        synthetic.put("logical.ref", true);
        defsNode.set(entry.getKey(), synthetic);
      }
      rootNode.set("$defs", defsNode);
    }

    // normalize() canonicalizes key order via a json-sKema re-parse, which
    // also rebuilds the in-memory schemaObj from the JsonNode (with $defs as
    // Map<String, everit.Schema> via SchemaTranslator) — no need for the
    // converter to construct one itself.
    //
    // V1 must stay byte-equivalent to Flink's writer output, which does not
    // normalize. Normalize would alphabetically sort object keys — visible
    // deviation from Flink's insertion-order emission.
    JsonSchema result = new JsonSchema(
        rootNode, logicalType.getReferences(), logicalType.getResolvedReferences(), null);
    if (!ctx.isV1()) {
      result = result.normalize();
    }
    Map<String, String> metadataProps = new LinkedHashMap<>();
    metadataProps.put("confluent:edition", version == LogicalTypeVersion.V1 ? "1" : "2");
    return (JsonSchema) result.copy(new Metadata(null, metadataProps, null), null);
  }

  /**
   * Walks the external schema's $defs (and draft-7 definitions) and registers
   * each entry key as an addressable type name mapped to the doc URI. Refs
   * use the JSON Pointer form {@code #/$defs/&lt;key&gt;}, so the entry key
   * IS the addressable identifier.
   */
  private static void collectExternalNames(
      String docUri, String schemaString, FromLogicalContext<String> ctx) {
    try {
      JsonNode root = JacksonMapper.INSTANCE.readTree(schemaString);
      collectKeysFrom(root.get("$defs"), docUri, ctx);
      collectKeysFrom(root.get("definitions"), docUri, ctx);
    } catch (java.io.IOException e) {
      throw new ValidationException(
          "Failed to parse external schema for name collection", e);
    }
  }

  /**
   * Registers each top-level field name of the given object container as an
   * external type name mapped to the doc URI. Skips keys already present in
   * the LT's local namedTypes so an external $defs entry can't shadow a
   * locally-defined type with the same simple name.
   */
  private static void collectKeysFrom(
      JsonNode container, String docUri, FromLogicalContext<String> ctx) {
    if (container == null || !container.isObject()) {
      return;
    }
    Map<String, Schema> localNamedTypes = ctx.getLogicalType().getLocalNamedTypes();
    Iterator<String> names = container.fieldNames();
    while (names.hasNext()) {
      String name = names.next();
      if (!localNamedTypes.containsKey(name)) {
        ctx.putConverted(name, docUri);
      }
    }
  }

  private static org.everit.json.schema.Schema fromLogicalTypeRaw(
      Schema schema, String rowName, FromLogicalContext<String> ctx) {
    return fromLogicalTypeBuilder(schema, rowName, ctx).build();
  }

  private static org.everit.json.schema.Schema.Builder<?> fromLogicalTypeBuilder(
      Schema schema, String rowName, FromLogicalContext<String> ctx) {
    org.everit.json.schema.Schema.Builder<?> notNullSchema =
        fromLogicalTypeIgnoreNullable(schema, rowName, ctx);
    return schema.isNullable() ? nullableSchema(notNullSchema.build()) : notNullSchema;
  }

  private static org.everit.json.schema.Schema.Builder<?> fromLogicalTypeIgnoreNullable(
      Schema schema, String rowName, FromLogicalContext<String> ctx) {
    switch (schema.getType()) {
      case BOOLEAN:
        return BooleanSchema.builder();
      case TINYINT:
        return NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT8));
      case SMALLINT:
        return NumberSchema.builder()
            .unprocessedProperties(
                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT16));
      case INT:
        return NumberSchema.builder()
            .unprocessedProperties(
                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT32));
      case BIGINT:
        return NumberSchema.builder()
            .unprocessedProperties(
                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT64));
      case FLOAT:
        return NumberSchema.builder()
            .unprocessedProperties(
                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_FLOAT32));
      case DOUBLE:
        return NumberSchema.builder()
            .unprocessedProperties(
                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_FLOAT64));
      case CHAR: {
        int length = schema.getLength();
        final StringSchema.Builder builder = StringSchema.builder();
        builder.minLength(length);
        builder.maxLength(length);
        return builder;
      }
      case VARCHAR: {
        int length = schema.getLength();
        final StringSchema.Builder builder = StringSchema.builder();
        if (length < MAX_LENGTH) {
          builder.maxLength(length);
        }
        return builder;
      }
      case BINARY:
        return createBinaryStringType(schema.getLength(), schema.getLength());
      case VARBINARY:
        return createBinaryStringType(-1, schema.getLength());
      case TIMESTAMP:
        if (ctx.isV1() && schema.getPrecision() > 3) {
          throw new ValidationException(
              "TIMESTAMP precision > 3 not supported in V1 emission mode");
        }
        return convertTimestamp(schema.getPrecision(), true);
      case TIMESTAMP_LTZ:
        if (ctx.isV1() && schema.getPrecision() > 3) {
          throw new ValidationException(
              "TIMESTAMP_LTZ precision > 3 not supported in V1 emission mode");
        }
        return convertTimestamp(schema.getPrecision(), false);
      case DATE:
        return NumberSchema.builder()
            .title(CONNECT_TYPE_DATE)
            .unprocessedProperties(
                Collections.singletonMap(CONNECT_TYPE_PROP, CONNECT_TYPE_INT32));
      case TIME:
        if (ctx.isV1() && schema.getPrecision() > 3) {
          throw new ValidationException(
              "TIME precision > 3 not supported in V1 emission mode");
        }
        return convertTime(schema.getPrecision());
      case DECIMAL: {
        final Map<String, Object> props = getDecimalProperties(schema);
        return NumberSchema.builder()
            .unprocessedProperties(props)
            .title(CONNECT_TYPE_DECIMAL);
      }
      case STRUCT: {
        final ObjectSchema.Builder rowBuilder = ObjectSchema.builder();
        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
          Field field = fields.get(i);
          final String fieldName = field.getName();
          Schema fieldType = field.getSchema();
          final org.everit.json.schema.Schema.Builder<?> fieldSchema =
              fromLogicalTypeBuilder(fieldType, rowName + "_" + fieldName, ctx);
          if (field.hasDefaultValue() && !ctx.isV1()) {
            fieldSchema.defaultValue(
                JsonDefaultValueConverter.toJsonValue(fieldType, field.getDefaultValue()));
          }
          final Map<String, Object> extendedProps =
              new HashMap<>(fieldSchema.unprocessedProperties);
          extendedProps.put(CONNECT_INDEX_PROP, i);
          if (!ctx.isV1()) {
            if (!field.getTags().isEmpty()) {
              extendedProps.put("confluent:tags", field.getTags());
            }
            if (!field.getParams().isEmpty()) {
              extendedProps.put("confluent:params", field.getParams());
            }
            if (!field.getRules().isEmpty()) {
              extendedProps.put("confluent:rules", buildRulesWire(field.getRules()));
            }
          }
          fieldSchema.unprocessedProperties(extendedProps);
          if (field.getDoc() != null) {
            fieldSchema.description(field.getDoc());
          }
          rowBuilder.addPropertySchema(fieldName, fieldSchema.build());
          if (!fieldType.isNullable()) {
            rowBuilder.addRequiredProperty(fieldName);
          }
        }
        // Match Flink's emission: only set additionalProperties(false) when
        // the row has at least one field (Flink writes it inside its field
        // loop, so an empty row never gets the property). And only attach
        // schema-level description in V2 — Flink's writer never emits a
        // row-level description.
        if (!fields.isEmpty()) {
          rowBuilder.additionalProperties(false);
        }
        if (!ctx.isV1()) {
          rowBuilder.description(schema.getDoc());
          addSchemaTags(rowBuilder, schema);
          addSchemaParams(rowBuilder, schema);
          addSchemaRules(rowBuilder, schema);
        }
        return rowBuilder.title(rowName);
      }
      case ENUM: {
        if (ctx.isV1()) {
          throw new ValidationException("ENUM not supported in V1 emission mode");
        }
        List<Object> values = schema.getEnumValues().stream()
            .map(EnumValue::getSymbol)
            .collect(Collectors.toList());
        org.everit.json.schema.EnumSchema.Builder enumBuilder =
            org.everit.json.schema.EnumSchema.builder().possibleValues(values);
        enumBuilder.title(rowName);
        enumBuilder.description(schema.getDoc());
        addSchemaTags(enumBuilder, schema);
        addSchemaParams(enumBuilder, schema);
        addEnumValueMeta(enumBuilder, schema);
        return enumBuilder;
      }
      case UNION: {
        if (ctx.isV1()) {
          throw new ValidationException("UNION not supported in V1 emission mode");
        }
        CombinedSchema.Builder builder = CombinedSchema.builder()
            .criterion(CombinedSchema.ONE_CRITERION);
        List<Map<String, Object>> unionMeta = new ArrayList<>();
        for (UnionBranch branch : schema.getBranches()) {
          builder.subschema(fromLogicalTypeRaw(branch.getSchema(), branch.getName(), ctx));
          Map<String, Object> entry = new LinkedHashMap<>();
          entry.put("name", branch.getName());
          if (branch.getDoc() != null) {
            entry.put("doc", branch.getDoc());
          }
          if (!branch.getParams().isEmpty()) {
            entry.put("params", branch.getParams());
          }
          unionMeta.add(entry);
        }
        builder.unprocessedProperties(
            Collections.singletonMap("confluent:union", unionMeta));
        return builder;
      }
      case MAP:
        return convertMap(schema, rowName, ctx);
      case ARRAY:
        return ArraySchema.builder()
            .allItemSchema(fromLogicalTypeRaw(schema.getElementType(), rowName, ctx));
      case MULTISET:
        return convertMultiset(schema, rowName, ctx);
      case VARIANT:
        if (ctx.isV1()) {
          throw new ValidationException("VARIANT not supported in V1 emission mode");
        }
        return EmptySchema.builder();
      case NAMED_TYPE_REF: {
        String name = schema.getQualifiedName();
        // Synthetic-wrapper externals: emit a local `#/$defs/<name>` pointing
        // at the synthetic $defs entry materialized at the active-schema
        // level. The original URI is preserved in that entry's body.
        if (ctx.getLogicalType().getExternalImports().containsKey(name)) {
          return ReferenceSchema.builder().refValue("#/$defs/" + name);
        }
        // Externals are lazy-promoted into namedTypes too (so the LT stays
        // self-contained), so checking hasNamedType alone would route them
        // through the local "#/$defs/<name>" path. Dispatch externals first.
        if (ctx.getLogicalType().isExternal(name)) {
          if (ctx.isAlreadyConverted(name)) {
            // Canonical external: typeName is a $defs key registered by
            // collectExternalNames, with its source docUri as the value.
            String docUri = ctx.getConverted(name);
            return ReferenceSchema.builder().refValue(docUri + "#/$defs/" + name);
          }
          // Defensive fallback: external typeName not discovered via
          // collectExternalNames and not bound via externalImports. Emit
          // verbatim as the $ref value (callers must arrange for the
          // resolvedReferences side to make this resolvable).
          return ReferenceSchema.builder().refValue(name);
        }
        if (ctx.hasNamedType(name)) {
          // Local type — JSON Pointer into the outer doc's $defs.
          return ReferenceSchema.builder().refValue("#/$defs/" + name);
        }
        // External type — JSON Pointer into the external doc's $defs (or
        // definitions, which JsonSchema bridges to $defs at load time).
        if (ctx.isAlreadyConverted(name)) {
          String docUri = ctx.getConverted(name);
          return ReferenceSchema.builder().refValue(docUri + "#/$defs/" + name);
        }
        throw new ValidationException(
            "Unknown named type reference: '" + name + "'. Either define it "
                + "as a local ROW/ENUM declaration, or place it as a $defs "
                + "(or definitions) entry in an external schema in "
                + "resolvedReferences.");
      }
      default:
        throw new ValidationException(
            "Unsupported to derive JSON Schema for type: " + schema);
    }
  }

  private static org.everit.json.schema.Schema.Builder<StringSchema> createBinaryStringType(
      int minLength, int maxLength) {
    final StringSchema.Builder builder = StringSchema.builder();
    final Map<String, Object> props = new HashMap<>();
    if (minLength == maxLength && minLength > 0) {
      props.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
      props.put(FLINK_MIN_LENGTH, minLength);
      props.put(FLINK_MAX_LENGTH, maxLength);
    } else if (maxLength < MAX_LENGTH) {
      props.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
      props.put(FLINK_MAX_LENGTH, maxLength);
    }
    props.put(CONNECT_TYPE_PROP, CONNECT_TYPE_BYTES);
    return builder.unprocessedProperties(props);
  }

  private static Map<String, Object> getDecimalProperties(Schema schema) {
    final Map<String, Object> props = new HashMap<>();
    props.put(CONNECT_TYPE_PROP, CONNECT_TYPE_BYTES);
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put(CONNECT_TYPE_DECIMAL_PRECISION, String.valueOf(schema.getPrecision()));
    parameters.put(CONNECT_TYPE_DECIMAL_SCALE, String.valueOf(schema.getScale()));
    props.put(CONNECT_PARAMETERS, parameters);
    return props;
  }

  private static org.everit.json.schema.Schema.Builder<?> convertMap(
      Schema schema, String rowName, FromLogicalContext<String> ctx) {
    final Schema keyType = schema.getKeyType();
    final Schema valueType = schema.getValueType();
    return convertMapLikeType(rowName, keyType, valueType, ctx);
  }

  private static org.everit.json.schema.Schema.Builder<?> convertMultiset(
      Schema schema, String rowName, FromLogicalContext<String> ctx) {
    final Schema keyType = schema.getElementType();
    final Schema valueType = Schema.create(Schema.Type.INT).setNullable(false);
    final org.everit.json.schema.Schema.Builder<?> builder =
        convertMapLikeType(rowName, keyType, valueType, ctx);
    builder.unprocessedProperties.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
    builder.unprocessedProperties.put(FLINK_TYPE_PROP, FLINK_TYPE_MULTISET);
    return builder;
  }

  private static org.everit.json.schema.Schema.Builder<?> convertMapLikeType(
      String rowName, Schema keyType, Schema valueType, FromLogicalContext<String> ctx) {
    boolean isCharacterString =
        keyType.getType() == Schema.Type.VARCHAR || keyType.getType() == Schema.Type.CHAR;
    if (isCharacterString) {
      final ObjectSchema.Builder builder = ObjectSchema.builder()
          .schemaOfAdditionalProperties(fromLogicalTypeRaw(valueType, rowName, ctx));
      builder.unprocessedProperties.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
      int keyLength = keyType.getLength();
      if (keyLength < Schema.MAX_LENGTH && !ctx.isV1()) {
        // V1 (Flink-compat) drops key-length / CHAR-vs-VARCHAR info. logical.*
        // keys are emitted as top-level schema properties, not nested in
        // confluent:params.
        builder.unprocessedProperties.put(CommonConstants.LOGICAL_KEY_LENGTH_PROP, keyLength);
        if (keyType.getType() == Schema.Type.CHAR) {
          builder.unprocessedProperties.put(CommonConstants.LOGICAL_KEY_TYPE_PROP, "CHAR");
        }
      }
      return builder;
    } else {
      return connectCustomMap(rowName, keyType, valueType, ctx);
    }
  }

  private static org.everit.json.schema.Schema.Builder<?> connectCustomMap(
      String rowName, Schema keyType, Schema valueType, FromLogicalContext<String> ctx) {
    final ArraySchema.Builder builder = ArraySchema.builder()
        .allItemSchema(
            ObjectSchema.builder()
                .addPropertySchema("key", fromLogicalTypeRaw(keyType, rowName + "_key", ctx))
                .addPropertySchema("value", fromLogicalTypeRaw(valueType, rowName + "_value", ctx))
                .additionalProperties(false)
                .build());
    builder.unprocessedProperties.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
    return builder;
  }

  private static org.everit.json.schema.Schema.Builder<?> convertTime(int precision) {
    if (precision < 0 || precision > 9) {
      throw new ValidationException(
          "TIME precision must be in [0, 9], got " + precision);
    }
    final Map<String, Object> properties = new HashMap<>();
    properties.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT32);
    if (precision != 3) {
      properties.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
      properties.put(FLINK_PRECISION, precision);
    }
    return NumberSchema.builder()
        .title(CONNECT_TYPE_TIME)
        .unprocessedProperties(properties);
  }

  private static org.everit.json.schema.Schema.Builder<?> convertTimestamp(
      int precision, boolean isLocalTimestamp) {
    if (precision < 0 || precision > 9) {
      throw new ValidationException(
          (isLocalTimestamp ? "TIMESTAMP" : "TIMESTAMP_LTZ")
              + " precision must be in [0, 9], got " + precision);
    }
    final Map<String, Object> properties = new HashMap<>();
    properties.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT64);
    if (precision != 3) {
      properties.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
      properties.put(FLINK_PRECISION, precision);
    }
    org.everit.json.schema.Schema.Builder<NumberSchema> builder = NumberSchema.builder();
    if (!isLocalTimestamp) {
      builder = builder.title(CONNECT_TYPE_TIMESTAMP);
    } else {
      properties.put(FLINK_PROPERTY_VERSION, FLINK_PROPERTY_CURRENT_VERSION);
      properties.put(FLINK_TYPE_PROP, FLINK_TYPE_TIMESTAMP);
    }
    return builder.unprocessedProperties(properties);
  }

  private static void addSchemaTags(
      org.everit.json.schema.Schema.Builder<?> builder, Schema schema) {
    if (!schema.getTags().isEmpty()) {
      builder.unprocessedProperties.put("confluent:tags", schema.getTags());
    }
  }

  private static void addEnumValueMeta(
      org.everit.json.schema.Schema.Builder<?> builder, Schema schema) {
    List<Map<String, Object>> enumMeta = new ArrayList<>();
    boolean hasMeta = false;
    for (EnumValue ev : schema.getEnumValues()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("name", ev.getSymbol());
      if (ev.getDoc() != null) {
        entry.put("doc", ev.getDoc());
        hasMeta = true;
      }
      if (!ev.getParams().isEmpty()) {
        entry.put("params", ev.getParams());
        hasMeta = true;
      }
      enumMeta.add(entry);
    }
    if (hasMeta) {
      builder.unprocessedProperties.put("confluent:enum", enumMeta);
    }
  }

  private static void addSchemaParams(
      org.everit.json.schema.Schema.Builder<?> builder, Schema schema) {
    if (!schema.getParams().isEmpty()) {
      builder.unprocessedProperties.put("confluent:params", schema.getParams());
    }
  }

  /**
   * Emit struct-level CHECK rules as a {@code confluent:rules} array property
   * on the JSON Schema object. See {@link #buildRulesWire} for the wire shape.
   */
  private static void addSchemaRules(
      org.everit.json.schema.Schema.Builder<?> builder, Schema schema) {
    if (!schema.getRules().isEmpty()) {
      builder.unprocessedProperties.put(
          "confluent:rules", buildRulesWire(schema.getRules()));
    }
  }

  /**
   * Build the wire form for a list of CHECK rules: a JSON array of objects
   * {@code {name?, doc?, expr, sql}} (only non-null fields included).
   */
  private static List<Map<String, Object>> buildRulesWire(
      List<io.confluent.kafka.schemaregistry.type.logical.Rule> rules) {
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
    return wire;
  }

  private static org.everit.json.schema.Schema.Builder<CombinedSchema> nullableSchema(
      org.everit.json.schema.Schema schema) {
    // If the wrapped schema is itself a oneOf/anyOf union, splice null into
    // its members rather than nest. Produces oneOf:[null, A, B] instead of
    // oneOf:[null, oneOf:[A, B]] — the canonical flat form most JSON Schema
    // tools emit, and structurally consistent with the Avro converter.
    if (schema instanceof CombinedSchema) {
      CombinedSchema inner = (CombinedSchema) schema;
      CombinedSchema.ValidationCriterion criterion = inner.getCriterion();
      if (criterion == CombinedSchema.ONE_CRITERION
          || criterion == CombinedSchema.ANY_CRITERION) {
        CombinedSchema.Builder builder = CombinedSchema.builder()
            .criterion(criterion)
            .subschema(NullSchema.INSTANCE);
        for (org.everit.json.schema.Schema sub : inner.getSubschemas()) {
          builder.subschema(sub);
        }
        // Preserve confluent:union branch metadata (and any other unprocessed
        // properties) on the merged outer schema so the reader can recover
        // branch names/docs/params.
        if (!inner.getUnprocessedProperties().isEmpty()) {
          builder.unprocessedProperties(inner.getUnprocessedProperties());
        }
        return builder;
      }
    }
    return CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NullSchema.INSTANCE)
        .subschema(schema);
  }
}
