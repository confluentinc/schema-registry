/*
 * Copyright 2023 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.json.schema;

import static io.confluent.kafka.schemaregistry.json.JsonSchema.DEFAULT_BASE_URI;
import static io.confluent.kafka.schemaregistry.json.schema.SchemaUtils.isSynthetic;
import static io.confluent.kafka.schemaregistry.json.schema.SchemaUtils.merge;
import static io.confluent.kafka.schemaregistry.json.schema.SchemaUtils.toBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.erosb.jsonsKema.AdditionalPropertiesSchema;
import com.github.erosb.jsonsKema.AllOfSchema;
import com.github.erosb.jsonsKema.AnyOfSchema;
import com.github.erosb.jsonsKema.CompositeSchema;
import com.github.erosb.jsonsKema.ConstSchema;
import com.github.erosb.jsonsKema.ContainsSchema;
import com.github.erosb.jsonsKema.DependentRequiredSchema;
import com.github.erosb.jsonsKema.DependentSchemasSchema;
import com.github.erosb.jsonsKema.DynamicRefSchema;
import com.github.erosb.jsonsKema.EnumSchema;
import com.github.erosb.jsonsKema.ExclusiveMaximumSchema;
import com.github.erosb.jsonsKema.ExclusiveMinimumSchema;
import com.github.erosb.jsonsKema.FalseSchema;
import com.github.erosb.jsonsKema.FormatSchema;
import com.github.erosb.jsonsKema.IJsonArray;
import com.github.erosb.jsonsKema.IJsonBoolean;
import com.github.erosb.jsonsKema.IJsonNull;
import com.github.erosb.jsonsKema.IJsonNumber;
import com.github.erosb.jsonsKema.IJsonObject;
import com.github.erosb.jsonsKema.IJsonString;
import com.github.erosb.jsonsKema.IJsonValue;
import com.github.erosb.jsonsKema.IfThenElseSchema;
import com.github.erosb.jsonsKema.ItemsSchema;
import com.github.erosb.jsonsKema.JsonArray;
import com.github.erosb.jsonsKema.JsonBoolean;
import com.github.erosb.jsonsKema.JsonNull;
import com.github.erosb.jsonsKema.JsonNumber;
import com.github.erosb.jsonsKema.JsonString;
import com.github.erosb.jsonsKema.JsonVisitor;
import com.github.erosb.jsonsKema.MaxItemsSchema;
import com.github.erosb.jsonsKema.MaxLengthSchema;
import com.github.erosb.jsonsKema.MaxPropertiesSchema;
import com.github.erosb.jsonsKema.MaximumSchema;
import com.github.erosb.jsonsKema.MinItemsSchema;
import com.github.erosb.jsonsKema.MinLengthSchema;
import com.github.erosb.jsonsKema.MinPropertiesSchema;
import com.github.erosb.jsonsKema.MinimumSchema;
import com.github.erosb.jsonsKema.MultiTypeSchema;
import com.github.erosb.jsonsKema.MultipleOfSchema;
import com.github.erosb.jsonsKema.NotSchema;
import com.github.erosb.jsonsKema.OneOfSchema;
import com.github.erosb.jsonsKema.PatternSchema;
import com.github.erosb.jsonsKema.PrefixItemsSchema;
import com.github.erosb.jsonsKema.PropertyNamesSchema;
import com.github.erosb.jsonsKema.ReferenceSchema;
import com.github.erosb.jsonsKema.Regexp;
import com.github.erosb.jsonsKema.RequiredSchema;
import com.github.erosb.jsonsKema.Schema;
import com.github.erosb.jsonsKema.SchemaVisitor;
import com.github.erosb.jsonsKema.TrueSchema;
import com.github.erosb.jsonsKema.TypeSchema;
import com.github.erosb.jsonsKema.UnevaluatedItemsSchema;
import com.github.erosb.jsonsKema.UnevaluatedPropertiesSchema;
import com.github.erosb.jsonsKema.UniqueItemsSchema;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import kotlin.Pair;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.loader.OrgJsonUtil;
import org.json.JSONArray;
import org.json.JSONObject;

public class SchemaTranslator extends SchemaVisitor<SchemaTranslator.SchemaContext> {

  private static final Object NONE_MARKER = new Object();

  private static final ObjectMapper objectMapper = Jackson.newObjectMapper();

  private final Map<Schema, org.everit.json.schema.Schema.Builder<?>> schemaMapping;
  private final Deque<Pair<org.everit.json.schema.ReferenceSchema, Schema>> refMapping;

  public SchemaTranslator() {
    this.schemaMapping = new IdentityHashMap<>();
    this.refMapping = new ArrayDeque<>();
  }

  @Override
  public SchemaContext accumulate(Schema parent,
      SchemaContext previous, SchemaContext current) {
    if (previous == null) {
      return current;
    }
    if (current == null) {
      return previous;
    }
    return previous.join(parent, current);
  }

  @Override
  public SchemaContext identity(Schema parent) {
    if (parent instanceof AllOfSchema) {
      return new SchemaContext(parent, CombinedSchema.allOf(Collections.emptyList()));
    }
    if (parent instanceof AnyOfSchema) {
      return new SchemaContext(parent, CombinedSchema.anyOf(Collections.emptyList()));
    }
    if (parent instanceof OneOfSchema) {
      return new SchemaContext(parent, CombinedSchema.oneOf(Collections.emptyList()));
    }
    return super.identity(parent);
  }

  /*
   * Note: we only call accept() in the code below when the schema has multiple children
   * and calling super.visit would not return a proper combined schema.
   */
  
  @Override
  public SchemaContext visitAdditionalPropertiesSchema(
      AdditionalPropertiesSchema schema) {
    SchemaContext ctx = super.visitAdditionalPropertiesSchema(schema);
    assert ctx != null;
    ObjectSchema.Builder builder = ObjectSchema.builder().requiresObject(false);
    if (ctx.schemaBuilder() instanceof org.everit.json.schema.FalseSchema.Builder) {
      builder.additionalProperties(false);
    } else if (!(ctx.schemaBuilder() instanceof org.everit.json.schema.TrueSchema.Builder)) {
      builder.schemaOfAdditionalProperties(ctx.schema());
    }
    return new SchemaContext(schema, builder);
  }

  @Override
  public SchemaContext visitAllOfSchema(AllOfSchema schema) {
    return super.visitAllOfSchema(schema);
  }

  @Override
  public SchemaContext visitAnyOfSchema(AnyOfSchema schema) {
    return super.visitAnyOfSchema(schema);
  }

  @Override
  public SchemaContext visitChildren(Schema parent) {
    SchemaContext ctx = super.visitChildren(parent);
    return ctx != null
        ? ctx
        : new SchemaContext(
            parent, CombinedSchema.allOf(Collections.emptyList()).isSynthetic(true));
  }

  @Override
  public SchemaContext visitCompositeSchema(CompositeSchema schema) {
    SchemaContext ctx = super.visitCompositeSchema(schema);
    if (ctx == null) {
      return new SchemaContext(
          schema, CombinedSchema.allOf(Collections.emptyList()).isSynthetic(true));
    }
    org.everit.json.schema.Schema ctxSchema = ctx.schema();
    if (ctxSchema instanceof CombinedSchema) {
      CombinedSchema combinedSchema = (CombinedSchema) ctxSchema;
      if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION
          && isSynthetic(combinedSchema)) {
        if (combinedSchema.getSubschemas().isEmpty()) {
          ctx = new SchemaContext(ctx.source(), EmptySchema.builder());
        } else if (combinedSchema.getSubschemas().size() == 1) {
          ctx = new SchemaContext(ctx.source(),
              SchemaUtils.schemaToBuilder(combinedSchema.getSubschemas().iterator().next()));
        }
      }
    }
    if (schema.getId() != null) {
      ctx.schemaBuilder().id(schema.getId().getValue());
    }
    if (schema.getTitle() != null) {
      ctx.schemaBuilder().title(schema.getTitle().getValue());
    }
    if (schema.getDescription() != null) {
      ctx.schemaBuilder().description(schema.getDescription().getValue());
    }
    if (schema.getDefault() != null) {
      ctx.schemaBuilder().defaultValue(schema.getDefault().accept(new JsonValueVisitor()));
    }
    if (!schema.getUnprocessedProperties().isEmpty()) {
      Map<String, Object> unprocessed = new HashMap<>();
      for (Map.Entry<IJsonString, IJsonValue> entry :
          schema.getUnprocessedProperties().entrySet()) {
        String key = entry.getKey().getValue();
        IJsonValue value = entry.getValue();
        Object primitiveValue = NONE_MARKER;
        if (value instanceof JsonBoolean) {
          primitiveValue = ((JsonBoolean) value).getValue();
        } else if (value instanceof JsonNull) {
          primitiveValue = null;
        } else if (value instanceof JsonNumber) {
          primitiveValue = ((JsonNumber) value).getValue();
        } else if (value instanceof JsonString) {
          primitiveValue = ((JsonString) value).getValue();
        }
        if (primitiveValue != NONE_MARKER) {
          unprocessed.put(key, primitiveValue);
        } else {
          if (value instanceof JsonArray) {
            unprocessed.put(
                key, OrgJsonUtil.toList(objectMapper.convertValue(value, JSONArray.class)));
          } else {
            unprocessed.put(
                key, OrgJsonUtil.toMap(objectMapper.convertValue(value, JSONObject.class)));
          }
        }
      }
      ctx.schemaBuilder().unprocessedProperties(unprocessed);
    }
    return ctx;
  }

  @Override
  public SchemaContext visitConstSchema(ConstSchema schema) {
    return new SchemaContext(schema, org.everit.json.schema.ConstSchema.builder()
        .permittedValue(schema.getConstant().accept(new JsonValueVisitor())));
  }

  @Override
  public SchemaContext visitContainsSchema(ContainsSchema schema) {
    SchemaContext ctx = super.visitContainsSchema(schema);
    assert ctx != null;
    return new SchemaContext(schema, ArraySchema.builder().requiresArray(false)
        .containsItemSchema(ctx.schema()));
  }

  @Override
  public SchemaContext visitDependentRequiredSchema(
      DependentRequiredSchema schema) {
    ObjectSchema.Builder builder = ObjectSchema.builder().requiresObject(false);
    for (Map.Entry<String, List<String>> entry : schema.getDependentRequired().entrySet()) {
      for (String s : entry.getValue()) {
        builder.propertyDependency(entry.getKey(), s);
      }
    }
    return new SchemaContext(schema, builder);
  }

  @Override
  public SchemaContext visitDependentSchemas(DependentSchemasSchema schema) {
    ObjectSchema.Builder builder = ObjectSchema.builder().requiresObject(false);
    for (Map.Entry<String, Schema> entry : schema.getDependentSchemas().entrySet()) {
      SchemaContext ctx = entry.getValue().accept(this);
      assert ctx != null;
      builder.schemaDependency(entry.getKey(), ctx.schema());
    }
    return new SchemaContext(schema, builder);
  }

  @Override
  public SchemaContext visitDynamicRefSchema(DynamicRefSchema schema) {
    // ignore dynamic refs
    return super.visitDynamicRefSchema(schema);
  }

  @Override
  public SchemaContext visitEnumSchema(EnumSchema schema) {
    return new SchemaContext(schema, org.everit.json.schema.EnumSchema.builder()
        .possibleValues(schema.getPotentialValues().stream()
            .map(v -> v.accept(new JsonValueVisitor()))
            .collect(Collectors.toList())));
  }

  @Override
  public SchemaContext visitExclusiveMaximumSchema(
      ExclusiveMaximumSchema schema) {
    return new SchemaContext(schema, NumberSchema.builder().requiresNumber(false)
        .exclusiveMaximum(schema.getMaximum()));
  }

  @Override
  public SchemaContext visitExclusiveMinimumSchema(
      ExclusiveMinimumSchema schema) {
    return new SchemaContext(schema, NumberSchema.builder().requiresNumber(false)
        .exclusiveMinimum(schema.getMinimum()));
  }

  @Override
  public SchemaContext visitFalseSchema(FalseSchema schema) {
    return new SchemaContext(schema, org.everit.json.schema.FalseSchema.builder());
  }

  @Override
  public SchemaContext visitFormatSchema(FormatSchema schema) {
    // ignore formats
    return super.visitFormatSchema(schema);
  }

  @Override
  public SchemaContext visitIfThenElseSchema(IfThenElseSchema schema) {
    org.everit.json.schema.Schema ifSchema = null;
    if (schema.getIfSchema() != null) {
      SchemaContext ctx = schema.getIfSchema().accept(this);
      assert ctx != null;
      ifSchema = ctx.schema();
    }
    org.everit.json.schema.Schema thenSchema = null;
    if (schema.getThenSchema() != null) {
      SchemaContext ctx = schema.getThenSchema().accept(this);
      assert ctx != null;
      thenSchema = ctx.schema();
    }
    org.everit.json.schema.Schema elseSchema = null;
    if (schema.getElseSchema() != null) {
      SchemaContext ctx = schema.getElseSchema().accept(this);
      assert ctx != null;
      elseSchema = ctx.schema();
    }
    return new SchemaContext(schema, ConditionalSchema.builder()
        .ifSchema(ifSchema)
        .thenSchema(thenSchema)
        .elseSchema(elseSchema));
  }

  @Override
  public SchemaContext visitItemsSchema(ItemsSchema schema) {
    SchemaContext ctx = super.visitItemsSchema(schema);
    assert ctx != null;
    ArraySchema.Builder builder = ArraySchema.builder().requiresArray(false);
    if (ctx.schemaBuilder() instanceof org.everit.json.schema.FalseSchema.Builder) {
      builder.additionalItems(false);
    } else if (!(ctx.schemaBuilder() instanceof org.everit.json.schema.TrueSchema.Builder)) {
      if (schema.getPrefixItemCount() == 0) {
        builder.allItemSchema(ctx.schema());
      } else {
        builder.schemaOfAdditionalItems(ctx.schema());
      }
    }
    return new SchemaContext(schema, builder);
  }

  @Override
  public SchemaContext visitMaxItemsSchema(MaxItemsSchema schema) {
    return new SchemaContext(schema, ArraySchema.builder().requiresArray(false)
        .maxItems(schema.getMaxItems().intValue()));
  }

  @Override
  public SchemaContext visitMaxLengthSchema(MaxLengthSchema schema) {
    return new SchemaContext(schema, StringSchema.builder().requiresString(false)
        .maxLength(schema.getMaxLength()));
  }

  @Override
  public SchemaContext visitMaxPropertiesSchema(MaxPropertiesSchema schema) {
    return new SchemaContext(schema, ObjectSchema.builder().requiresObject(false)
        .maxProperties(schema.getMaxProperties().intValue()));
  }

  @Override
  public SchemaContext visitMaximumSchema(MaximumSchema schema) {
    return new SchemaContext(schema, NumberSchema.builder().requiresNumber(false)
        .maximum(schema.getMaximum()));
  }

  @Override
  public SchemaContext visitMinItemsSchema(MinItemsSchema schema) {
    return new SchemaContext(schema, ArraySchema.builder().requiresArray(false)
        .minItems(schema.getMinItems().intValue()));
  }

  @Override
  public SchemaContext visitMinLengthSchema(MinLengthSchema schema) {
    return new SchemaContext(schema, StringSchema.builder().requiresString(false)
        .minLength(schema.getMinLength()));
  }

  @Override
  public SchemaContext visitMinPropertiesSchema(MinPropertiesSchema schema) {
    return new SchemaContext(schema, ObjectSchema.builder().requiresObject(false)
        .minProperties(schema.getMinProperties().intValue()));
  }

  @Override
  public SchemaContext visitMinimumSchema(MinimumSchema schema) {
    return new SchemaContext(schema, NumberSchema.builder().requiresNumber(false)
        .minimum(schema.getMinimum()));
  }

  @Override
  public SchemaContext visitMultiTypeSchema(MultiTypeSchema schema) {
    List<org.everit.json.schema.Schema> schemas = schema.getTypes().getElements().stream()
        .map(json -> typeToSchema(json.requireString().getValue()).build())
        .collect(Collectors.toList());
    return new SchemaContext(schema, CombinedSchema.anyOf(schemas));
  }

  @Override
  public SchemaContext visitMultipleOfSchema(MultipleOfSchema schema) {
    return new SchemaContext(schema, NumberSchema.builder().requiresNumber(false)
        .multipleOf(schema.getDenominator()));
  }

  @Override
  public SchemaContext visitNotSchema(NotSchema schema) {
    SchemaContext ctx = super.visitNotSchema(schema);
    assert ctx != null;
    return new SchemaContext(schema, org.everit.json.schema.NotSchema.builder()
        .mustNotMatch(ctx.schema()));
  }

  @Override
  public SchemaContext visitOneOfSchema(OneOfSchema schema) {
    return super.visitOneOfSchema(schema);
  }

  @Override
  public SchemaContext visitPatternPropertySchema(Regexp pattern,
      Schema schema) {
    SchemaContext ctx = schema.accept(this);
    assert ctx != null;
    return new SchemaContext(schema, ObjectSchema.builder().requiresObject(false)
        .patternProperty(pattern.toString(), ctx.schema()));
  }

  @Override
  public SchemaContext visitPatternSchema(PatternSchema schema) {
    return new SchemaContext(schema, StringSchema.builder().requiresString(false)
        .pattern(schema.getPattern().toString()));
  }

  @Override
  public SchemaContext visitPrefixItemsSchema(PrefixItemsSchema schema) {
    ArraySchema.Builder builder = ArraySchema.builder().requiresArray(false);
    for (Schema s : schema.getPrefixSchemas()) {
      SchemaContext ctx = s.accept(this);
      assert ctx != null;
      builder.addItemSchema(ctx.schema());
    }
    return new SchemaContext(schema, builder);
  }

  @Override
  public SchemaContext visitPropertyNamesSchema(
      PropertyNamesSchema propertyNamesSchema) {
    SchemaContext ctx = super.visitPropertyNamesSchema(propertyNamesSchema);
    assert ctx != null;
    return new SchemaContext(propertyNamesSchema, ObjectSchema.builder().requiresObject(false)
        .propertyNameSchema(ctx.schema()));
  }

  @Override
  public SchemaContext visitPropertySchema(String property,
      Schema schema) {
    SchemaContext ctx = schema.accept(this);
    assert ctx != null;
    return new SchemaContext(schema, ObjectSchema.builder().requiresObject(false)
        .addPropertySchema(property, ctx.schema()));
  }

  @Override
  public SchemaContext visitReferenceSchema(ReferenceSchema schema) {
    Schema referredSchema = schema.getReferredSchema();
    String refValue = schema.getRef();
    if (refValue.startsWith(DEFAULT_BASE_URI)) {
      refValue = refValue.substring(DEFAULT_BASE_URI.length());
    }
    org.everit.json.schema.ReferenceSchema.Builder ref =
        org.everit.json.schema.ReferenceSchema.builder()
            .refValue(refValue);
    this.refMapping.offer(new Pair<>(ref.build(), referredSchema));
    return new SchemaContext(schema, ref);
  }

  @Override
  public SchemaContext visitRequiredSchema(RequiredSchema schema) {
    ObjectSchema.Builder builder = ObjectSchema.builder().requiresObject(false);
    for (String p : schema.getRequiredProperties()) {
      builder.addRequiredProperty(p);
    }
    return new SchemaContext(schema, builder);
  }

  @Override
  public SchemaContext visitTrueSchema(TrueSchema schema) {
    return new SchemaContext(schema, org.everit.json.schema.TrueSchema.builder());
  }

  @Override
  public SchemaContext visitTypeSchema(TypeSchema schema) {
    return new SchemaContext(schema, typeToSchema(schema.getType().getValue()));
  }

  @Override
  public SchemaContext visitUnevaluatedItemsSchema(
      UnevaluatedItemsSchema schema) {
    SchemaContext ctx = super.visitUnevaluatedItemsSchema(schema);
    assert ctx != null;
    ArraySchema.Builder builder = ArraySchema.builder().requiresArray(false);
    builder.unprocessedProperties(Collections.singletonMap("unevaluatedItems", ctx.schema()));
    return new SchemaContext(schema, builder);
  }

  @Override
  public SchemaContext visitUnevaluatedPropertiesSchema(
      UnevaluatedPropertiesSchema schema) {
    SchemaContext ctx = super.visitUnevaluatedPropertiesSchema(schema);
    assert ctx != null;
    ObjectSchema.Builder builder = ObjectSchema.builder().requiresObject(false);
    builder.unprocessedProperties(Collections.singletonMap("unevaluatedProperties", ctx.schema()));
    return new SchemaContext(schema, builder);
  }

  @Override
  public SchemaContext visitUniqueItemsSchema(UniqueItemsSchema schema) {
    return new SchemaContext(schema, ArraySchema.builder().requiresArray(false)
        .uniqueItems(schema.getUnique()));
  }

  private org.everit.json.schema.Schema.Builder<?> typeToSchema(String type) {
    switch (type) {
      case "string":
        return StringSchema.builder();
      case "integer":
        return NumberSchema.builder().requiresInteger(true);
      case "number":
        return NumberSchema.builder();
      case "boolean":
        return BooleanSchema.builder();
      case "null":
        return org.everit.json.schema.NullSchema.builder();
      case "array":
        return ArraySchema.builder();
      case "object":
        return ObjectSchema.builder();
      default:
        throw new IllegalArgumentException();
    }
  }

  static class JsonValueVisitor implements JsonVisitor<Object> {

    @Override
    public Object accumulate(Object previous, Object current) {
      return current != null ? current : previous;
    }

    @Override
    public Object identity() {
      return null;
    }

    @Override
    public Object visitArray(IJsonArray<?> arr) {
      return objectMapper.convertValue(arr, JSONArray.class);
    }

    @Override
    public Object visitBoolean(IJsonBoolean bool) {
      return bool.getValue();
    }

    @Override
    public Object visitNull(IJsonNull nil) {
      return null;
    }

    @Override
    public Object visitNumber(IJsonNumber num) {
      return num.getValue();
    }

    @Override
    public Object visitObject(IJsonObject<?, ?> obj) {
      return objectMapper.convertValue(obj, JSONObject.class);
    }

    @Override
    public Object visitString(IJsonString str) {
      return str.getValue();
    }
  }

  public class SchemaContext implements AutoCloseable {

    private final Schema source;
    private final org.everit.json.schema.Schema.Builder<?> target;

    public SchemaContext(Schema source, org.everit.json.schema.Schema.Builder<?> target) {
      if (target == null) {
        throw new NullPointerException();
      }
      this.source = source;
      this.target = target;
      SchemaTranslator.this.schemaMapping.put(source, target);
    }

    public Schema source() {
      return source;
    }

    public org.everit.json.schema.Schema.Builder<?> schemaBuilder() {
      return target;
    }

    public org.everit.json.schema.Schema schema() {
      return target.build();
    }

    public SchemaContext join(Schema parent, SchemaContext ctx) {
      return new SchemaContext(parent, join(parent, ctx.schema()));
    }

    public org.everit.json.schema.Schema.Builder<?> join(
        Schema parent, org.everit.json.schema.Schema current) {
      org.everit.json.schema.Schema schema = schema();
      if (schema instanceof ArraySchema && current instanceof ArraySchema) {
        return merge(toBuilder((ArraySchema) schema), (ArraySchema) current);
      }
      if (schema instanceof NumberSchema && current instanceof NumberSchema) {
        return merge(toBuilder((NumberSchema) schema), (NumberSchema) current);
      }
      if (schema instanceof ObjectSchema && current instanceof ObjectSchema) {
        return merge(toBuilder((ObjectSchema) schema), (ObjectSchema) current);
      }
      if (schema instanceof StringSchema && current instanceof StringSchema) {
        return merge(toBuilder((StringSchema) schema), (StringSchema) current);
      }
      if (parent instanceof AllOfSchema) {
        return CombinedSchema.allOf(
            concat(((CombinedSchema) schema).getSubschemas(), flatten(current)));
      }
      if (parent instanceof AnyOfSchema) {
        return CombinedSchema.anyOf(
            concat(((CombinedSchema) schema).getSubschemas(), flatten(current)));
      }
      if (parent instanceof OneOfSchema) {
        return CombinedSchema.oneOf(
            concat(((CombinedSchema) schema).getSubschemas(), flatten(current)));
      }
      return CombinedSchema.allOf(accumulate(concat(flatten(schema), flatten(current))))
          .isSynthetic(true);
    }

    private List<org.everit.json.schema.Schema> accumulate(
        List<org.everit.json.schema.Schema> schemas) {
      List<org.everit.json.schema.Schema> result = new ArrayList<>();
      for (int i = 0; i < schemas.size(); i++) {
        accumulate(result, schemas.get(i));
      }
      return result;
    }

    private void accumulate(
        List<org.everit.json.schema.Schema> product, org.everit.json.schema.Schema current) {
      for (int i = 0; i < product.size(); i++) {
        org.everit.json.schema.Schema schema = product.get(i);
        if (schema.getClass() == current.getClass()) {
          if (schema instanceof ArraySchema) {
            product.set(i, merge(toBuilder((ArraySchema) schema), (ArraySchema) current).build());
            return;
          }
          if (schema instanceof NumberSchema) {
            product.set(i, merge(toBuilder((NumberSchema) schema), (NumberSchema) current).build());
            return;
          }
          if (schema instanceof ObjectSchema) {
            product.set(i, merge(toBuilder((ObjectSchema) schema), (ObjectSchema) current).build());
            return;
          }
          if (schema instanceof StringSchema) {
            product.set(i, merge(toBuilder((StringSchema) schema), (StringSchema) current).build());
            return;
          }
        }
      }
      product.add(current);
    }

    private List<org.everit.json.schema.Schema> concat(
        Collection<org.everit.json.schema.Schema> previous,
        Collection<org.everit.json.schema.Schema> current
    ) {
      List<org.everit.json.schema.Schema> schemas = new ArrayList<>(previous);
      schemas.addAll(current);
      return schemas;
    }

    private List<org.everit.json.schema.Schema> flatten(
        org.everit.json.schema.Schema schema) {
      if (schema instanceof CombinedSchema) {
        CombinedSchema combinedSchema = (CombinedSchema) schema;
        if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION
            && isSynthetic(combinedSchema)) {
          // Unwrap the synthetic allOf
          return new ArrayList<>(combinedSchema.getSubschemas());
        }
      }
      return Collections.singletonList(schema);
    }

    @Override
    public void close() {
      while (!SchemaTranslator.this.refMapping.isEmpty()) {
        Pair<org.everit.json.schema.ReferenceSchema, Schema> pair =
            SchemaTranslator.this.refMapping.poll();

        org.everit.json.schema.ReferenceSchema refSchema = pair.component1();
        Schema oldReferredSchema = pair.component2();
        org.everit.json.schema.Schema.Builder<?> referredSchema =
            SchemaTranslator.this.schemaMapping.get(oldReferredSchema);
        if (referredSchema != null) {
          refSchema.setReferredSchema(referredSchema.build());
        } else {
          SchemaContext ctx = oldReferredSchema.accept(SchemaTranslator.this);
          assert ctx != null;
          ctx.close();
          refSchema.setReferredSchema(ctx.schema());
        }
      }
    }
  }
}
