/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.json;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.TrueSchema;

public class JsonSchemaComparator implements Comparator<Schema> {

  public enum SchemaType {
    // Keep in alphabetical order
    ARRAY(ArraySchema.class),
    BOOLEAN(BooleanSchema.class),
    COMBINED(CombinedSchema.class),
    CONDITIONAL(ConditionalSchema.class),
    CONST(ConstSchema.class),
    EMPTY(EmptySchema.class),
    ENUM(EnumSchema.class),
    FALSE(FalseSchema.class),
    NOT(NotSchema.class),
    NULL(NullSchema.class),
    NUMBER(NumberSchema.class),
    OBJECT(ObjectSchema.class),
    REFERENCE(ReferenceSchema.class),
    STRING(StringSchema.class),
    TRUE(TrueSchema.class);

    Class<? extends Schema> cls;

    SchemaType(Class<? extends Schema> cls) {
      this.cls = cls;
    }

    public static SchemaType forClass(Class<? extends Schema> cls) {
      for (SchemaType value : values()) {
        if (value.cls.equals(cls)) {
          return value;
        }
      }
      // Check whether subclass of the known types, such as CombinedSchemaExt
      for (SchemaType value : values()) {
        if (value.cls.isAssignableFrom(cls)) {
          return value;
        }
      }
      throw new IllegalArgumentException("Unknown schema type : " + cls);
    }
  }

  private static final Comparator<String> STRING_COMPARATOR =
      Comparator.<String>nullsFirst(Comparator.<String>naturalOrder());

  @Override
  public int compare(Schema schema1, Schema schema2) {
    return compare(schema1, schema2, null);
  }

  /**
   * Compares two schemas. Everit schemas are immutable apart from a ReferenceSchema's referred
   * schema, so every cycle in the schema graph passes through a pair of references. Reference
   * pairs are therefore the only state tracked: a pair already being compared higher up the
   * stack is treated as equal, and a finished pair's result is reused, which bounds the work on
   * shared and cyclic references. {@code refs} is created on the first reference pair and scoped
   * to one top-level comparison, which keeps this comparator stateless and thread-safe.
   */
  private int compare(Schema schema1, Schema schema2, RefState refs) {
    if (schema1 == schema2) {
      return 0;
    }
    if (schema1 == null) {
      if (schema2 != null) {
        return -1;
      } else {
        return 0;
      }
    } else if (schema2 == null) {
      return 1;
    }

    SchemaType schemaType1 = SchemaType.forClass(schema1.getClass());
    SchemaType schemaType2 = SchemaType.forClass(schema2.getClass());

    int cmp = schemaType1.compareTo(schemaType2);
    if (cmp != 0) {
      return cmp;
    }
    cmp = STRING_COMPARATOR.compare(schema1.getTitle(), schema2.getTitle());
    if (cmp != 0) {
      return cmp;
    }
    cmp = STRING_COMPARATOR.compare(schema1.getDescription(), schema2.getDescription());
    if (cmp != 0) {
      return cmp;
    }
    String def1 = schema1.getDefaultValue() != null ? schema1.getDefaultValue().toString() : null;
    String def2 = schema2.getDefaultValue() != null ? schema2.getDefaultValue().toString() : null;
    cmp = STRING_COMPARATOR.compare(def1, def2);
    if (cmp != 0) {
      return cmp;
    }

    switch (schemaType1) {
      case STRING:
      case NUMBER:
      case BOOLEAN:
      case NULL:
        return 0;
      case CONST:
        ConstSchema const1 = (ConstSchema) schema1;
        ConstSchema const2 = (ConstSchema) schema2;
        return STRING_COMPARATOR.compare(
          const1.getPermittedValue().toString(), const2.getPermittedValue().toString());
      case ENUM:
        EnumSchema enum1 = (EnumSchema) schema1;
        EnumSchema enum2 = (EnumSchema) schema2;
        return compareCollections(enum1.getPossibleValues(), enum2.getPossibleValues());
      case COMBINED:
        CombinedSchema comb1 = (CombinedSchema) schema1;
        CombinedSchema comb2 = (CombinedSchema) schema2;
        cmp = STRING_COMPARATOR.compare(getCriterion(comb1), getCriterion(comb2));
        if (cmp != 0) {
          return cmp;
        }
        cmp = comb1.getSubschemas().size() - comb2.getSubschemas().size();
        if (cmp != 0) {
          return cmp;
        }
        List<Schema> comb1Schemas = new ArrayList<>(comb1.getSubschemas());
        List<Schema> comb2Schemas = new ArrayList<>(comb2.getSubschemas());
        Comparator<Schema> subschemaComparator = (s1, s2) -> compare(s1, s2, refs);
        comb1Schemas.sort(subschemaComparator);
        comb2Schemas.sort(subschemaComparator);
        for (int i = 0; i < comb1Schemas.size(); i++) {
          cmp = compare(comb1Schemas.get(i), comb2Schemas.get(i), refs);
          if (cmp != 0) {
            return cmp;
          }
        }
        return 0;
      case NOT:
        NotSchema not1 = (NotSchema) schema1;
        NotSchema not2 = (NotSchema) schema2;
        return compare(not1.getMustNotMatch(), not2.getMustNotMatch(), refs);
      case CONDITIONAL:
        ConditionalSchema cond1 = (ConditionalSchema) schema1;
        ConditionalSchema cond2 = (ConditionalSchema) schema2;
        cmp = compare(cond1.getIfSchema().orElse(EmptySchema.INSTANCE),
          cond2.getIfSchema().orElse(EmptySchema.INSTANCE), refs);
        if (cmp != 0) {
          return cmp;
        }
        cmp = compare(cond1.getThenSchema().orElse(EmptySchema.INSTANCE),
          cond2.getThenSchema().orElse(EmptySchema.INSTANCE), refs);
        if (cmp != 0) {
          return cmp;
        }
        return compare(cond1.getElseSchema().orElse(EmptySchema.INSTANCE),
          cond2.getElseSchema().orElse(EmptySchema.INSTANCE), refs);
      case OBJECT:
        ObjectSchema obj1 = (ObjectSchema) schema1;
        ObjectSchema obj2 = (ObjectSchema) schema2;
        cmp = compareCollections(
          obj1.getPropertySchemas().keySet(), obj2.getPropertySchemas().keySet());
        if (cmp != 0) {
          return cmp;
        }
        return compareCollections(obj1.getRequiredProperties(), obj2.getRequiredProperties());
      case ARRAY:
        ArraySchema arr1 = (ArraySchema) schema1;
        ArraySchema arr2 = (ArraySchema) schema2;
        return compare(arr1.getAllItemSchema(), arr2.getAllItemSchema(), refs);
      case REFERENCE:
        ReferenceSchema ref1 = (ReferenceSchema) schema1;
        ReferenceSchema ref2 = (ReferenceSchema) schema2;
        return compareReferences(ref1, ref2, refs != null ? refs : new RefState());
      default:
        return 0;
    }
  }

  private int compareReferences(ReferenceSchema ref1, ReferenceSchema ref2, RefState refs) {
    JsonSchemaCancellation.throwIfInterrupted();
    SchemaPair pair = new SchemaPair(ref1, ref2);
    Integer cached = refs.results.get(pair);
    if (cached != null) {
      return cached;
    }
    if (!refs.inProgress.add(pair)) {
      return 0;
    }
    int cmp;
    try {
      cmp = compare(ref1.getReferredSchema(), ref2.getReferredSchema(), refs);
    } finally {
      refs.inProgress.remove(pair);
    }
    refs.results.put(pair, cmp);
    return cmp;
  }

  private int compareCollections(Collection<?> coll1, Collection<?> coll2) {
    int cmp = coll1.size() - coll2.size();
    if (cmp != 0) {
      return cmp;
    }
    List<String> list1 = coll1.stream()
        .map(Objects::toString)
        .collect(Collectors.toList());
    List<String> list2 = coll2.stream()
        .map(Objects::toString)
        .collect(Collectors.toList());
    list1.sort(STRING_COMPARATOR);
    list2.sort(STRING_COMPARATOR);
    for (int i = 0; i < list1.size(); i++) {
      cmp = STRING_COMPARATOR.compare(list1.get(i), list2.get(i));
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  public static String getCriterion(CombinedSchema schema) {
    if (schema.getCriterion() == CombinedSchema.ALL_CRITERION) {
      return "allof";
    } else if (schema.getCriterion() == CombinedSchema.ANY_CRITERION) {
      return "anyof";
    } else if (schema.getCriterion() == CombinedSchema.ONE_CRITERION) {
      return "oneof";
    } else {
      return null;
    }
  }

  private static final class RefState {
    private final Set<SchemaPair> inProgress = new HashSet<>();
    private final Map<SchemaPair, Integer> results = new HashMap<>();
  }

  /**
   * An ordered pair of schemas compared by identity.
   */
  private static final class SchemaPair {
    private final Schema first;
    private final Schema second;

    SchemaPair(Schema first, Schema second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SchemaPair)) {
        return false;
      }
      SchemaPair other = (SchemaPair) o;
      return first == other.first && second == other.second;
    }

    @Override
    public int hashCode() {
      return 31 * System.identityHashCode(first) + System.identityHashCode(second);
    }
  }
}
