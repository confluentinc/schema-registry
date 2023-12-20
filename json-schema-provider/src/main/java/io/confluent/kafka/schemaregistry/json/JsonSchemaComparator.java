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
import java.util.List;
import java.util.Objects;
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
      throw new IllegalArgumentException("Unknown schema type : " + cls);
    }
  }

  private static final Comparator<String> STRING_COMPARATOR =
      Comparator.<String>nullsFirst(Comparator.<String>naturalOrder());

  public int compare(Schema schema1, Schema schema2) {
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
        comb1Schemas.sort(this);
        comb2Schemas.sort(this);
        for (int i = 0; i < comb1Schemas.size(); i++) {
          cmp = compare(comb1Schemas.get(i), comb2Schemas.get(i));
          if (cmp != 0) {
            return cmp;
          }
        }
        return 0;
      case NOT:
        NotSchema not1 = (NotSchema) schema1;
        NotSchema not2 = (NotSchema) schema2;
        return compare(not1.getMustNotMatch(), not2.getMustNotMatch());
      case CONDITIONAL:
        ConditionalSchema cond1 = (ConditionalSchema) schema1;
        ConditionalSchema cond2 = (ConditionalSchema) schema2;
        cmp = compare(cond1.getIfSchema().orElse(EmptySchema.INSTANCE),
          cond2.getIfSchema().orElse(EmptySchema.INSTANCE));
        if (cmp != 0) {
          return cmp;
        }
        cmp = compare(cond1.getThenSchema().orElse(EmptySchema.INSTANCE),
          cond2.getThenSchema().orElse(EmptySchema.INSTANCE));
        if (cmp != 0) {
          return cmp;
        }
        return compare(cond1.getElseSchema().orElse(EmptySchema.INSTANCE),
          cond2.getElseSchema().orElse(EmptySchema.INSTANCE));
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
        return compare(arr1.getAllItemSchema(), arr2.getAllItemSchema());
      case REFERENCE:
        ReferenceSchema ref1 = (ReferenceSchema) schema1;
        ReferenceSchema ref2 = (ReferenceSchema) schema2;
        return compare(ref1.getReferredSchema(), ref2.getReferredSchema());
      default:
        return 0;
    }
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
}
