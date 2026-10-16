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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

  // Depth at which both schemas' unfoldings are truncated. Well beyond the depth any recursive
  // comparison can reach on a default thread stack, so those compare by full structure; it also
  // bounds the rounds of the iterative comparison.
  private static final int TRUNCATION_DEPTH = 1 << 14;

  // Nesting depth explored by direct recursion before switching to the iterative comparison.
  // Both produce the same result, so this only trades stack depth against speed.
  private static final int DEFAULT_MAX_DIRECT_DEPTH = 64;

  private final int maxDirectDepth;

  public JsonSchemaComparator() {
    this(DEFAULT_MAX_DIRECT_DEPTH);
  }

  JsonSchemaComparator(int maxDirectDepth) {
    this.maxDirectDepth = maxDirectDepth;
  }

  /**
   * Compares two schemas by structure, following references. The result is the comparison of
   * both schemas' unfoldings truncated at {@link #TRUNCATION_DEPTH} nesting levels: a fixed
   * finite tree per schema, so the ordering is a valid total preorder even when references form
   * cycles, and it equals the plain recursive comparison whenever that comparison terminates.
   * Shallow comparisons recurse directly; deeper ones, including any that follow a reference
   * cycle, are computed iteratively by {@link Refinement}.
   */
  @Override
  public int compare(Schema schema1, Schema schema2) {
    if (schema1 != null && schema2 != null && childCount(schema1) == 0) {
      // Schemas without subschemas compare by label alone.
      return schema1 == schema2 ? 0 : compareLabels(schema1, schema2);
    }
    try {
      return compare(schema1, schema2, new Memo(this), 0);
    } catch (DepthExceeded e) {
      return new Refinement(schema1, schema2).compare();
    }
  }

  private int compare(Schema schema1, Schema schema2, Memo memo, int depth) {
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
    if (depth >= maxDirectDepth) {
      throw DepthExceeded.INSTANCE;
    }

    int cmp = compareLabels(schema1, schema2);
    if (cmp != 0) {
      return cmp;
    }
    if (schema1 instanceof ReferenceSchema) {
      SchemaPair pair = new SchemaPair(schema1, schema2);
      Integer cached = memo.get(pair);
      if (cached != null) {
        return cached;
      }
      cmp = compare(child(schema1, 0), child(schema2, 0), memo, depth + 1);
      memo.put(pair, cmp);
      return cmp;
    }
    if (schema1 instanceof CombinedSchema) {
      List<Schema> comb1Schemas = new ArrayList<>(((CombinedSchema) schema1).getSubschemas());
      List<Schema> comb2Schemas = new ArrayList<>(((CombinedSchema) schema2).getSubschemas());
      memo.sortAtDepth(comb1Schemas, depth + 1);
      memo.sortAtDepth(comb2Schemas, depth + 1);
      for (int i = 0; i < comb1Schemas.size(); i++) {
        cmp = compare(comb1Schemas.get(i), comb2Schemas.get(i), memo, depth + 1);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    }
    int count = childCount(schema1);
    for (int i = 0; i < count; i++) {
      cmp = compare(child(schema1, i), child(schema2, i), memo, depth + 1);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  /**
   * Compares everything about two schemas except the subschemas they contain.
   */
  private static int compareLabels(Schema schema1, Schema schema2) {
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
        return comb1.getSubschemas().size() - comb2.getSubschemas().size();
      case OBJECT:
        ObjectSchema obj1 = (ObjectSchema) schema1;
        ObjectSchema obj2 = (ObjectSchema) schema2;
        cmp = compareCollections(
          obj1.getPropertySchemas().keySet(), obj2.getPropertySchemas().keySet());
        if (cmp != 0) {
          return cmp;
        }
        return compareCollections(obj1.getRequiredProperties(), obj2.getRequiredProperties());
      default:
        return 0;
    }
  }

  /**
   * The number of subschemas a schema is compared by. Two schemas whose labels compare equal have
   * the same count. The subschemas of a CombinedSchema are unordered and are compared after
   * sorting; all others are compared in index order.
   */
  private static int childCount(Schema schema) {
    if (schema instanceof CombinedSchema) {
      return ((CombinedSchema) schema).getSubschemas().size();
    } else if (schema instanceof ConditionalSchema) {
      return 3;
    } else if (schema instanceof NotSchema
        || schema instanceof ArraySchema
        || schema instanceof ReferenceSchema) {
      return 1;
    }
    return 0;
  }

  /**
   * The subschema at {@code index} that {@code schema} is compared by; may be null. Not used for
   * a CombinedSchema, whose subschemas are unordered.
   */
  private static Schema child(Schema schema, int index) {
    if (schema instanceof NotSchema) {
      return ((NotSchema) schema).getMustNotMatch();
    } else if (schema instanceof ConditionalSchema) {
      ConditionalSchema cond = (ConditionalSchema) schema;
      Optional<Schema> part = index == 0 ? cond.getIfSchema()
          : index == 1 ? cond.getThenSchema() : cond.getElseSchema();
      return part.orElse(EmptySchema.INSTANCE);
    } else if (schema instanceof ArraySchema) {
      return ((ArraySchema) schema).getAllItemSchema();
    } else if (schema instanceof ReferenceSchema) {
      return ((ReferenceSchema) schema).getReferredSchema();
    }
    throw new IllegalArgumentException("No subschema " + index + " in " + schema.getClass());
  }

  private static int compareCollections(Collection<?> coll1, Collection<?> coll2) {
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

  /**
   * State for one top-level comparison: results of reference pairs already compared, and the
   * comparator used to sort subschemas.
   */
  private static final class Memo implements Comparator<Schema> {
    private final JsonSchemaComparator owner;
    private Map<SchemaPair, Integer> results;
    private int sortDepth;

    Memo(JsonSchemaComparator owner) {
      this.owner = owner;
    }

    /**
     * Sorts subschemas found at {@code depth}. Sorts nest strictly, so the depth is saved and
     * restored around each one.
     */
    void sortAtDepth(List<Schema> schemas, int depth) {
      int outer = sortDepth;
      sortDepth = depth;
      try {
        schemas.sort(this);
      } finally {
        sortDepth = outer;
      }
    }

    @Override
    public int compare(Schema schema1, Schema schema2) {
      return owner.compare(schema1, schema2, this, sortDepth);
    }

    Integer get(SchemaPair pair) {
      return results != null ? results.get(pair) : null;
    }

    void put(SchemaPair pair, int cmp) {
      if (results == null) {
        results = new HashMap<>();
      }
      results.put(pair, cmp);
    }
  }

  private static final class DepthExceeded extends RuntimeException {
    private static final long serialVersionUID = 1L;
    static final DepthExceeded INSTANCE = new DepthExceeded();

    private DepthExceeded() {
      super(null, null, false, false);
    }
  }

  /**
   * Iterative form of the comparison over the schema graph reachable from two schemas. Round
   * {@code k} ranks every schema by its unfolding truncated at depth {@code k}: by label, then by
   * the previous round's ranks of its children, with a CombinedSchema's children as a sorted
   * multiset. The graph is finite, so the rounds eventually cycle, which gives the ranks at round
   * {@link #TRUNCATION_DEPTH} without computing every round.
   */
  private static final class Refinement {
    private static final int HISTORY_WINDOW = 64;

    private final List<Schema> nodes = new ArrayList<>();
    private final Map<Schema, Integer> ids = new IdentityHashMap<>();
    private final int[][] children;
    private final boolean[] unordered;
    private final int first;
    private final int second;

    Refinement(Schema schema1, Schema schema2) {
      first = add(schema1);
      second = add(schema2);
      List<int[]> childIds = new ArrayList<>();
      for (int i = 0; i < nodes.size(); i++) {
        Schema node = nodes.get(i);
        List<Schema> kids = new ArrayList<>();
        if (node instanceof CombinedSchema) {
          kids.addAll(((CombinedSchema) node).getSubschemas());
        } else {
          for (int j = 0; j < childCount(node); j++) {
            kids.add(child(node, j));
          }
        }
        int[] kidIds = new int[kids.size()];
        for (int j = 0; j < kids.size(); j++) {
          kidIds[j] = kids.get(j) != null ? add(kids.get(j)) : -1;
        }
        childIds.add(kidIds);
      }
      children = childIds.toArray(new int[0][]);
      unordered = new boolean[nodes.size()];
      for (int i = 0; i < nodes.size(); i++) {
        unordered[i] = nodes.get(i) instanceof CombinedSchema;
      }
    }

    private int add(Schema schema) {
      Integer id = ids.get(schema);
      if (id == null) {
        id = nodes.size();
        ids.put(schema, id);
        nodes.add(schema);
      }
      return id;
    }

    int compare() {
      int n = nodes.size();
      Integer[] order = new Integer[n];
      for (int i = 0; i < n; i++) {
        order[i] = i;
      }
      Arrays.sort(order, (x, y) -> compareLabels(nodes.get(x), nodes.get(y)));
      int[] labelRank = denseRanks(order, (x, y) -> compareLabels(nodes.get(x), nodes.get(y)));

      // Rounds of the first HISTORY_WINDOW are kept, so a short cycle of rounds is resolved
      // without iterating to TRUNCATION_DEPTH.
      Map<RankVector, Integer> seen = new HashMap<>();
      List<int[]> history = new ArrayList<>();
      int[] rank = labelRank;
      for (int round = 0; round < TRUNCATION_DEPTH; round++) {
        if (round < HISTORY_WINDOW) {
          Integer start = seen.putIfAbsent(new RankVector(rank), round);
          if (start != null) {
            int[] ranks = history.get(start + (TRUNCATION_DEPTH - start) % (round - start));
            return Integer.signum(ranks[first] - ranks[second]);
          }
          history.add(rank);
        }
        JsonSchemaCancellation.throwIfInterrupted();
        int[] next = refine(labelRank, rank, order);
        if (Arrays.equals(next, rank)) {
          break;
        }
        rank = next;
      }
      return Integer.signum(rank[first] - rank[second]);
    }

    private int[] refine(int[] labelRank, int[] rank, Integer[] order) {
      int n = nodes.size();
      int[][] keys = new int[n][];
      for (int i = 0; i < n; i++) {
        int[] key = new int[children[i].length];
        for (int j = 0; j < key.length; j++) {
          key[j] = children[i][j] >= 0 ? rank[children[i][j]] : -1;
        }
        if (unordered[i]) {
          Arrays.sort(key);
        }
        keys[i] = key;
      }
      Comparator<Integer> byKey = (x, y) -> {
        int cmp = Integer.compare(labelRank[x], labelRank[y]);
        if (cmp != 0) {
          return cmp;
        }
        for (int j = 0; j < keys[x].length; j++) {
          cmp = Integer.compare(keys[x][j], keys[y][j]);
          if (cmp != 0) {
            return cmp;
          }
        }
        return 0;
      };
      Arrays.sort(order, byKey);
      return denseRanks(order, byKey);
    }

    private static int[] denseRanks(Integer[] sorted, Comparator<Integer> cmp) {
      int[] rank = new int[sorted.length];
      int current = 0;
      for (int i = 0; i < sorted.length; i++) {
        if (i > 0 && cmp.compare(sorted[i - 1], sorted[i]) != 0) {
          current++;
        }
        rank[sorted[i]] = current;
      }
      return rank;
    }
  }

  private static final class RankVector {
    private final int[] ranks;
    private final int hash;

    RankVector(int[] ranks) {
      this.ranks = ranks;
      this.hash = Arrays.hashCode(ranks);
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof RankVector && Arrays.equals(ranks, ((RankVector) o).ranks);
    }

    @Override
    public int hashCode() {
      return hash;
    }
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
