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

package io.confluent.kafka.serializers.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.schemaregistry.json.schema.CombinedSchemaExt;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.errors.SerializationException;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Carries provenance into a JSON document by pruning it.
 *
 * <p>JSON Schema identifies a property by its name, so provenance cannot turn a rename into a
 * match. What it can tell is a reader property whose provenance id the writer does not have — a
 * property dropped and re-added, or one under a union reshaped across the pair — and a reader
 * converting by name would hand it data written for another. Every such property is removed, so
 * the reader finds nothing there; one the reader requires takes its default, or fails the record.
 *
 * <p>The document is walked alongside the reader's schema, as {@code JsonSchema} walks one for
 * field transforms: a union step is the branch declaring the next property that the value
 * validates against. That resolves a property two branches share, one continuing and one new;
 * where it stays ambiguous — several such branches fit, or none does, as when the value to prune
 * is itself what fails them — it is pruned.
 */
final class JsonProvenancePruner {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int MAX_INTEGRAL_DIGITS = 1000;
  // Readings of one value weighed before falling back to requiring every reach's requirement.
  private static final int MAX_READINGS = 1024;
  // As JsonSchema converts a document for everit.
  private static final ObjectMapper ORG_JSON = Jackson.newObjectMapper();

  private final Schema reader;
  private final List<Target> targets;

  private JsonProvenancePruner(Schema reader, List<Target> targets) {
    this.reader = reader;
    this.targets = targets;
  }

  /** A property to prune, spelled by its names, and every reader location spelled the same. */
  private static final class Target {
    final List<String> names;
    final List<Candidate> candidates = new ArrayList<>();
    // Indexed once planned: a value reaches the property once per path, so look-ups add up.
    private Map<List<Integer>, List<Candidate>> byChoices;
    private boolean clashes;

    Target(List<String> names) {
      this.names = names;
    }

    Target indexed() {
      byChoices = new HashMap<>();
      for (Candidate candidate : candidates) {
        byChoices.computeIfAbsent(candidate.choices, c -> new ArrayList<>()).add(candidate);
      }
      clashes = candidates.stream().anyMatch(c -> c.continues);
      return this;
    }

    boolean clashes() {
      return clashes;
    }
  }

  /**
   * One reader location at a target's names: its union branch choices, and whether it
   * continues.
   */
  private static final class Candidate {
    final List<Integer> choices;
    final boolean continues;

    Candidate(List<Integer> choices, boolean continues) {
      this.choices = choices;
      this.continues = continues;
    }
  }

  /**
   * The plan for reading a writer's documents under {@code reader} as {@code mapping} pairs them.
   *
   * @throws SerializationException if a location's names are missing, or a property to prune is
   *     not declared by the reader
   */
  static JsonProvenancePruner plan(ProvenanceMapping mapping, JsonSchema reader) {
    // A property the walk cannot find would keep a value provenance says is new.
    mapping.requireNames();
    Schema raw = reader.rawSchema();
    Map<List<String>, Target> byNames = new LinkedHashMap<>();
    for (List<Integer> path : mapping.readerPaths()) {
      List<String> names = mapping.readerNamesOf(path);
      if (isProperty(mapping, path, names)) {
        byNames.computeIfAbsent(names, Target::new).candidates.add(
            new Candidate(branchChoices(mapping, path), mapping.writerPathOf(path) != null));
      }
    }
    List<Target> targets = new ArrayList<>();
    for (Target target : byNames.values()) {
      if (target.candidates.stream().anyMatch(c -> !c.continues)) {
        if (!declares(target.names, raw, 0, new IdentityHashMap<>())) {
          throw new SerializationException("Property " + target.names + " of schema id "
              + mapping.readerId() + " is not declared by the reader schema");
        }
        targets.add(target.indexed());
      }
    }
    // Outermost first: a property removed takes whatever lay under it along.
    targets.sort(Comparator.comparingInt(t -> t.names.size()));
    return new JsonProvenancePruner(raw, Collections.unmodifiableList(targets));
  }

  boolean isEmpty() {
    return targets.isEmpty();
  }

  /**
   * Prunes {@code document} in place.
   *
   * @throws SerializationException if a property to prune is required and has no default
   */
  void prune(JsonNode document) {
    // A default put in place of a pruned value is the reader's own: nothing under it is pruned.
    Set<JsonNode> defaults = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Target target : targets) {
      // allOf parts and ambiguous branches reach one value more than once: decided together.
      Map<ObjectNode, List<Reach>> reached = new IdentityHashMap<>();
      walk(target.names, reader, document, 0, new ArrayList<>(), false, Collections.emptyList(),
          (node, object, name, choices, ambiguous, alternatives) -> {
            if (!defaults.contains(node) && !keeps(target, choices, ambiguous)) {
              reached.computeIfAbsent(node, n -> new ArrayList<>())
                  .add(new Reach(object, alternatives));
            }
          });
      String name = target.names.get(target.names.size() - 1);
      reached.forEach((node, reaches) -> {
        JsonNode value = remove(node, reaches, name, target.names);
        if (value != null) {
          addContainers(value, defaults);
        }
      });
    }
  }

  /** One object schema reaching a property, and the ambiguous union branches taken to reach it. */
  private static final class Reach {
    final ObjectSchema object;
    final List<Alternative> alternatives;

    Reach(ObjectSchema object, List<Alternative> alternatives) {
      this.object = object;
      this.alternatives = alternatives;
    }
  }

  /** A branch of an ambiguous union step: one reading of the value among several. */
  private static final class Alternative {
    final Schema union;
    final int branch;

    Alternative(Schema union, int branch) {
      this.union = union;
      this.branch = branch;
    }
  }

  private static void addContainers(JsonNode node, Set<JsonNode> containers) {
    if (node.isContainerNode()) {
      containers.add(node);
      node.elements().forEachRemaining(child -> addContainers(child, containers));
    }
  }

  /** What to do at a property the walk reaches, with the union branches taken to reach it. */
  interface AtProperty {
    void accept(ObjectNode node, ObjectSchema object, String name, List<Integer> choices,
        boolean ambiguous, List<Alternative> alternatives);
  }

  /**
   * The union branches {@code document} takes on the way to the property spelled {@code names},
   * as the pruner resolves them; null if it does not reach one.
   */
  static List<Integer> branchesTaken(JsonSchema reader, JsonNode document, List<String> names) {
    List<List<Integer>> taken = new ArrayList<>();
    walk(names, reader.rawSchema(), document, 0, new ArrayList<>(), false,
        Collections.emptyList(),
        (node, object, name, choices, ambiguous, alternatives) -> taken.add(choices));
    return taken.isEmpty() ? null : taken.get(0);
  }

  /** As the response spells them: the branch index of every union branch on the way to a path. */
  static List<Integer> branchChoicesAt(ProvenanceMapping mapping, List<Integer> path) {
    return branchChoices(mapping, path);
  }

  /**
   * Whether the location at {@code path} is a property of its own. A union branch has no step in
   * the document, so it spells the same names as the location holding it and is never removed.
   */
  private static boolean isProperty(ProvenanceMapping mapping, List<Integer> path,
      List<String> names) {
    return !names.isEmpty() && names.get(names.size() - 1) != null
        && !isBranch(mapping, path, names);
  }

  /**
   * Whether the location at {@code path} is a union branch: a JSON location is a property or a
   * branch.
   */
  private static boolean isBranch(ProvenanceMapping mapping, List<Integer> path,
      List<String> names) {
    List<String> enclosing = mapping.enclosingReaderNamesOf(path);
    if (names.size() < enclosing.size()
        || !names.subList(0, enclosing.size()).equals(enclosing)) {
      return false;
    }
    // A branch spells its enclosing location's names, then at most unnamed steps into an array or
    // map; a property adds a name of its own.
    for (String step : names.subList(enclosing.size(), names.size())) {
      if (step != null) {
        return false;
      }
    }
    return true;
  }

  /**
   * The branch index of every union branch on the way to {@code path}, outermost first.
   */
  private static List<Integer> branchChoices(ProvenanceMapping mapping, List<Integer> path) {
    List<Integer> choices = new ArrayList<>();
    for (int k = 1; k <= path.size(); k++) {
      List<Integer> prefix = path.subList(0, k);
      List<String> names = mapping.readerNamesOf(prefix);
      if (names != null && isBranch(mapping, prefix, names)) {
        choices.add(prefix.get(k - 1));
      }
    }
    return choices;
  }

  /**
   * Whether some branch of {@code schema} declares the property spelled by {@code names} from
   * {@code step} on, looking where {@link #walk} would.
   */
  private static boolean declares(List<String> names, Schema schema, int step,
      Map<Schema, Set<Integer>> seen) {
    // By identity: an everit schema's hashCode walks its whole tree.
    if (schema == null || !seen.computeIfAbsent(schema, s -> new HashSet<>()).add(step)) {
      return false;
    }
    if (schema instanceof ReferenceSchema) {
      return declares(names, ((ReferenceSchema) schema).getReferredSchema(), step, seen);
    }
    if (schema instanceof CombinedSchema) {
      for (Schema subschema : ((CombinedSchema) schema).getSubschemas()) {
        if (declares(names, subschema, step, seen)) {
          return true;
        }
      }
      return false;
    }
    String name = names.get(step);
    Schema child;
    if (name == null) {
      child = schema instanceof ArraySchema
          ? ((ArraySchema) schema).getAllItemSchema()
          : schema instanceof ObjectSchema
              ? ((ObjectSchema) schema).getSchemaOfAdditionalProperties() : null;
    } else {
      child = schema instanceof ObjectSchema
          ? ((ObjectSchema) schema).getPropertySchemas().get(name) : null;
    }
    if (child == null) {
      return false;
    }
    return step + 1 == names.size() || declares(names, child, step + 1, seen);
  }

  private static void walk(List<String> names, Schema schema, JsonNode node, int step,
      List<Integer> choices, boolean ambiguous, List<Alternative> alternatives, AtProperty at) {
    if (schema == null || node == null) {
      return;
    }
    if (schema instanceof ReferenceSchema) {
      walk(names, ((ReferenceSchema) schema).getReferredSchema(), node, step, choices, ambiguous,
          alternatives, at);
      return;
    }
    if (schema instanceof CombinedSchema) {
      walkCombined(names, (CombinedSchema) schema, node, step, choices, ambiguous, alternatives,
          at);
      return;
    }
    String name = names.get(step);
    if (name == null) {
      // An unnamed step: each element of an array, or each value of an object keyed by string.
      Schema child = schema instanceof ArraySchema
          ? ((ArraySchema) schema).getAllItemSchema()
          : schema instanceof ObjectSchema
              ? ((ObjectSchema) schema).getSchemaOfAdditionalProperties() : null;
      for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
        walk(names, child, it.next(), step + 1, choices, ambiguous, alternatives, at);
      }
      return;
    }
    if (!(schema instanceof ObjectSchema) || !node.isObject() || !node.has(name)) {
      return;
    }
    ObjectSchema object = (ObjectSchema) schema;
    boolean declared = object.getPropertySchemas().containsKey(name);
    if (step + 1 < names.size()) {
      if (declared) {
        walk(names, object.getPropertySchemas().get(name), node.get(name), step + 1, choices,
            ambiguous, alternatives, at);
      }
    } else if (declared || object.getRequiredProperties().contains(name)) {
      // A part that only requires the property, as allOf [Base, {required: [p]}], is reached too.
      at.accept((ObjectNode) node, object, name, choices, ambiguous, alternatives);
    }
  }

  private static void walkCombined(List<String> names, CombinedSchema schema, JsonNode node,
      int step, List<Integer> choices, boolean ambiguous, List<Alternative> alternatives,
      AtProperty at) {
    String name = names.get(step);
    if (name != null && !node.has(name)) {
      // Every branch reaches the property as a member of this very value: nothing to prune.
      return;
    }
    List<Schema> subschemas = new ArrayList<>(schema.getSubschemas());
    if (schema.getCriterion() == CombinedSchema.ALL_CRITERION) {
      // The converter merges an allOf; the next step lives in whichever part declares it.
      for (Schema part : subschemas) {
        walk(names, part, node, step, choices, ambiguous, alternatives, at);
      }
      return;
    }
    List<Schema> branches = new ArrayList<>();
    for (Schema subschema : subschemas) {
      if (!(subschema instanceof NullSchema)) {
        branches.add(subschema);
      }
    }
    if (branches.size() == 1 && branches.size() < subschemas.size()) {
      // A nullable union, which the logical type collapses: no branch step.
      walk(names, branches.get(0), node, step, choices, ambiguous, alternatives, at);
      return;
    }
    // Only a branch declaring the next step can hold the property; one that does not bears on it
    // only if no declaring branch fits, and then the property is merely an extra there.
    List<Integer> valid = new ArrayList<>();
    List<Integer> declaring = new ArrayList<>();
    // A union translated from 2019-09 or 2020-12, which Schema Registry validates with json-sKema.
    Object validatable = validatable(
        schema instanceof CombinedSchemaExt ? integralDecimals(node) : node);
    for (int i = 0; i < branches.size(); i++) {
      if (declares(names, branches.get(i), step, new IdentityHashMap<>())) {
        declaring.add(i);
        if (validates(branches.get(i), validatable)) {
          valid.add(i);
        }
      }
    }
    boolean fallback = false;
    if (valid.isEmpty()) {
      fallback = true;
      for (int i = 0; i < branches.size() && fallback; i++) {
        if (!declaring.contains(i) && validates(branches.get(i), validatable)) {
          fallback = false;
        }
      }
    }
    // No branch fits, often because of the very value provenance withholds (its type changed):
    // every declaring branch is walked, as ambiguous, so the property is pruned.
    List<Integer> walked = fallback ? declaring : valid;
    // Only where several branches are walked are they alternatives; one taken alone is the value's.
    boolean alternative = walked.size() > 1;
    for (int i : walked) {
      List<Integer> extended = new ArrayList<>(choices);
      extended.add(i);
      List<Alternative> readings = alternatives;
      if (alternative) {
        readings = new ArrayList<>(alternatives);
        readings.add(new Alternative(schema, i));
      }
      walk(names, branches.get(i), node, step, extended,
          ambiguous || fallback || valid.size() > 1, readings, at);
    }
  }

  /**
   * Whether the value may stay: exactly one location matches the branches taken, and it
   * continues.
   */
  private static boolean keeps(Target target, List<Integer> choices, boolean ambiguous) {
    if (!target.clashes() || ambiguous) {
      return false;
    }
    List<Candidate> matches = target.byChoices.get(choices);
    return matches != null && matches.size() == 1 && matches.get(0).continues;
  }

  /**
   * Removes {@code name} from {@code node}, reached by {@code reaches}, and returns the default
   * put in its place, if any. Every reading of the value — one branch of each ambiguous union
   * taken on the way — applies every reach consistent with it, and allOf parts all apply: the
   * property is required if some applying reach requires it in every reading.
   */
  private static JsonNode remove(ObjectNode node, List<Reach> reaches, String name,
      List<String> names) {
    boolean declared = false;
    Schema withDefault = null;
    for (Reach reach : reaches) {
      Schema property = reach.object.getPropertySchemas().get(name);
      declared |= property != null;
      if (withDefault == null && property != null && property.hasDefaultValue()) {
        withDefault = property;
      }
    }
    if (!declared) {
      // Only parts requiring it reached the property: an extra property, not a location.
      return null;
    }
    node.remove(name);
    if (!requiredInEveryReading(reaches, name)) {
      return null;
    }
    if (withDefault == null) {
      throw new SerializationException("Property " + names + " is new to the reader: "
          + "provenance pairs it with nothing the writer wrote, and the reader requires it and "
          + "declares no default. There is no value to read.");
    }
    try {
      JsonNode value = MAPPER.readTree(JSONObject.valueToString(withDefault.getDefaultValue()));
      node.set(name, value);
      return value;
    } catch (IOException e) {
      throw new SerializationException("Could not read the default of property '" + name + "'", e);
    }
  }

  /**
   * Whether every reading of the value requires {@code name}: each combination of the branches
   * seen for each ambiguous union, over the reaches consistent with it.
   */
  private static boolean requiredInEveryReading(List<Reach> reaches, String name) {
    boolean anyRequires = false;
    boolean allRequire = true;
    for (Reach reach : reaches) {
      boolean requires = reach.object.getRequiredProperties().contains(name);
      if (requires && reach.alternatives.isEmpty()) {
        // An allOf part or an unambiguous step applies in every reading.
        return true;
      }
      anyRequires |= requires;
      allRequire &= requires;
    }
    if (!anyRequires || allRequire) {
      return anyRequires;
    }
    Map<Schema, List<Integer>> unions = new IdentityHashMap<>();
    for (Reach reach : reaches) {
      for (Alternative alternative : reach.alternatives) {
        List<Integer> seen = unions.computeIfAbsent(alternative.union, u -> new ArrayList<>());
        if (!seen.contains(alternative.branch)) {
          seen.add(alternative.branch);
        }
      }
    }
    List<Map<Schema, Integer>> readings = new ArrayList<>();
    readings.add(new IdentityHashMap<>());
    for (Map.Entry<Schema, List<Integer>> union : unions.entrySet()) {
      List<Map<Schema, Integer>> next = new ArrayList<>();
      for (Map<Schema, Integer> reading : readings) {
        for (int branch : union.getValue()) {
          Map<Schema, Integer> extended = new IdentityHashMap<>(reading);
          extended.put(union.getKey(), branch);
          next.add(extended);
        }
      }
      if (next.size() > MAX_READINGS) {
        // Too many to weigh: required only where every reach requires it.
        return reaches.stream().allMatch(r -> r.object.getRequiredProperties().contains(name));
      }
      readings = next;
    }
    for (Map<Schema, Integer> reading : readings) {
      boolean applies = false;
      boolean requires = false;
      for (Reach reach : reaches) {
        if (reach.alternatives.stream()
            .allMatch(a -> Integer.valueOf(a.branch).equals(reading.get(a.union)))) {
          applies = true;
          requires |= reach.object.getRequiredProperties().contains(name);
        }
      }
      if (applies && !requires) {
        return false;
      }
    }
    return true;
  }

  /**
   * {@code node} with every integral decimal ({@code 1.0}, {@code 1e2}) as an integer. json-sKema
   * counts one an integer and everit does not, and a branch everit alone rejects would leave the
   * value to a branch validation does not choose.
   */
  private static JsonNode integralDecimals(JsonNode node) {
    if (node.isFloatingPointNumber()) {
      BigDecimal value = node.isBigDecimal() || Double.isFinite(node.doubleValue())
          ? node.decimalValue() : null;
      // Bounded, so a huge exponent does not become a huge integer.
      return value != null && value.stripTrailingZeros().scale() <= 0
          && value.precision() - value.scale() <= MAX_INTEGRAL_DIGITS
          ? BigIntegerNode.valueOf(value.toBigInteger()) : node;
    }
    JsonNode copy = null;
    if (node.isObject()) {
      for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> field = it.next();
        JsonNode value = integralDecimals(field.getValue());
        if (value != field.getValue()) {
          copy = copy != null ? copy : node.deepCopy();
          ((ObjectNode) copy).set(field.getKey(), value);
        }
      }
    } else if (node.isArray()) {
      for (int i = 0; i < node.size(); i++) {
        JsonNode value = integralDecimals(node.get(i));
        if (value != node.get(i)) {
          copy = copy != null ? copy : node.deepCopy();
          ((ArrayNode) copy).set(i, value);
        }
      }
    }
    return copy != null ? copy : node;
  }

  /**
   * {@code node} as everit validates it, as {@code JsonSchema.validate} converts it. Converted
   * once per union step, and never converted back: the pruner wants only whether it validates.
   */
  private static Object validatable(JsonNode node) {
    try {
      if (node.isObject()) {
        return ORG_JSON.treeToValue(node, JSONObject.class);
      }
      if (node.isArray()) {
        return ORG_JSON.treeToValue(node, JSONArray.class);
      }
      if (node.isNull()) {
        return null;
      }
      if (node.isBoolean()) {
        return node.asBoolean();
      }
      if (node.isNumber()) {
        return node.numberValue();
      }
      return node.asText();
    } catch (IOException e) {
      throw new SerializationException("Could not read a value to validate", e);
    }
  }

  private static boolean validates(Schema schema, Object validatable) {
    try {
      schema.validate(validatable);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
