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
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
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
 * field transforms: a union step is the first branch the value validates against. That resolves a
 * property two branches share, one continuing and one new; where it stays ambiguous, it is pruned.
 */
final class JsonProvenancePruner {

  private static final ObjectMapper MAPPER = new ObjectMapper();

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

    Target(List<String> names) {
      this.names = names;
    }

    boolean clashes() {
      return candidates.stream().anyMatch(c -> c.continues);
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
   */
  static JsonProvenancePruner plan(ProvenanceMapping mapping, JsonSchema reader) {
    Map<List<String>, Target> byNames = new LinkedHashMap<>();
    for (List<Integer> path : mapping.readerPaths()) {
      List<String> names = mapping.readerNamesOf(path);
      if (names != null && isProperty(mapping, path, names)) {
        byNames.computeIfAbsent(names, Target::new).candidates.add(
            new Candidate(branchChoices(mapping, path), mapping.writerPathOf(path) != null));
      }
    }
    List<Target> targets = new ArrayList<>();
    for (Target target : byNames.values()) {
      if (target.candidates.stream().anyMatch(c -> !c.continues)) {
        targets.add(target);
      }
    }
    // Outermost first: a property removed takes whatever lay under it along.
    targets.sort(Comparator.comparingInt(t -> t.names.size()));
    return new JsonProvenancePruner(reader.rawSchema(), Collections.unmodifiableList(targets));
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
    for (Target target : targets) {
      walk(target, reader, document, 0, new ArrayList<>(), false);
    }
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

  private static boolean isBranch(ProvenanceMapping mapping, List<Integer> path,
      List<String> names) {
    List<String> parent = path.size() > 1
        ? mapping.readerNamesOf(path.subList(0, path.size() - 1)) : Collections.emptyList();
    return names.equals(parent);
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

  private void walk(Target target, Schema schema, JsonNode node, int step, List<Integer> choices,
      boolean ambiguous) {
    if (schema == null || node == null) {
      return;
    }
    if (schema instanceof ReferenceSchema) {
      walk(target, ((ReferenceSchema) schema).getReferredSchema(), node, step, choices, ambiguous);
      return;
    }
    if (schema instanceof CombinedSchema) {
      walkCombined(target, (CombinedSchema) schema, node, step, choices, ambiguous);
      return;
    }
    String name = target.names.get(step);
    if (name == null) {
      // An unnamed step: each element of an array, or each value of an object keyed by string.
      Schema child = schema instanceof ArraySchema
          ? ((ArraySchema) schema).getAllItemSchema()
          : schema instanceof ObjectSchema
              ? ((ObjectSchema) schema).getSchemaOfAdditionalProperties() : null;
      for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
        walk(target, child, it.next(), step + 1, choices, ambiguous);
      }
      return;
    }
    if (!(schema instanceof ObjectSchema) || !node.isObject() || !node.has(name)
        || !((ObjectSchema) schema).getPropertySchemas().containsKey(name)) {
      return;
    }
    ObjectSchema object = (ObjectSchema) schema;
    if (step + 1 < target.names.size()) {
      walk(target, object.getPropertySchemas().get(name), node.get(name), step + 1, choices,
          ambiguous);
    } else if (!keeps(target, choices, ambiguous)) {
      remove((ObjectNode) node, object, name);
    }
  }

  private void walkCombined(Target target, CombinedSchema schema, JsonNode node, int step,
      List<Integer> choices, boolean ambiguous) {
    List<Schema> subschemas = new ArrayList<>(schema.getSubschemas());
    if (schema.getCriterion() == CombinedSchema.ALL_CRITERION) {
      // The converter merges an allOf; the next step lives in whichever part declares it.
      for (Schema part : subschemas) {
        walk(target, part, node, step, choices, ambiguous);
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
      walk(target, branches.get(0), node, step, choices, ambiguous);
      return;
    }
    int chosen = -1;
    boolean several = false;
    for (int i = 0; i < branches.size(); i++) {
      if (validates(branches.get(i), node)) {
        if (chosen >= 0) {
          several = true;
          break;
        }
        chosen = i;
      }
    }
    if (chosen < 0) {
      return;
    }
    List<Integer> extended = new ArrayList<>(choices);
    extended.add(chosen);
    // oneOf takes the first valid branch as JsonSchema does; overlapping anyOf is ambiguous.
    walk(target, branches.get(chosen), node, step, extended,
        ambiguous || (several && schema.getCriterion() == CombinedSchema.ANY_CRITERION));
  }

  /**
   * Whether the value may stay: exactly one location matches the branches taken, and it
   * continues.
   */
  private static boolean keeps(Target target, List<Integer> choices, boolean ambiguous) {
    if (!target.clashes() || ambiguous) {
      return false;
    }
    Candidate match = null;
    for (Candidate candidate : target.candidates) {
      if (candidate.choices.equals(choices)) {
        if (match != null) {
          return false;
        }
        match = candidate;
      }
    }
    return match != null && match.continues;
  }

  private static void remove(ObjectNode node, ObjectSchema object, String name) {
    if (!object.getRequiredProperties().contains(name)) {
      node.remove(name);
      return;
    }
    Schema property = object.getPropertySchemas().get(name);
    if (property == null || !property.hasDefaultValue()) {
      throw new SerializationException("Property '" + name + "' has no counterpart in the "
          + "writer schema, is required by the reader and declares no default. There is no value "
          + "to read.");
    }
    try {
      node.set(name, MAPPER.readTree(JSONObject.valueToString(property.getDefaultValue())));
    } catch (IOException e) {
      throw new SerializationException("Could not read the default of property '" + name + "'", e);
    }
  }

  private static boolean validates(Schema schema, JsonNode node) {
    try {
      JsonSchema.validate(schema, node);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
