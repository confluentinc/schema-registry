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
import java.util.function.Function;
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
  // Partial readings of one value searched for one that does not require a property; past it, the
  // property is taken as not required.
  private static final int MAX_READINGS = 1024;
  // A value fitting none of a union's branches, in place of a branch index.
  private static final int NO_BRANCH = -1;
  // As JsonSchema converts a document for everit.
  private static final ObjectMapper ORG_JSON = Jackson.newObjectMapper();

  private final Schema reader;
  private final Schema writer;
  private final List<Target> targets;

  private JsonProvenancePruner(Schema reader, Schema writer, List<Target> targets) {
    this.reader = reader;
    this.writer = writer;
    this.targets = targets;
  }

  /** A property to prune, spelled by its names, and every reader location spelled the same. */
  private static final class Target {
    final List<String> names;
    final List<Candidate> candidates = new ArrayList<>();
    // The branches of the property's own union, by branch choices: a primitive lives there.
    final Map<List<Integer>, Candidate> branches = new HashMap<>();
    // Each side's branches under the property, by its own branch choices, to their kinds.
    final Map<List<Integer>, String> readerKinds = new HashMap<>();
    final Map<List<Integer>, String> writerKinds = new HashMap<>();
    // Indexed once planned: a value reaches the property once per path, so look-ups add up.
    private Map<List<Integer>, List<Candidate>> byChoices;
    private boolean clashes;
    // Whether some location spelled so is new; if not, only a value read as another is pruned.
    private boolean anyNew;

    Target(List<String> names) {
      this.names = names;
    }

    Target indexed() {
      byChoices = new HashMap<>();
      for (Candidate candidate : candidates) {
        byChoices.computeIfAbsent(candidate.choices, c -> new ArrayList<>()).add(candidate);
      }
      clashes = candidates.stream().anyMatch(c -> c.continues);
      anyNew = candidates.stream().anyMatch(c -> !c.continues)
          || branches.values().stream().anyMatch(c -> !c.continues);
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
    // The writer's branch choices to the location it continues; null if it continues none.
    final List<Integer> writerChoices;

    Candidate(List<Integer> choices, List<Integer> writerChoices) {
      this.choices = choices;
      this.continues = writerChoices != null;
      this.writerChoices = writerChoices;
    }
  }

  /**
   * The plan for reading a writer's documents under {@code reader} as {@code mapping} pairs them.
   *
   * @throws SerializationException if a location's names are missing, or a property to prune is
   *     not declared by the reader
   */
  static JsonProvenancePruner plan(ProvenanceMapping mapping, JsonSchema reader) {
    return plan(mapping, reader, null);
  }

  /**
   * As {@link #plan(ProvenanceMapping, JsonSchema)}; with {@code writer}, a value read at a
   * location continuing another than it can be read as under the writer is pruned too.
   */
  static JsonProvenancePruner plan(ProvenanceMapping mapping, JsonSchema reader,
      JsonSchema writer) {
    // A property the walk cannot find would keep a value provenance says is new.
    mapping.requireNames();
    mapping.requireKinds();
    Schema raw = reader.rawSchema();
    Map<List<String>, Target> byNames = new LinkedHashMap<>();
    for (List<Integer> path : mapping.readerPaths()) {
      List<String> names = mapping.readerNamesOf(path);
      if (!mapping.isReaderBranch(path)) {
        byNames.computeIfAbsent(names, Target::new).candidates.add(
            new Candidate(branchChoices(mapping, path), writerChoices(mapping, path)));
      }
    }
    for (List<Integer> path : mapping.readerPaths()) {
      // A branch of a property's own union, or of its items' or map values': the nearest
      // property enclosing it.
      if (!mapping.isReaderBranch(path)) {
        continue;
      }
      Target target = byNames.get(readerPropertyOf(mapping, path));
      if (target != null) {
        target.branches.put(branchChoices(mapping, path),
            new Candidate(branchChoices(mapping, path), writerChoices(mapping, path)));
        target.readerKinds.put(branchChoices(mapping, path), mapping.readerKindOf(path));
      }
    }
    // Writer locations spelled alike: a value may have been written as any of them, and read as
    // another wherever the reader's schema or a pruned property changes which branch it fits.
    Map<List<String>, Integer> spelled = new HashMap<>();
    Map<List<String>, List<String>> outermost = new HashMap<>();
    for (List<Integer> path : mapping.writerPaths()) {
      List<String> names = mapping.writerNamesOf(path);
      if (mapping.isWriterBranch(path)) {
        Target target = byNames.get(writerPropertyOf(mapping, path));
        if (target != null) {
          target.writerKinds.put(writerBranchChoices(mapping, path), mapping.writerKindOf(path));
        }
      }
      if (names != null) {
        spelled.merge(names, 1, Integer::sum);
        List<String> union = outermostUnion(mapping, path);
        if (union != null) {
          outermost.merge(names, union, (a, b) -> a.size() <= b.size() ? a : b);
        }
      }
    }
    // What may be pruned: the new, then each spelled several times that must be checked — pruned,
    // it too may change the branch a value under its union reads as. Until none is added.
    Set<List<String>> pruned = new HashSet<>();
    for (Target target : byNames.values()) {
      if (target.indexed().anyNew) {
        pruned.add(target.names);
      }
    }
    Map<List<String>, Boolean> alike = new HashMap<>();
    boolean added = writer != null;
    while (added) {
      added = false;
      for (Target target : byNames.values()) {
        if (!pruned.contains(target.names)
            && (spelled.getOrDefault(target.names, 0) > 1 || crossesBranch(target))
            && !readsAsWritten(target, outermost.getOrDefault(target.names,
                Collections.emptyList()), pruned, alike, reader, writer)) {
          pruned.add(target.names);
          added = true;
        }
      }
    }
    List<Target> targets = new ArrayList<>();
    for (Target target : byNames.values()) {
      if (pruned.contains(target.names)) {
        if (!declares(target.names, raw, 0, new IdentityHashMap<>())) {
          throw new SerializationException("Property " + target.names + " of schema id "
              + mapping.readerId() + " is not declared by the reader schema");
        }
        targets.add(target);
      }
    }
    // Outermost first: a property removed takes whatever lay under it along.
    targets.sort(Comparator.comparingInt(t -> t.names.size()));
    return new JsonProvenancePruner(raw, writer != null ? writer.rawSchema() : null,
        Collections.unmodifiableList(targets));
  }

  /**
   * The writer's branch choices to the location {@code readerPath} continues; null if none.
   */
  private static List<Integer> writerChoices(ProvenanceMapping mapping, List<Integer> readerPath) {
    List<Integer> written = mapping.writerPathOf(readerPath);
    return written == null ? null : writerBranchChoices(mapping, written);
  }

  /**
   * The branch index of every union branch on the way to the writer's {@code path}, outermost
   * first.
   */
  private static List<Integer> writerBranchChoices(ProvenanceMapping mapping, List<Integer> path) {
    List<Integer> choices = new ArrayList<>();
    for (int k = 1; k <= path.size(); k++) {
      List<Integer> prefix = path.subList(0, k);
      if (mapping.isWriterBranch(prefix)) {
        choices.add(prefix.get(k - 1));
      }
    }
    return choices;
  }

  // The names of the nearest reader property enclosing a branch at path; empty at the root.
  private static List<String> readerPropertyOf(ProvenanceMapping mapping, List<Integer> path) {
    for (int k = path.size() - 1; k > 0; k--) {
      List<Integer> prefix = path.subList(0, k);
      if (mapping.readerKindOf(prefix) != null && !mapping.isReaderBranch(prefix)) {
        return mapping.readerNamesOf(prefix);
      }
    }
    return Collections.emptyList();
  }

  // The names of the nearest writer property enclosing a branch at path; empty at the root.
  private static List<String> writerPropertyOf(ProvenanceMapping mapping, List<Integer> path) {
    for (int k = path.size() - 1; k > 0; k--) {
      List<Integer> prefix = path.subList(0, k);
      if (mapping.writerKindOf(prefix) != null && !mapping.isWriterBranch(prefix)) {
        return mapping.writerNamesOf(prefix);
      }
    }
    return Collections.emptyList();
  }

  /**
   * Whether a branch of {@code target}'s items' or map values' union continues the writer's at
   * another position: spelled once by name, the property may still read a value as another's.
   */
  private static boolean crossesBranch(Target target) {
    return target.branches.values().stream()
        .anyMatch(b -> b.continues && !b.choices.equals(b.writerChoices));
  }

  /**
   * Whether a value of {@code target}'s names reads as the location it was written as without
   * checking: each, and each branch of its own union, continues the location at its own branches,
   * nothing under the outermost union it sits in may be pruned, and that union validates every
   * value alike on both sides.
   */
  private static boolean readsAsWritten(Target target, List<String> union,
      Set<List<String>> fresh, Map<List<String>, Boolean> alike, JsonSchema reader,
      JsonSchema writer) {
    for (Candidate candidate : target.candidates) {
      if (!candidate.choices.equals(candidate.writerChoices)) {
        return false;
      }
    }
    // A branch may continue one at another position where the shapes agree, as a title pairs
    // them, which the shape leaves out.
    for (Candidate branch : target.branches.values()) {
      if (!branch.choices.equals(branch.writerChoices)) {
        return false;
      }
    }
    if (fresh.stream().anyMatch(names -> startsWith(names, union))) {
      return false;
    }
    return alike.computeIfAbsent(union, u -> {
      JsonNode written = JsonValidationShape.of(writer.toJsonNode(), u);
      JsonNode read = JsonValidationShape.of(reader.toJsonNode(), u);
      return written != null && read != null && JsonValidationShape.alike(written, read);
    });
  }

  /**
   * The names of the outermost union a writer location sits under: the property holding it, or
   * none for a union at the root, where the whole schema stands in; null if it sits under none.
   */
  private static List<String> outermostUnion(ProvenanceMapping mapping, List<Integer> path) {
    for (int k = 1; k <= path.size(); k++) {
      List<Integer> prefix = path.subList(0, k);
      if (mapping.isWriterBranch(prefix)) {
        return writerPropertyOf(mapping, prefix);
      }
    }
    return null;
  }

  private static boolean startsWith(List<String> names, List<String> prefix) {
    return names.size() >= prefix.size() && names.subList(0, prefix.size()).equals(prefix);
  }

  /**
   * The union branches a property's value is held in, each as the branch indexes of the unions on
   * the way: one in its own union's, its items' or its map values', unless that branch is a
   * struct, whose properties the walk tells. {@code kinds} are that side's branches, by choices
   * from {@code reached}, the branches taken to the property.
   */
  private static List<List<Integer>> heldIn(ObjectSchema object, String name, JsonNode value,
      List<Integer> reached, Map<List<Integer>, String> kinds) {
    List<List<Integer>> held = new ArrayList<>();
    heldIn(object.getPropertySchemas().get(name), value, Collections.emptyList(), held,
        choices -> {
          List<Integer> full = new ArrayList<>(reached);
          full.addAll(choices);
          return kinds.get(full);
        });
    return held;
  }

  private static void heldIn(Schema schema, JsonNode value, List<Integer> branches,
      List<List<Integer>> held, Function<List<Integer>, String> kindAt) {
    // A definition that only refers to itself, however indirectly, holds nothing.
    schema = referred(schema);
    if (value == null) {
      return;
    }
    if (schema instanceof CombinedSchema
        && ((CombinedSchema) schema).getCriterion() == CombinedSchema.ALL_CRITERION) {
      // Every part applies, as the walk takes it: a union in one is the property's own.
      for (Schema part : ((CombinedSchema) schema).getSubschemas()) {
        heldIn(part, value, branches, held, kindAt);
      }
    } else if (schema instanceof CombinedSchema) {
      List<Schema> options = new ArrayList<>();
      boolean nullable = false;
      for (Schema subschema : ((CombinedSchema) schema).getSubschemas()) {
        // A null member behind a $ref is a null member all the same.
        if (referred(subschema) instanceof NullSchema) {
          nullable = true;
        } else {
          options.add(subschema);
        }
      }
      if (options.size() == 1 && nullable) {
        // A nullable value, no union; a bare one-branch oneOf stays one, as in walkCombined.
        heldIn(options.get(0), value, branches, held, kindAt);
        return;
      }
      // As the walk validates it: json-sKema counts 1.0 an integer, everit does not.
      Object validatable = validatable(
          schema instanceof CombinedSchemaExt ? integralDecimals(value) : value);
      boolean fits = false;
      boolean mapBranch = false;
      for (int i = 0; i < options.size(); i++) {
        List<Integer> in = new ArrayList<>(branches);
        in.add(i);
        String kind = kindAt.apply(in);
        mapBranch |= kind != null && kind.startsWith("MAP");
        if (validates(options.get(i), validatable)) {
          fits = true;
          if (!value.isObject() || kind != null && !kind.equals("STRUCT")) {
            held.add(in);
          }
          heldIn(options.get(i), value, in, held, kindAt);
        }
      }
      // An object fits a map branch as well as a struct one: a map's union decides it too.
      if (!fits && !value.isNull() && (!value.isObject() || mapBranch)) {
        // In none of the branches: which it was written in decides.
        List<Integer> none = new ArrayList<>(branches);
        none.add(NO_BRANCH);
        held.add(none);
      }
    } else if (schema instanceof ArraySchema && value.isArray()) {
      Schema items = ((ArraySchema) schema).getAllItemSchema();
      for (JsonNode element : value) {
        heldIn(items, element, branches, held, kindAt);
      }
    } else if (schema instanceof ObjectSchema && value.isObject()) {
      ObjectSchema object = (ObjectSchema) schema;
      Iterator<Map.Entry<String, JsonNode>> fields = value.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        if (!object.getPropertySchemas().containsKey(field.getKey())) {
          heldIn(object.getSchemaOfAdditionalProperties(), field.getValue(), branches, held,
              kindAt);
        }
      }
    }
  }

  // The schema a chain of references ends at; one only referring to itself, however indirectly.
  private static Schema referred(Schema schema) {
    Set<Schema> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    while (schema instanceof ReferenceSchema && seen.add(schema)) {
      schema = ((ReferenceSchema) schema).getReferredSchema();
    }
    return schema;
  }

  // Whether a branch of the union reached by these choices continues one the value was written
  // in, as the writer read it.
  private static boolean continuesWritten(Target target, List<Integer> union,
      Set<List<Integer>> writtenAs) {
    if (writtenAs == null) {
      return false;
    }
    for (Map.Entry<List<Integer>, Candidate> branch : target.branches.entrySet()) {
      if (isBranchOf(branch.getKey(), union) && branch.getValue().continues
          && writtenAs.contains(branch.getValue().writerChoices)) {
        return true;
      }
    }
    return false;
  }

  // Whether the union reached by these branch choices has a branch new to the pairing: one under
  // a plain additionalProperties has no branches, and no provenance to follow.
  private static boolean hasNewBranch(Target target, List<Integer> union) {
    for (Map.Entry<List<Integer>, Candidate> branch : target.branches.entrySet()) {
      if (isBranchOf(branch.getKey(), union) && !branch.getValue().continues) {
        return true;
      }
    }
    return false;
  }

  private static boolean isBranchOf(List<Integer> branch, List<Integer> union) {
    return branch.size() == union.size() + 1 && branch.subList(0, union.size()).equals(union);
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
    // Where a default was put, by object and name: Jackson shares one node for small values, so
    // the value itself cannot tell a default from a property holding the same.
    Map<ObjectNode, Set<String>> placed = new IdentityHashMap<>();
    // How the writer reads each property, before anything is pruned: the branches taken to it,
    // and, for a primitive under a union of its own, the branch holding it.
    Map<Target, Map<ObjectNode, Set<List<Integer>>>> written = new IdentityHashMap<>();
    if (writer != null) {
      for (Target target : targets) {
        Map<ObjectNode, Set<List<Integer>>> readings = new IdentityHashMap<>();
        walk(target.names, writer, document, 0, new ArrayList<>(), false, Collections.emptyList(),
            new Reached(), (node, object, name, choices, ambiguous, alternatives) -> {
              Set<List<Integer>> as = readings.computeIfAbsent(node, n -> new HashSet<>());
              as.add(choices);
              if (!target.branches.isEmpty()) {
                for (List<Integer> branches
                    : heldIn(object, name, node.get(name), choices, target.writerKinds)) {
                  List<Integer> extended = new ArrayList<>(choices);
                  extended.addAll(branches);
                  as.add(extended);
                }
              }
            });
        written.put(target, readings);
      }
    }
    // Required only by a sibling's presence: decided once pruning settles, as the sibling may be
    // pruned too.
    List<Deferred> deferred = new ArrayList<>();
    // Objects held only in new struct branches: once every member is pruned, the empty object
    // is no value of the reader's either, so the property goes too (an extra key keeps it).
    Map<ObjectNode, Map<String, Deferred>> emptied = new IdentityHashMap<>();
    // A property removed can change how the rest of the value reads: pruned until nothing more is.
    boolean changed = true;
    while (changed) {
      changed = false;
      for (Target target : targets) {
        // allOf parts and ambiguous branches reach one value more than once: decided together.
        Map<ObjectNode, List<Reach>> reached = new IdentityHashMap<>();
        Map<ObjectNode, Set<List<Integer>>> readings = written.get(target);
        walk(target.names, reader, document, 0, new ArrayList<>(), false,
            Collections.emptyList(), new Reached(),
            (node, object, name, choices, ambiguous, alternatives) -> {
              if (!ambiguous && !defaults.contains(node)
                  && inNewStructBranchOnly(target, choices, object, name, node.get(name))) {
                emptied.computeIfAbsent(node, n -> new HashMap<>()).putIfAbsent(name,
                    new Deferred(node, Collections.singletonList(new Reach(object, alternatives)),
                        name, target.names));
              }
              if (!defaults.contains(node)
                  && !placed.getOrDefault(node, Collections.emptySet()).contains(name)
                  && !keeps(target, choices, ambiguous, node, object, name,
                      readings != null ? readings.get(node) : null)) {
                reached.computeIfAbsent(node, n -> new ArrayList<>())
                    .add(new Reach(object, alternatives));
              }
            });
        String name = target.names.get(target.names.size() - 1);
        for (Map.Entry<ObjectNode, List<Reach>> e : reached.entrySet()) {
          JsonNode before = e.getKey().get(name);
          JsonNode value = remove(e.getKey(), e.getValue(), name, target.names, deferred);
          if (value != null) {
            addContainers(value, defaults);
            placed.computeIfAbsent(e.getKey(), n -> new HashSet<>()).add(name);
          }
          changed |= e.getKey().get(name) != before;
        }
      }
      for (Map<String, Deferred> byName : emptied.values()) {
        for (Deferred d : byName.values()) {
          JsonNode value = d.node.get(d.name);
          if (value != null && value.isObject() && value.size() == 0) {
            JsonNode placedValue = remove(d.node, d.reaches, d.name, d.names, deferred);
            if (placedValue != null) {
              addContainers(placedValue, defaults);
              placed.computeIfAbsent(d.node, n -> new HashSet<>()).add(d.name);
            }
            changed = true;
          }
        }
      }
      emptied.values().forEach(byName -> byName.values()
          .removeIf(d -> d.node.get(d.name) == null || placed.getOrDefault(d.node,
              Collections.emptySet()).contains(d.name)));
    }
    // A deferred property whose object was pruned since stands for nothing in the document.
    Set<JsonNode> attached = Collections.newSetFromMap(new IdentityHashMap<>());
    if (!deferred.isEmpty()) {
      addContainers(document, attached);
    }
    // A default put for one deferred property can make another required, as along a chain of
    // dependencies: decided again until none is put.
    boolean put = true;
    while (put) {
      put = false;
      for (Deferred d : deferred) {
        if (attached.contains(d.node) && !d.node.has(d.name)
            && requiredInEveryReading(d.reaches, d.name, d.node, true)) {
          placeDefault(d.node, d.reaches, d.name, d.names);
          put = true;
        }
      }
    }
  }

  /**
   * Whether the property's object value fits only struct branches of its own union that are new
   * to the reader: their members are all new, so nothing of the object continues.
   */
  private static boolean inNewStructBranchOnly(Target target, List<Integer> choices,
      ObjectSchema object, String name, JsonNode value) {
    if (value == null || !value.isObject() || target.branches.isEmpty()) {
      return false;
    }
    Schema schema = referred(object.getPropertySchemas().get(name));
    if (!(schema instanceof CombinedSchema)
        || ((CombinedSchema) schema).getCriterion() == CombinedSchema.ALL_CRITERION) {
      return false;
    }
    List<Schema> options = new ArrayList<>();
    for (Schema subschema : ((CombinedSchema) schema).getSubschemas()) {
      if (!(referred(subschema) instanceof NullSchema)) {
        options.add(subschema);
      }
    }
    Object validatable = validatable(
        schema instanceof CombinedSchemaExt ? integralDecimals(value) : value);
    boolean any = false;
    for (int i = 0; i < options.size(); i++) {
      List<Integer> in = new ArrayList<>(choices);
      in.add(i);
      if (validates(options.get(i), validatable)) {
        Candidate branch = target.branches.get(in);
        if (branch == null || branch.continues || !"STRUCT".equals(target.readerKinds.get(in))) {
          return false;
        }
        any = true;
      }
    }
    return any;
  }

  /** A pruned property only a sibling's presence requires, decided once pruning settles. */
  private static final class Deferred {
    final ObjectNode node;
    final List<Reach> reaches;
    final String name;
    final List<String> names;

    Deferred(ObjectNode node, List<Reach> reaches, String name, List<String> names) {
      this.node = node;
      this.reaches = reaches;
      this.name = name;
      this.names = names;
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
    // A reading of the union by a branch not declaring the property, where it is only an extra.
    static final int EXTRA = -1;

    final Schema union;
    final int branch;
    // Whether the union also has that reading.
    final boolean extra;

    Alternative(Schema union, int branch, boolean extra) {
      this.union = union;
      this.branch = branch;
      this.extra = extra;
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
        Collections.emptyList(), new Reached(),
        (node, object, name, choices, ambiguous, alternatives) -> taken.add(choices));
    return taken.isEmpty() ? null : taken.get(0);
  }

  /** As the response spells them: the branch index of every union branch on the way to a path. */
  static List<Integer> branchChoicesAt(ProvenanceMapping mapping, List<Integer> path) {
    return branchChoices(mapping, path);
  }

  /**
   * The branch index of every union branch on the way to {@code path}, outermost first.
   */
  private static List<Integer> branchChoices(ProvenanceMapping mapping, List<Integer> path) {
    List<Integer> choices = new ArrayList<>();
    for (int k = 1; k <= path.size(); k++) {
      List<Integer> prefix = path.subList(0, k);
      if (mapping.isReaderBranch(prefix)) {
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
      List<Integer> choices, boolean ambiguous, List<Alternative> alternatives, Reached reached,
      AtProperty at) {
    if (schema == null || node == null) {
      return;
    }
    if (schema instanceof ReferenceSchema) {
      walk(names, ((ReferenceSchema) schema).getReferredSchema(), node, step, choices, ambiguous,
          alternatives, reached, at);
      return;
    }
    if (schema instanceof CombinedSchema) {
      walkCombined(names, (CombinedSchema) schema, node, step, choices, ambiguous, alternatives,
          reached, at);
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
        walk(names, child, it.next(), step + 1, choices, ambiguous, alternatives, reached, at);
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
            ambiguous, alternatives, reached, at);
      }
    } else if (declared || object.getRequiredProperties().contains(name)
        || dependedOn(object, name)) {
      // A part that only requires the property, as allOf [Base, {required: [p]}] or a
      // dependencies clause, is reached too.
      at.accept((ObjectNode) node, object, name, choices, ambiguous, alternatives);
    }
  }

  private static void walkCombined(List<String> names, CombinedSchema schema, JsonNode node,
      int step, List<Integer> choices, boolean ambiguous, List<Alternative> alternatives,
      Reached reached, AtProperty at) {
    String name = names.get(step);
    if (name != null && !node.has(name)) {
      // Every branch reaches the property as a member of this very value: nothing to prune.
      return;
    }
    List<Schema> subschemas = new ArrayList<>(schema.getSubschemas());
    if (schema.getCriterion() == CombinedSchema.ALL_CRITERION) {
      // The converter merges an allOf; the next step lives in whichever part declares it.
      for (Schema part : subschemas) {
        walk(names, part, node, step, choices, ambiguous, alternatives, reached, at);
      }
      return;
    }
    List<Schema> branches = new ArrayList<>();
    for (Schema subschema : subschemas) {
      if (!(referred(subschema) instanceof NullSchema)) {
        branches.add(subschema);
      }
    }
    if (branches.size() == 1 && branches.size() < subschemas.size()) {
      // A nullable union, which the logical type collapses: no branch step. A bare one-branch
      // oneOf stays a union, as provenance's V1 conversion keeps it; V2 unwraps it, so moving
      // provenance to V2 changes this rule, and heldIn's.
      walk(names, branches.get(0), node, step, choices, ambiguous, alternatives, reached, at);
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
        boolean fits = validates(branches.get(i), validatable);
        // A nested union fitting only through a branch that does not declare the step holds the
        // property as an extra, as a flat union does: not a reading of it.
        if (fits && !reaches(names, branches.get(i), node, step, reached)) {
          continue;
        }
        declaring.add(i);
        if (fits) {
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
    // A branch not declaring the property that fits too is another reading, where it is an extra.
    // It is judged on the value as pruned: a closed branch rejects the property, not its absence.
    boolean extra = false;
    Object pruned = !fallback && step == names.size() - 1 && name != null && node.isObject()
        ? validatable(withoutProperty((ObjectNode) node, name, schema)) : validatable;
    for (int i = 0; i < branches.size() && !fallback && !extra; i++) {
      extra = !declaring.contains(i) && validates(branches.get(i), pruned);
    }
    // Only where several readings remain are they alternatives; one taken alone is the value's.
    boolean alternative = walked.size() > 1 || extra;
    for (int i : walked) {
      List<Integer> extended = new ArrayList<>(choices);
      extended.add(i);
      List<Alternative> readings = alternatives;
      if (alternative) {
        readings = new ArrayList<>(alternatives);
        readings.add(new Alternative(schema, i, extra));
      }
      walk(names, branches.get(i), node, step, extended,
          ambiguous || fallback || valid.size() > 1, readings, reached, at);
    }
  }

  /**
   * Whether the value may stay: exactly one location matches the branches taken, and it
   * continues.
   */
  private static boolean keeps(Target target, List<Integer> choices, boolean ambiguous,
      ObjectNode node, ObjectSchema object, String name, Set<List<Integer>> writtenAs) {
    List<Candidate> matches = target.byChoices.get(choices);
    Candidate match = matches != null && matches.size() == 1 ? matches.get(0) : null;
    boolean known = writtenAs != null && !writtenAs.isEmpty();
    if (!target.anyNew) {
      // Every location spelled so continues: only a value read, unambiguously, at one continuing
      // another than it was written as is pruned.
      if (ambiguous || match == null || !known) {
        return true;
      }
      return writtenAs.contains(match.writerChoices) && heldAsWritten(target, choices, node,
          object, name, writtenAs);
    }
    if (!target.clashes() || ambiguous || match == null || !match.continues) {
      return false;
    }
    if (known && !writtenAs.contains(match.writerChoices)) {
      return false;
    }
    return heldAsWritten(target, choices, node, object, name, known ? writtenAs : null);
  }

  /**
   * Whether the value reads, in each union branch it is held in, as one continuing — and, the
   * writer known, one it is held in under the writer. A branch no location stands for, as under a
   * plain {@code additionalProperties}, has no provenance to follow.
   */
  private static boolean heldAsWritten(Target target, List<Integer> choices, ObjectNode node,
      ObjectSchema object, String name, Set<List<Integer>> writtenAs) {
    if (target.branches.isEmpty()) {
      return true;
    }
    for (List<Integer> branches
        : heldIn(object, name, node.get(name), choices, target.readerKinds)) {
      List<Integer> extended = new ArrayList<>(choices);
      extended.addAll(branches);
      if (extended.get(extended.size() - 1) == NO_BRANCH) {
        // In none of the reader's branches: a new one may still take it, as a lenient reader
        // counts 1.0 an integer, unless the branch it was written in continues into one.
        List<Integer> union = extended.subList(0, extended.size() - 1);
        if (hasNewBranch(target, union) && !continuesWritten(target, union, writtenAs)) {
          return false;
        }
        continue;
      }
      Candidate held = target.branches.get(extended);
      if (held != null && (!held.continues
          || writtenAs != null && !writtenAs.contains(held.writerChoices))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Removes {@code name} from {@code node}, reached by {@code reaches}, and returns the default
   * put in its place, if any. Every reading of the value — one branch of each ambiguous union
   * taken on the way — applies every reach consistent with it, and allOf parts all apply: the
   * property is required if some applying reach requires it in every reading.
   */
  private static JsonNode remove(ObjectNode node, List<Reach> reaches, String name,
      List<String> names, List<Deferred> deferred) {
    boolean declared = false;
    for (Reach reach : reaches) {
      declared |= reach.object.getPropertySchemas().get(name) != null;
    }
    if (!declared) {
      // Only parts requiring it reached the property: an extra property, not a location.
      return null;
    }
    node.remove(name);
    if (!requiredInEveryReading(reaches, name, node, false)) {
      // Required only by a sibling's presence: decided once pruning settles, even where the
      // sibling is gone for now, as its default may yet be put.
      if (reaches.stream().anyMatch(reach -> dependedOn(reach.object, name))) {
        deferred.add(new Deferred(node, reaches, name, names));
      }
      return null;
    }
    return placeDefault(node, reaches, name, names);
  }

  /**
   * Puts the reader's default for a pruned property that every reading requires.
   *
   * @throws SerializationException if it declares none, or one that does not validate under it
   */
  private static JsonNode placeDefault(ObjectNode node, List<Reach> reaches, String name,
      List<String> names) {
    Schema withDefault = null;
    for (Reach reach : reaches) {
      if (withDefault == null) {
        withDefault = withDefault(reach.object.getPropertySchemas().get(name));
      }
    }
    if (withDefault == null) {
      throw new SerializationException("Property " + names + " is new to the reader: "
          + "provenance pairs it with nothing the writer wrote, and the reader requires it and "
          + "declares no default. There is no value to read.");
    }
    try {
      JsonNode value = MAPPER.readTree(JSONObject.valueToString(withDefault.getDefaultValue()));
      if (!validates(withDefault, validatable(value))) {
        // A default the reader itself rejects is no value to read either.
        throw new SerializationException("Property " + names + " is new to the reader, and the "
            + "default it declares does not validate under it. There is no value to read.");
      }
      node.set(name, value);
      return value;
    } catch (IOException e) {
      throw new SerializationException("Could not read the default of property '" + name + "'", e);
    }
  }

  // Whether some property's presence makes name required: dependencies (either form),
  // dependentRequired, or dependentSchemas.
  private static boolean dependedOn(ObjectSchema object, String name) {
    return object.getPropertyDependencies().values().stream().anyMatch(d -> d.contains(name))
        || object.getSchemaDependencies().values().stream().anyMatch(d -> requires(d, name));
  }

  // Whether a property present in node makes name required: dependencies (either form),
  // dependentRequired, or dependentSchemas.
  private static boolean requiredBy(ObjectSchema object, ObjectNode node, String name) {
    return object.getPropertyDependencies().entrySet().stream()
        .anyMatch(e -> node.has(e.getKey()) && e.getValue().contains(name))
        || object.getSchemaDependencies().entrySet().stream()
            .anyMatch(e -> node.has(e.getKey()) && requires(e.getValue(), name));
  }

  // Whether a dependency's schema requires name: directly, or in any part of an allOf.
  private static boolean requires(Schema schema, String name) {
    Schema object = referred(schema);
    if (object instanceof CombinedSchema
        && ((CombinedSchema) object).getCriterion() == CombinedSchema.ALL_CRITERION) {
      return ((CombinedSchema) object).getSubschemas().stream()
          .anyMatch(part -> requires(part, name));
    }
    return object instanceof ObjectSchema
        && ((ObjectSchema) object).getRequiredProperties().contains(name);
  }

  // Whether walking the value under schema reaches a declaration of the property.
  private static boolean reaches(List<String> names, Schema schema, JsonNode node, int step,
      Reached reached) {
    Map<Integer, Boolean> bySteps = reached.answers
        .computeIfAbsent(schema, k -> new IdentityHashMap<>())
        .computeIfAbsent(node, k -> new HashMap<>());
    Boolean known = bySteps.get(step);
    if (known != null) {
      return known;
    }
    boolean[] declared = {false};
    walk(names, schema, node, step, new ArrayList<>(), false, Collections.emptyList(), reached,
        (n, object, name, choices, ambiguous, alternatives) ->
            declared[0] |= object.getPropertySchemas().containsKey(name));
    bySteps.put(step, declared[0]);
    return declared[0];
  }

  // Answers of reaches within one top-level walk, which never changes the document, by branch,
  // value and step: a union nested k deep is then walked once rather than 2^k times.
  private static final class Reached {
    private final Map<Schema, Map<JsonNode, Map<Integer, Boolean>>> answers =
        new IdentityHashMap<>();
  }

  private static JsonNode withoutProperty(ObjectNode node, String name, CombinedSchema schema) {
    ObjectNode copy = node.deepCopy();
    copy.remove(name);
    return schema instanceof CombinedSchemaExt ? integralDecimals(copy) : copy;
  }

  /**
   * {@code property}, or the schema it refers to, that declares a default, as the converter reads
   * one; null if none does.
   */
  private static Schema withDefault(Schema property) {
    Set<Schema> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Schema schema = property; schema != null && seen.add(schema);
        schema = schema instanceof ReferenceSchema
            ? ((ReferenceSchema) schema).getReferredSchema() : null) {
      if (schema.hasDefaultValue()) {
        return schema;
      }
    }
    return null;
  }

  /**
   * Whether every reading of the value requires {@code name}: each combination of the branches
   * seen for each ambiguous union, over the reaches consistent with it.
   */
  private static boolean requiredInEveryReading(List<Reach> reaches, String name,
      ObjectNode node, boolean dependencies) {
    boolean anyRequires = false;
    boolean allRequire = true;
    boolean extras = false;
    for (Reach reach : reaches) {
      boolean requires = reach.object.getRequiredProperties().contains(name)
          || dependencies && requiredBy(reach.object, node, name);
      if (requires && reach.alternatives.isEmpty()) {
        // An allOf part or an unambiguous step applies in every reading.
        return true;
      }
      anyRequires |= requires;
      allRequire &= requires;
      extras |= reach.alternatives.stream().anyMatch(a -> a.extra);
    }
    if (!anyRequires || allRequire && !extras) {
      return anyRequires;
    }
    Map<Schema, List<Integer>> unions = new IdentityHashMap<>();
    for (Reach reach : reaches) {
      for (Alternative alternative : reach.alternatives) {
        List<Integer> seen = unions.computeIfAbsent(alternative.union, u -> new ArrayList<>());
        if (!seen.contains(alternative.branch)) {
          seen.add(alternative.branch);
        }
        if (alternative.extra && !seen.contains(Alternative.EXTRA)) {
          seen.add(Alternative.EXTRA);
        }
      }
    }
    // Unions a requiring reach depends on first, so a reading it settles is dropped early.
    List<Schema> order = new ArrayList<>(unions.keySet());
    Set<Schema> requiring = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Reach reach : reaches) {
      if (reach.object.getRequiredProperties().contains(name)) {
        reach.alternatives.forEach(a -> requiring.add(a.union));
      }
    }
    order.sort(Comparator.comparing(u -> !requiring.contains(u)));
    int[] budget = {MAX_READINGS};
    boolean witness = witness(order, 0, unions, new IdentityHashMap<>(), reaches, name, budget);
    // Past the budget, required only where every reach requires it: none here.
    return !witness && budget[0] >= 0;
  }

  /**
   * Whether some completion of {@code reading} is a reading of the value no requiring reach
   * applies in, though another does; false, with {@code budget} spent, once it runs out.
   */
  private static boolean witness(List<Schema> order, int depth, Map<Schema, List<Integer>> unions,
      Map<Schema, Integer> reading, List<Reach> reaches, String name, int[] budget) {
    if (--budget[0] < 0) {
      return false;
    }
    for (Reach reach : reaches) {
      if (reach.object.getRequiredProperties().contains(name) && applies(reach, reading)) {
        // It applies however the rest is read.
        return false;
      }
    }
    if (depth == order.size()) {
      // A reading where the property is an extra requires nothing, though no reach applies.
      return reachesExtra(reading, reaches)
          || reaches.stream().anyMatch(reach -> applies(reach, reading));
    }
    Schema union = order.get(depth);
    for (int branch : unions.get(union)) {
      reading.put(union, branch);
      boolean found = witness(order, depth + 1, unions, reading, reaches, name, budget);
      reading.remove(union);
      if (found || budget[0] < 0) {
        return found;
      }
    }
    return false;
  }

  /**
   * Whether {@code reading} takes some union's extra reading and reaches that union: a reach under
   * it agrees with the reading on every other union.
   */
  private static boolean reachesExtra(Map<Schema, Integer> reading, List<Reach> reaches) {
    for (Reach reach : reaches) {
      for (Alternative alternative : reach.alternatives) {
        if (alternative.extra
            && Integer.valueOf(Alternative.EXTRA).equals(reading.get(alternative.union))
            && reach.alternatives.stream().allMatch(other -> other.union == alternative.union
                || Integer.valueOf(other.branch).equals(reading.get(other.union)))) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean applies(Reach reach, Map<Schema, Integer> reading) {
    return reach.alternatives.stream()
        .allMatch(a -> Integer.valueOf(a.branch).equals(reading.get(a.union)));
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
