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

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The names of the record, enum, fixed and map entry types the Avro writer generates, which derive
 * from a field's base: {@code <record>_<field>} unless the field's {@link #NAME} param pins one.
 *
 * <p>Two types can derive one name ({@code a.b} and {@code a_b} are both {@code p_a_b}), and a
 * derived name can be one a param pins or a named type has. A first pass records every name with
 * the type built under it. When two different types share one, a second pass gives a derived type
 * the next free suffix ({@code p_a_b_2}) if a pinned or named type has its name, or a derived type
 * before it keeps the name; the reader then pins the suffix. Identical types share a name, since
 * Avro writes the second as a reference to the first, except as members of one union, which Avro
 * refuses. A suffix renames the types derived under it, which can collide anew, so passes repeat
 * until none does.
 */
final class AvroTypeNames {

  // The base a field's type names derive from: <base>, <base>_<field>, ...
  static final String NAME = Schema.AVRO_NAME;
  // A map entry record's name, when it is not <base>_Entry.
  static final String ENTRY_NAME = Schema.AVRO_ENTRY_NAME;
  // A map key's type name, when it is not <base>_key.
  static final String KEY_NAME = Schema.AVRO_KEY_NAME;
  // A map value's type name, when it is not <base>_value.
  static final String VALUE_NAME = Schema.AVRO_VALUE_NAME;

  static final String ENTRY_SUFFIX = "_Entry";
  static final String KEY_SUFFIX = "_key";
  static final String VALUE_SUFFIX = "_value";

  private static final Pattern NAME_PART = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  // How a type got its name: derived from its field, pinned by a param, or its own (a named
  // type's, the root's).
  enum Kind { DERIVED, PINNED, FIXED }

  private static final class Site {
    // The name the walk asked for, and the one given, a suffix if it yielded
    private final String requested;
    private final String name;
    private final Kind kind;
    private Object type;

    private Site(String requested, String name, Kind kind) {
      this.requested = requested;
      this.name = name;
      this.kind = kind;
    }
  }

  // Passes beyond this many mean the suffixes do not settle.
  private static final int MAX_PASSES = 100;

  // Every generated type, in the order the writer builds them, which every pass shares.
  private final List<Site> sites = new ArrayList<>();
  // Names of named types outside the logical type, which a derived name yields to.
  private final Set<String> reserved = new HashSet<>();
  // Derived types that must yield to another member of their union of the same name.
  private final Set<Integer> mustYield = new HashSet<>();
  // For a later pass: the previous pass's sites, the suffixed names of those that yield so far,
  // and how many passes came before.
  private final List<Site> previousSites;
  private final Map<Integer, String> renamed;
  private final int pass;

  // Walk state.
  private int pinnedDepth;
  private int depth;
  private final Map<String, Map<String, Object>> exceptionsByBase = new HashMap<>();
  // The exceptions each entered field or branch replaced, restored on exit: {base, previous}.
  private final Deque<Object[]> replacedExceptions = new ArrayDeque<>();

  AvroTypeNames() {
    this(Collections.emptyList(), Collections.emptyMap(), 0);
  }

  private AvroTypeNames(List<Site> previousSites, Map<Integer, String> renamed, int pass) {
    this.previousSites = previousSites;
    this.renamed = renamed;
    this.pass = pass;
  }

  void reserve(Set<String> names) {
    reserved.addAll(names);
  }

  // A next pass that suffixes the derived types that yield their names, or null if none does.
  AvroTypeNames forSuffixing() {
    Map<String, List<Integer>> byName = new LinkedHashMap<>();
    for (int i = 0; i < sites.size(); i++) {
      byName.computeIfAbsent(sites.get(i).name, n -> new ArrayList<>()).add(i);
    }
    Set<String> taken = new HashSet<>(reserved);
    taken.addAll(byName.keySet());
    Map<Integer, String> suffixed = new HashMap<>();
    for (Map.Entry<String, List<Integer>> e : byName.entrySet()) {
      // A name only one type has, and that need not yield, collides with nothing: its body,
      // which serializes the whole subtree, is never compared.
      if (e.getValue().size() == 1 && !reserved.contains(e.getKey())
          && !mustYield.contains(e.getValue().get(0))) {
        continue;
      }
      List<String> kept = new ArrayList<>();
      for (int i : e.getValue()) {
        if (sites.get(i).kind != Kind.DERIVED) {
          kept.add(body(i));
        }
      }
      for (int i : e.getValue()) {
        if (sites.get(i).kind != Kind.DERIVED) {
          continue;
        }
        String body = body(i);
        if (!mustYield.contains(i) && !reserved.contains(e.getKey())
            && (kept.isEmpty() || kept.contains(body))) {
          kept.add(body);
          continue;
        }
        String candidate;
        for (int n = 2; taken.contains(candidate = e.getKey() + "_" + n); n++) {
          // the next free suffix
        }
        taken.add(candidate);
        suffixed.put(i, candidate);
      }
    }
    if (suffixed.isEmpty()) {
      return null;
    }
    if (pass == MAX_PASSES) {
      throw new ValidationException("Could not give the Avro types distinct names");
    }
    Map<Integer, String> allRenamed = new HashMap<>(renamed);
    allRenamed.putAll(suffixed);
    AvroTypeNames next = new AvroTypeNames(sites, allRenamed, pass + 1);
    next.reserve(reserved);
    return next;
  }

  private String body(int index) {
    Object type = sites.get(index).type;
    return type != null ? type.toString() : "#" + index;
  }

  // The name a generated type takes: its own, or the suffix an earlier pass gave it.
  String allocate(String name, Kind kind) {
    int index = sites.size();
    Kind effective = kind == Kind.DERIVED && pinnedDepth > 0 ? Kind.PINNED : kind;
    String result = name;
    String suffix = renamed.get(index);
    // Under a type renamed since, a type's name derives from the new one and stands.
    if (suffix != null && previousSites.get(index).requested.equals(name)) {
      result = suffix;
    }
    sites.add(new Site(name, result, effective));
    return result;
  }

  /**
   * The members of a union, which Avro refuses two of one name. A derived member yields to the
   * other, whatever its body; until a next pass suffixes it, a stand-in keeps its place.
   */
  List<org.apache.avro.Schema> distinctMembers(List<org.apache.avro.Schema> members) {
    Map<String, org.apache.avro.Schema> byName = new HashMap<>();
    List<org.apache.avro.Schema> distinct = new ArrayList<>();
    // A stand-in takes a name no member has, nor an earlier stand-in.
    Set<String> taken = new HashSet<>();
    for (org.apache.avro.Schema member : members) {
      if (isNamed(member)) {
        taken.add(member.getFullName());
      }
    }
    for (org.apache.avro.Schema member : members) {
      if (!isNamed(member)) {
        distinct.add(member);
        continue;
      }
      String name = member.getFullName();
      org.apache.avro.Schema prior = byName.putIfAbsent(name, member);
      if (prior == null) {
        distinct.add(member);
        continue;
      }
      int yielding = derivedSite(member);
      if (yielding < 0) {
        yielding = derivedSite(prior);
      }
      if (yielding < 0) {
        throw new ValidationException("Avro type name '" + name + "' is used for two members "
            + "of one union; rename one of them");
      }
      mustYield.add(yielding);
      String standIn;
      for (int n = 1; !taken.add(standIn = name + "_standin" + n); n++) {
        // the next free name
      }
      distinct.add(org.apache.avro.Schema.createRecord(
          standIn, null, null, false, Collections.emptyList()));
    }
    return distinct;
  }

  private static boolean isNamed(org.apache.avro.Schema schema) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        return true;
      default:
        return false;
    }
  }

  // The site of a derived type, or -1 if the type was not derived.
  private int derivedSite(org.apache.avro.Schema type) {
    for (int i = 0; i < sites.size(); i++) {
      if (sites.get(i).type == type) {
        return sites.get(i).kind == Kind.DERIVED ? i : -1;
      }
    }
    return -1;
  }

  // The type of the name last allocated.
  void attach(Object type) {
    sites.get(sites.size() - 1).type = type;
  }

  // True outside every field and branch: the root's own type.
  boolean atRoot() {
    return depth == 0;
  }

  // The base of a field's types, and enters it until exitField.
  String enterField(String rowName, String fieldName, Map<String, Object> params) {
    depth++;
    String pin = name(params, NAME);
    String base = pin != null ? pin : rowName + "_" + fieldName;
    if (pin != null) {
      pinnedDepth++;
    }
    putExceptions(base, params);
    return base;
  }

  // A field's or branch's map name exceptions hold only while it is entered.
  private void putExceptions(String base, Map<String, Object> params) {
    if (hasExceptions(params)) {
      replacedExceptions.push(new Object[] {base, exceptionsByBase.put(base, params)});
    }
  }

  @SuppressWarnings("unchecked")
  private void restoreExceptions(Map<String, Object> params) {
    if (hasExceptions(params)) {
      Object[] replaced = replacedExceptions.pop();
      if (replaced[1] != null) {
        exceptionsByBase.put((String) replaced[0], (Map<String, Object>) replaced[1]);
      } else {
        exceptionsByBase.remove((String) replaced[0]);
      }
    }
  }

  private static boolean hasExceptions(Map<String, Object> params) {
    return params.containsKey(ENTRY_NAME) || params.containsKey(KEY_NAME)
        || params.containsKey(VALUE_NAME);
  }

  void exitField(Map<String, Object> params) {
    depth--;
    restoreExceptions(params);
    if (params.containsKey(NAME)) {
      pinnedDepth--;
    }
  }

  // A union branch's base, as a field's: its NAME param, entered until exitBranch.
  String enterBranch(String derived, Map<String, Object> params) {
    depth++;
    String pin = name(params, NAME);
    String base = pin != null ? pin : derived;
    if (pin != null) {
      pinnedDepth++;
    }
    putExceptions(base, params);
    return base;
  }

  void exitBranch(Map<String, Object> params) {
    depth--;
    restoreExceptions(params);
    if (params.containsKey(NAME)) {
      pinnedDepth--;
    }
  }

  // The walk state a declared named type's names are made apart from, restored after it.
  static final class Saved {
    private final int pinnedDepth;
    private final int depth;
    private final Map<String, Map<String, Object>> exceptions;

    private Saved(int pinnedDepth, int depth, Map<String, Map<String, Object>> exceptions) {
      this.pinnedDepth = pinnedDepth;
      this.depth = depth;
      this.exceptions = exceptions;
    }
  }

  // A declared named type's names are its own; no field's base or pin reaches into it.
  Saved enterNamedType() {
    final Saved saved = new Saved(pinnedDepth, depth, new HashMap<>(exceptionsByBase));
    pinnedDepth = 0;
    depth = 0;
    exceptionsByBase.clear();
    return saved;
  }

  void exitNamedType(Saved saved) {
    pinnedDepth = saved.pinnedDepth;
    depth = saved.depth;
    exceptionsByBase.clear();
    exceptionsByBase.putAll(saved.exceptions);
  }

  // The name of the map entry record under base.
  String entryName(String base) {
    String pin = mapPin(base, ENTRY_SUFFIX);
    return pin != null ? pin : base + ENTRY_SUFFIX;
  }

  // The name of a map's key or value type under base, entered until exitMapPart.
  String enterMapPart(String base, String suffix) {
    String pin = mapPin(base, suffix);
    if (pin == null) {
      return base + suffix;
    }
    pinnedDepth++;
    return pin;
  }

  void exitMapPart(String base, String suffix) {
    if (mapPin(base, suffix) != null) {
      pinnedDepth--;
    }
  }

  boolean isPinnedEntry(String base) {
    return mapPin(base, ENTRY_SUFFIX) != null;
  }

  private String mapPin(String base, String suffix) {
    Map<String, Object> exceptions = exceptionsByBase.get(base);
    String key = suffix.equals(ENTRY_SUFFIX) ? ENTRY_NAME
        : suffix.equals(KEY_SUFFIX) ? KEY_NAME : VALUE_NAME;
    return exceptions != null ? name(exceptions, key) : null;
  }

  private static String name(Map<String, Object> params, String key) {
    Object value = params.get(key);
    if (value == null) {
      return null;
    }
    String name = value.toString();
    for (String part : name.split("\\.", -1)) {
      if (!NAME_PART.matcher(part).matches()) {
        throw new ValidationException(
            "Param " + key + " must be an Avro full name, got '" + name + "'");
      }
    }
    return name;
  }
}
