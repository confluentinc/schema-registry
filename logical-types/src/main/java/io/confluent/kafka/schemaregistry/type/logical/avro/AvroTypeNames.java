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
 * Avro writes the second as a reference to the first.
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
    private final String name;
    private final Kind kind;
    private Object type;

    private Site(String name, Kind kind) {
      this.name = name;
      this.kind = kind;
    }
  }

  // Every generated type, in the order the writer builds them, which both passes share.
  private final List<Site> sites = new ArrayList<>();
  // Names of named types outside the logical type, which a derived name yields to.
  private final Set<String> reserved = new HashSet<>();
  // For a second pass: its first pass's sites, and the suffixed names of those that yield.
  private final List<Site> firstSites;
  private final Map<Integer, String> renamed;

  // Walk state.
  private int pinnedDepth;
  private int depth;
  private final Map<String, Map<String, Object>> exceptionsByBase = new HashMap<>();
  // The exceptions each entered field or branch replaced, restored on exit: {base, previous}.
  private final Deque<Object[]> replacedExceptions = new ArrayDeque<>();

  AvroTypeNames() {
    this(Collections.emptyList(), Collections.emptyMap());
  }

  private AvroTypeNames(List<Site> firstSites, Map<Integer, String> renamed) {
    this.firstSites = firstSites;
    this.renamed = renamed;
  }

  void reserve(Set<String> names) {
    reserved.addAll(names);
  }

  // A second pass that suffixes the derived types that yield their names, or null if none does.
  AvroTypeNames forSuffixing() {
    Map<String, List<Integer>> byName = new LinkedHashMap<>();
    for (int i = 0; i < sites.size(); i++) {
      byName.computeIfAbsent(sites.get(i).name, n -> new ArrayList<>()).add(i);
    }
    Set<String> taken = new HashSet<>(reserved);
    taken.addAll(byName.keySet());
    Map<Integer, String> suffixed = new HashMap<>();
    for (Map.Entry<String, List<Integer>> e : byName.entrySet()) {
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
        if (!reserved.contains(e.getKey()) && (kept.isEmpty() || kept.contains(body))) {
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
    AvroTypeNames second = new AvroTypeNames(sites, suffixed);
    second.reserve(reserved);
    return second;
  }

  private String body(int index) {
    Object type = sites.get(index).type;
    return type != null ? type.toString() : "#" + index;
  }

  // The name a generated type takes: its own, or the suffix the first pass gave it.
  String allocate(String name, Kind kind) {
    int index = sites.size();
    Kind effective = kind == Kind.DERIVED && pinnedDepth > 0 ? Kind.PINNED : kind;
    String result = name;
    String suffix = renamed.get(index);
    // Under a type renamed this pass, a type's name derives from the new one and stands.
    if (suffix != null && firstSites.get(index).name.equals(name)) {
      result = suffix;
    }
    sites.add(new Site(result, effective));
    return result;
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

  // A declared named type's names are its own; no field's base or pin reaches into it.
  int[] enterNamedType() {
    int[] saved = {pinnedDepth, depth};
    pinnedDepth = 0;
    depth = 0;
    return saved;
  }

  void exitNamedType(int[] saved) {
    pinnedDepth = saved[0];
    depth = saved[1];
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
