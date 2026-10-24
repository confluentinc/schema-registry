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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The names of the record, enum, fixed and map entry types the Avro writer generates, which derive
 * from a field's base: {@code <record>_<field>} unless the field's {@link #NAME} param pins one.
 *
 * <p>Names under a pinned base are pinned too. A first pass records them and the names under each
 * derived base; when any are pinned, a second pass gives a derived base whose names would take a
 * pinned one the next free suffix ({@code p_f0_2}), so a renamed field keeps its types' names.
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

  // Pass 1 output, or for pass 2 the first pass's: names pinned, derived names under each base.
  private final Set<String> pinned = new HashSet<>();
  private final Map<String, Set<String>> namesByBase = new HashMap<>();
  private final Set<String> all = new HashSet<>();
  private final AvroTypeNames first;

  // Walk state.
  private int pinnedDepth;
  private Deque<String> openBases = new ArrayDeque<>();
  private final Map<String, Map<String, Object>> exceptionsByBase = new HashMap<>();

  AvroTypeNames() {
    this(null);
  }

  private AvroTypeNames(AvroTypeNames first) {
    this.first = first;
  }

  boolean hasPinned() {
    return !pinned.isEmpty();
  }

  // A second pass, which suffixes a derived base whose names would take a pinned one.
  AvroTypeNames forSuffixing() {
    return new AvroTypeNames(this);
  }

  // The base of a field's types, and enters it until exitField.
  String enterField(String rowName, String fieldName, Map<String, Object> params) {
    String pin = name(params, NAME);
    String base;
    if (pin != null) {
      base = pin;
      pinnedDepth++;
    } else {
      // Under a pinned base every name is pinned, so none of them yields.
      String derived = rowName + "_" + fieldName;
      base = pinnedDepth > 0 ? derived : suffixed(derived);
      openBases.push(base);
    }
    if (params.containsKey(ENTRY_NAME) || params.containsKey(KEY_NAME)
        || params.containsKey(VALUE_NAME)) {
      exceptionsByBase.put(base, params);
    }
    return base;
  }

  void exitField(Map<String, Object> params) {
    if (params.containsKey(NAME)) {
      pinnedDepth--;
    } else {
      openBases.pop();
    }
  }

  // A union branch's base: its NAME param, entered until exitBranch.
  String enterBranch(String derived, Map<String, Object> params) {
    String pin = name(params, NAME);
    if (pin == null) {
      return derived;
    }
    pinnedDepth++;
    return pin;
  }

  void exitBranch(Map<String, Object> params) {
    if (params.containsKey(NAME)) {
      pinnedDepth--;
    }
  }

  // A declared named type's names are its own; no field's base or pin reaches into it.
  Object enterNamedType() {
    Object saved = new Object[] {pinnedDepth, openBases};
    pinnedDepth = 0;
    openBases = new ArrayDeque<>();
    return saved;
  }

  @SuppressWarnings("unchecked")
  void exitNamedType(Object saved) {
    Object[] state = (Object[]) saved;
    pinnedDepth = (Integer) state[0];
    openBases = (Deque<String>) state[1];
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

  // Records a generated type's name; pinned for one a param names directly.
  void created(String name, boolean pinned) {
    all.add(name);
    if (pinned || pinnedDepth > 0) {
      this.pinned.add(name);
    } else {
      for (String base : openBases) {
        namesByBase.computeIfAbsent(base, b -> new HashSet<>()).add(name);
      }
    }
  }

  private String suffixed(String base) {
    if (first == null) {
      return base;
    }
    Set<String> names = first.namesByBase.get(base);
    if (names == null || names.stream().noneMatch(first.pinned::contains)) {
      return base;
    }
    for (int n = 2; ; n++) {
      String candidate = base + "_" + n;
      boolean free = names.stream()
          .map(name -> candidate + name.substring(base.length()))
          .noneMatch(name -> first.pinned.contains(name) || first.all.contains(name));
      if (free) {
        return candidate;
      }
    }
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
