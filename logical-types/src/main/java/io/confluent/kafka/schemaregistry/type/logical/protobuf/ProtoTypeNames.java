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

package io.confluent.kafka.schemaregistry.type.logical.protobuf;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The names of the messages and enums the Protobuf writer generates in a message, one per field
 * or oneof member: {@code <UpperCamel(field)><Suffix>} unless the field's or branch's {@link #NAME}
 * param pins one. A derived name already taken in the message, by a pin, a field, a oneof, a
 * declared nested type or a type generated before it ({@code a_b} and {@code aB} both derive
 * {@code ABRow}), gets the next free counter before its suffix ({@code AB2Row}); the reader then
 * pins it.
 */
final class ProtoTypeNames {

  // The exact name of the message or enum a field or oneof member generates in its message.
  static final String NAME = Schema.PROTOBUF_NAME;

  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  // A message's pins, by field and by oneof member (whose derived <oneof>_<branch> can be a
  // field's name), the names taken in it, and the member being written, if any.
  private static final class Scope {
    private final Map<String, String> pins = new HashMap<>();
    private final Map<List<String>, String> branchPins = new HashMap<>();
    private final Set<String> taken = new HashSet<>();
    private List<String> branch;
  }

  private final Deque<Scope> scopes = new ArrayDeque<>();

  // Enters a message built from struct, with its declared nested types, until exit.
  void enter(Schema struct, List<String> nestedTypes) {
    Scope scope = new Scope();
    scope.taken.addAll(nestedTypes);
    for (Field field : struct.getFields()) {
      scope.taken.add(field.getName());
      if (field.getSchema().getType() == Schema.Type.UNION) {
        for (UnionBranch branch : field.getSchema().getBranches()) {
          String pin = pin(scope, branch.getParams());
          if (pin != null) {
            scope.branchPins.put(Arrays.asList(field.getName(), branch.getName()), pin);
          }
        }
      } else {
        String pin = pin(scope, field.getParams());
        if (pin != null) {
          scope.pins.put(field.getName(), pin);
        }
      }
    }
    scopes.push(scope);
  }

  void exit() {
    scopes.pop();
  }

  // Takes names in the message entered last, which no derived name then takes.
  void reserve(Collection<String> names) {
    scopes.peek().taken.addAll(names);
  }

  // Names the type of oneof member branch of union until exitBranch.
  void enterBranch(String union, String branch) {
    scopes.peek().branch = Arrays.asList(union, branch);
  }

  void exitBranch() {
    scopes.peek().branch = null;
  }

  // The name of the type generated for fieldName, which would otherwise be derived.
  String name(String fieldName, String suffix, String derived) {
    Scope scope = scopes.peek();
    if (scope == null) {
      return derived;
    }
    String pin = scope.branch != null
        ? scope.branchPins.get(scope.branch) : scope.pins.get(fieldName);
    if (pin != null) {
      // The reader knows a map entry by its suffix.
      if (suffix.equals(CommonConstants.MAP_ENTRY_SUFFIX) && !pin.endsWith(suffix)) {
        throw new ValidationException("Param " + NAME + " of map field " + fieldName
            + " must end with " + CommonConstants.MAP_ENTRY_SUFFIX + ", got '" + pin + "'");
      }
      return pin;
    }
    String name = derived;
    // The counter goes before the suffix, and before V1's separator (a_Row, a2_Row).
    String prefix = derived.substring(0, derived.length() - suffix.length());
    String separator = prefix.endsWith("_") ? "_" : "";
    prefix = prefix.substring(0, prefix.length() - separator.length());
    for (int n = 2; scope.taken.contains(name); n++) {
      name = prefix + n + separator + suffix;
    }
    scope.taken.add(name);
    return name;
  }

  private static String pin(Scope scope, Map<String, Object> params) {
    Object value = params.get(NAME);
    if (value == null) {
      return null;
    }
    String pin = value.toString();
    if (!IDENTIFIER.matcher(pin).matches()) {
      throw new ValidationException(
          "Param " + NAME + " must be a Protobuf identifier, got '" + pin + "'");
    }
    scope.taken.add(pin);
    return pin;
  }
}
