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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The names of the messages and enums the Protobuf writer generates in a message, one per field
 * or oneof member: {@code <UpperCamel(field)><Suffix>} unless the field's or branch's {@link #NAME}
 * param pins one. A derived name that a pinned one already takes gets the next free counter before
 * its suffix ({@code Home2Row}), so a renamed field keeps its type's name.
 */
final class ProtoTypeNames {

  // The exact name of the message or enum a field or oneof member generates in its message.
  static final String NAME = Schema.PROTOBUF_NAME;

  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  // A message's pins, by the name its types derive from: a field's, or <oneof>_<branch>.
  private static final class Scope {
    private final Map<String, String> pins = new HashMap<>();
    private final Set<String> pinned = new HashSet<>();
  }

  private final Deque<Scope> scopes = new ArrayDeque<>();

  // Enters a message built from struct, until exit.
  void enter(Schema struct) {
    Scope scope = new Scope();
    for (Field field : struct.getFields()) {
      if (field.getSchema().getType() == Schema.Type.UNION) {
        for (UnionBranch branch : field.getSchema().getBranches()) {
          pin(scope, field.getName() + "_" + branch.getName(), branch.getParams());
        }
      } else {
        pin(scope, field.getName(), field.getParams());
      }
    }
    scopes.push(scope);
  }

  void exit() {
    scopes.pop();
  }

  // The name of the type generated for fieldName, which would otherwise be derived.
  String name(String fieldName, String suffix, String derived) {
    Scope scope = scopes.peek();
    if (scope == null) {
      return derived;
    }
    String pin = scope.pins.get(fieldName);
    if (pin != null) {
      // The reader knows a map entry by its suffix.
      if (suffix.equals(CommonConstants.MAP_ENTRY_SUFFIX) && !pin.endsWith(suffix)) {
        throw new ValidationException("Param " + NAME + " of map field " + fieldName
            + " must end with " + CommonConstants.MAP_ENTRY_SUFFIX + ", got '" + pin + "'");
      }
      return pin;
    }
    if (!scope.pinned.contains(derived)) {
      return derived;
    }
    String prefix = derived.substring(0, derived.length() - suffix.length());
    for (int n = 2; ; n++) {
      String candidate = prefix + n + suffix;
      if (!scope.pinned.contains(candidate)) {
        return candidate;
      }
    }
  }

  private static void pin(Scope scope, String name, Map<String, Object> params) {
    Object value = params.get(NAME);
    if (value == null) {
      return;
    }
    String pin = value.toString();
    if (!IDENTIFIER.matcher(pin).matches()) {
      throw new ValidationException(
          "Param " + NAME + " must be a Protobuf identifier, got '" + pin + "'");
    }
    scope.pins.put(name, pin);
    scope.pinned.add(pin);
  }
}
