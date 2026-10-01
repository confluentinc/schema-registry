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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What decides whether a value validates against the subschemas a property's names spell, as a
 * tree to compare with another schema's: local {@code $ref}s inlined, annotations left out. Two
 * equal shapes validate every value alike, so read it as the same union branches.
 */
final class JsonValidationShape {

  private static final Set<String> ANNOTATIONS = new HashSet<>(Arrays.asList(
      "title", "description", "$comment", "examples", "deprecated", "readOnly", "writeOnly"));
  // Only reached through a $ref, which is inlined where it is used.
  private static final Set<String> DEFINITIONS = new HashSet<>(Arrays.asList(
      "definitions", "$defs"));
  // The document's own: its draft is compared apart, and a local $ref resolves in it whatever
  // its id.
  private static final Set<String> ROOT_ONLY = new HashSet<>(Arrays.asList("$id", "$schema"));
  // Keywords changing how a $ref resolves, or resolving one this does not follow.
  private static final Set<String> UNFOLLOWED = new HashSet<>(Arrays.asList(
      "$id", "$anchor", "$dynamicAnchor", "$dynamicRef", "$recursiveAnchor", "$recursiveRef"));
  // Where subschemas sit: under each key of the first, as the value of the second, in the arrays
  // of the third.
  private static final Set<String> SCHEMA_MAPS = new HashSet<>(Arrays.asList(
      "properties", "patternProperties", "dependentSchemas", "dependencies"));
  private static final Set<String> SCHEMAS = new HashSet<>(Arrays.asList(
      "items", "additionalProperties", "not", "if", "then", "else", "contains", "propertyNames",
      "additionalItems", "unevaluatedItems", "unevaluatedProperties"));
  private static final Set<String> SCHEMA_ARRAYS = new HashSet<>(Arrays.asList(
      "allOf", "anyOf", "oneOf", "prefixItems", "items"));
  // The schema as written, each definition counted once; past this, the comparison is not worth
  // it.
  private static final int MAX_NODES = 10_000;

  private static final class Unfollowed extends RuntimeException {
    Unfollowed() {
      super(null, null, false, false);
    }
  }

  private final JsonNode root;
  private final Set<String> inlining = new HashSet<>();
  // Each definition's shape, made once and shared wherever it is used: the cap then counts the
  // schema as written, not as inlined, which grows with every use of a shared definition.
  private final Map<String, JsonNode> definitions = new HashMap<>();
  private int nodes;

  private JsonValidationShape(JsonNode root) {
    this.root = root;
  }

  /**
   * The shape of every subschema of {@code root} that {@code names} spell, in the order the walk
   * finds them, and the draft; null where it cannot be told, as for an external or recursive
   * {@code $ref}.
   */
  static JsonNode of(JsonNode root, List<String> names) {
    try {
      JsonValidationShape shape = new JsonValidationShape(root);
      ArrayNode found = JsonNodeFactory.instance.arrayNode();
      found.add(root.path("$schema"));
      shape.collect(root, names, 0, found);
      return found.size() > 1 ? found : null;
    } catch (Unfollowed e) {
      return null;
    }
  }

  // Through $ref, allOf, anyOf and oneOf, as the pruner walks, and an unnamed step through items
  // and additionalProperties.
  private void collect(JsonNode schema, List<String> names, int step, ArrayNode found) {
    count();
    if (step == names.size()) {
      found.add(shape(schema));
      return;
    }
    if (!schema.isObject()) {
      return;
    }
    JsonNode ref = schema.get("$ref");
    if (ref != null) {
      String target = ref.asText();
      enter(target);
      collect(resolve(target), names, step, found);
      inlining.remove(target);
    }
    for (String combinator : new String[] {"allOf", "anyOf", "oneOf"}) {
      for (JsonNode subschema : schema.path(combinator)) {
        collect(subschema, names, step, found);
      }
    }
    String name = names.get(step);
    if (name != null) {
      JsonNode property = schema.path("properties").get(name);
      if (property != null) {
        collect(property, names, step + 1, found);
      }
      return;
    }
    for (String unnamed : new String[] {"items", "additionalProperties"}) {
      JsonNode subschema = schema.get(unnamed);
      if (subschema != null && subschema.isObject()) {
        collect(subschema, names, step + 1, found);
      }
    }
  }

  private JsonNode shape(JsonNode schema) {
    count();
    if (!schema.isObject()) {
      // true, false
      return schema;
    }
    ObjectNode shaped = JsonNodeFactory.instance.objectNode();
    Iterator<Map.Entry<String, JsonNode>> fields = schema.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String key = field.getKey();
      JsonNode value = field.getValue();
      if (ANNOTATIONS.contains(key) || DEFINITIONS.contains(key)
          || schema == root && ROOT_ONLY.contains(key)) {
        continue;
      }
      if (UNFOLLOWED.contains(key)) {
        throw new Unfollowed();
      }
      if (key.equals("$ref")) {
        shaped.set(key, definition(value.asText()));
      } else if (SCHEMA_MAPS.contains(key) && value.isObject()) {
        ObjectNode map = JsonNodeFactory.instance.objectNode();
        value.fields().forEachRemaining(e ->
            map.set(e.getKey(), e.getValue().isObject() ? shape(e.getValue()) : e.getValue()));
        shaped.set(key, map);
      } else if (SCHEMAS.contains(key) && value.isObject()) {
        shaped.set(key, shape(value));
      } else if (SCHEMA_ARRAYS.contains(key) && value.isArray()) {
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        value.forEach(subschema -> array.add(shape(subschema)));
        shaped.set(key, array);
      } else {
        shaped.set(key, value);
      }
    }
    return shaped;
  }

  private JsonNode definition(String target) {
    JsonNode known = definitions.get(target);
    if (known == null) {
      enter(target);
      known = shape(resolve(target));
      inlining.remove(target);
      definitions.put(target, known);
    }
    return known;
  }

  // A reference into this document; one recursing into itself is not inlined.
  private void enter(String target) {
    if (!target.startsWith("#") || !inlining.add(target)) {
      throw new Unfollowed();
    }
  }

  private JsonNode resolve(String target) {
    try {
      JsonNode resolved = root.at(target.substring(1));
      if (resolved.isMissingNode()) {
        throw new Unfollowed();
      }
      return resolved;
    } catch (IllegalArgumentException e) {
      // An anchor, not a pointer.
      throw new Unfollowed();
    }
  }

  private void count() {
    if (++nodes > MAX_NODES) {
      throw new Unfollowed();
    }
  }
}
