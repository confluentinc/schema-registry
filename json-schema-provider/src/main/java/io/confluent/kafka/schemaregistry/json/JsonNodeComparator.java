/*
 * Copyright 2023 Confluent Inc.
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

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.TrueSchema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class JsonNodeComparator implements Comparator<JsonNode> {

  private static final JsonSchemaComparator jsonSchemaComparator = new JsonSchemaComparator();
  private static final ObjectMapper objectMapper = Jackson.newObjectMapper();

  private final Consumer<SchemaLoader.SchemaLoaderBuilder> loaderConfig;
  private final Supplier<Map<JsonNode, Schema>> branches;
  private Map<JsonNode, Schema> loaded;

  public JsonNodeComparator() {
    this(null);
  }

  // Configures the loader for the compared schemas; null means the loader default
  public JsonNodeComparator(Consumer<SchemaLoader.SchemaLoaderBuilder> loaderConfig) {
    this.loaderConfig = loaderConfig;
    this.branches = null;
  }

  // Caches loaded branches for its lifetime, seeded on the first compare so that paths
  // that never sort a union do not load anything
  JsonNodeComparator(Consumer<SchemaLoader.SchemaLoaderBuilder> loaderConfig,
                     Supplier<Map<JsonNode, Schema>> branches) {
    this.loaderConfig = loaderConfig;
    this.branches = branches;
  }

  @Override
  public int compare(JsonNode o1, JsonNode o2) {
    if (o1 == o2) {
      return 0;
    }
    if (o1 == null) {
      if (o2 != null) {
        return -1;
      } else {
        return 0;
      }
    } else if (o2 == null) {
      return 1;
    }

    try {
      Schema schema1 = load(o1);
      Schema schema2 = load(o2);
      return jsonSchemaComparator.compare(schema1, schema2);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Cannot parse Json Schema from JsonNode", e);
    }
  }

  private Schema load(JsonNode node) throws JsonProcessingException {
    if (branches == null) {
      return loadStandalone(node);
    }
    // Boolean nodes are shared singletons, so resolve them as the loaded graph does everywhere
    if (node.isBoolean()) {
      return node.booleanValue() ? TrueSchema.builder().build() : FalseSchema.builder().build();
    }
    if (loaded == null) {
      loaded = new IdentityHashMap<>(branches.get());
    }
    Schema schema = loaded.get(node);
    if (schema == null) {
      schema = loadStandalone(node);
      loaded.put(node, schema);
    }
    return schema;
  }

  private Schema loadStandalone(JsonNode node) throws JsonProcessingException {
    SchemaLoader.SchemaLoaderBuilder builder = SchemaLoader.builder()
        .schemaJson(objectMapper.readValue(node.toString(), JSONObject.class));
    try {
      if (loaderConfig != null) {
        loaderConfig.accept(builder);
      }
      return builder.build().load().build();
    } catch (RuntimeException e) {
      if (JsonSchema.isRemoteRefBlocked(e)) {
        throw new IllegalArgumentException(JsonSchema.REMOTE_REF_DISABLED_MESSAGE, e);
      }
      if (e instanceof IllegalArgumentException) {
        throw e;
      }
      throw new IllegalArgumentException(
          "Cannot load Json Schema from JsonNode: " + e.getMessage(), e);
    }
  }

  // Maps union branches of root to their schemas in the loaded graph, the same schemas that
  // inlineTaggedEntities sorts; a union that cannot be matched exactly is left out
  static Map<JsonNode, Schema> branchesOf(JsonNode root, Schema schema) {
    Map<JsonNode, Schema> branches = new IdentityHashMap<>();
    Set<Schema> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Schema> pending = new ArrayDeque<>();
    pending.push(schema);
    while (!pending.isEmpty()) {
      Schema s = pending.pop();
      if (!seen.add(s)) {
        continue;
      }
      List<Schema> children = new ArrayList<>();
      if (s instanceof CombinedSchema) {
        mapUnion(root, (CombinedSchema) s, branches);
        children.addAll(((CombinedSchema) s).getSubschemas());
      } else if (s instanceof ObjectSchema) {
        children.addAll(((ObjectSchema) s).getPropertySchemas().values());
      } else if (s instanceof ArraySchema) {
        children.add(((ArraySchema) s).getAllItemSchema());
      } else if (s instanceof NotSchema) {
        children.add(((NotSchema) s).getMustNotMatch());
      } else if (s instanceof ConditionalSchema) {
        ConditionalSchema c = (ConditionalSchema) s;
        c.getIfSchema().ifPresent(children::add);
        c.getThenSchema().ifPresent(children::add);
        c.getElseSchema().ifPresent(children::add);
      }
      for (Schema child : children) {
        if (child != null) {
          pending.push(child);
        }
      }
    }
    return branches;
  }

  private static void mapUnion(JsonNode root, CombinedSchema union,
                               Map<JsonNode, Schema> branches) {
    String criterion = JsonSchemaComparator.getCriterion(union);
    if (criterion == null) {
      return;
    }
    String keyword = criterion.substring(0, 3) + "Of";
    // A union Everit wrapped with its sibling keywords has no location; its paths keep today's sort
    String base = union.getSchemaLocation();
    // Everit escapes \ and " in locations as in JSON strings, so those keys cannot be resolved
    if (base == null || !base.startsWith("#")
        || base.indexOf('\\') >= 0 || base.indexOf('"') >= 0) {
      return;
    }
    List<Schema> subschemas = new ArrayList<>(union.getSubschemas());
    JsonNode array = root.at(JsonPointer.compile(base.substring(1) + "/" + keyword));
    if (!array.isArray() || array.size() != subschemas.size()) {
      return;
    }
    for (int i = 0; i < subschemas.size(); i++) {
      if (!matches(subschemas.get(i), array.get(i), base + "/" + keyword + "/" + i)) {
        return;
      }
    }
    for (int i = 0; i < subschemas.size(); i++) {
      branches.put(array.get(i), subschemas.get(i));
    }
  }

  private static boolean matches(Schema schema, JsonNode node, String location) {
    if (node.isBoolean()) {
      return node.booleanValue() ? schema instanceof TrueSchema : schema instanceof FalseSchema;
    }
    if (schema instanceof ReferenceSchema) {
      JsonNode ref = node.get("$ref");
      return ref != null && sameRef(ref.asText(), ((ReferenceSchema) schema).getReferenceValue());
    }
    return location.equals(schema.getSchemaLocation());
  }

  // Everit may store a fragment-only ref resolved against the base URI
  private static boolean sameRef(String ref, String value) {
    if (ref.equals(value)) {
      return true;
    }
    return ref.startsWith("#") && value.endsWith(ref)
        && value.indexOf('#') == value.lastIndexOf('#');
  }
}
