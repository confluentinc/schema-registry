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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.SchemaException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Consumer;

public class JsonNodeComparator implements Comparator<JsonNode> {

  private static final JsonSchemaComparator jsonSchemaComparator = new JsonSchemaComparator();
  private static final ObjectMapper objectMapper = Jackson.newObjectMapper();

  private final Consumer<SchemaLoader.SchemaLoaderBuilder> loaderConfig;
  private final JsonNode root;
  private final Map<JsonNode, Schema> loaded = new IdentityHashMap<>();
  private Map<JsonNode, String> refs;
  private JSONObject rootJson;

  public JsonNodeComparator() {
    this(null);
  }

  // Configures the loader for the compared schemas; null means the loader default
  public JsonNodeComparator(Consumer<SchemaLoader.SchemaLoaderBuilder> loaderConfig) {
    this(loaderConfig, null);
  }

  JsonNodeComparator(Consumer<SchemaLoader.SchemaLoaderBuilder> loaderConfig, JsonNode root) {
    this.loaderConfig = loaderConfig;
    this.root = root;
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
    Schema schema = loaded.get(node);
    if (schema == null) {
      schema = loadUncached(node);
      loaded.put(node, schema);
    }
    return schema;
  }

  private Schema loadUncached(JsonNode node) throws JsonProcessingException {
    SchemaLoader.SchemaLoaderBuilder builder = SchemaLoader.builder()
        .schemaJson(objectMapper.readValue(node.toString(), JSONObject.class));
    try {
      if (loaderConfig != null) {
        loaderConfig.accept(builder);
      }
      return builder.build().load().build();
    } catch (RuntimeException e) {
      // A branch that does not load on its own may need the root document and its scope
      String ref = root != null && loaderConfig != null
          && !JsonSchema.isRemoteRefBlocked(e) && isLoadFailure(e) ? refs().get(node) : null;
      if (ref == null) {
        throw loadFailure(e);
      }
      try {
        return loadInRoot(ref);
      } catch (RuntimeException inRoot) {
        inRoot.addSuppressed(e);
        throw loadFailure(inRoot);
      }
    }
  }

  private Schema loadInRoot(String ref) throws JsonProcessingException {
    if (rootJson == null) {
      rootJson = objectMapper.readValue(root.toString(), JSONObject.class);
    }
    JSONObject wrapper = new JSONObject(rootJson, JSONObject.getNames(rootJson));
    wrapper.put("$ref", ref);
    SchemaLoader.SchemaLoaderBuilder builder = SchemaLoader.builder()
        .draftV7Support().schemaJson(wrapper);
    loaderConfig.accept(builder);
    return ((ReferenceSchema) builder.build().load().build()).getReferredSchema();
  }

  private static boolean isLoadFailure(RuntimeException e) {
    return e instanceof SchemaException || e instanceof UncheckedIOException;
  }

  private static RuntimeException loadFailure(RuntimeException e) {
    if (JsonSchema.isRemoteRefBlocked(e)) {
      return new IllegalArgumentException(JsonSchema.REMOTE_REF_DISABLED_MESSAGE, e);
    }
    if (isLoadFailure(e)) {
      return new IllegalArgumentException("Cannot load Json Schema from JsonNode", e);
    }
    return e;
  }

  private Map<JsonNode, String> refs() {
    if (refs == null) {
      refs = new IdentityHashMap<>();
      indexRefs(root, "#");
    }
    return refs;
  }

  private void indexRefs(JsonNode node, String ref) {
    if (!node.isContainerNode()) {
      return;
    }
    refs.put(node, ref);
    if (node.isArray()) {
      for (int i = 0; i < node.size(); i++) {
        indexRefs(node.get(i), ref + "/" + i);
      }
    } else {
      node.fields().forEachRemaining(
          e -> indexRefs(e.getValue(), ref + "/" + encodeToken(e.getKey())));
    }
  }

  private static String encodeToken(String token) {
    try {
      return URLEncoder.encode(token.replace("~", "~0").replace("/", "~1"), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }
}
