/*
 * Copyright 2020 Confluent Inc.
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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.annotations.VisibleForTesting;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.diff.Difference;
import io.confluent.kafka.schemaregistry.json.diff.SchemaDiff;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;

public class JsonSchema implements ParsedSchema {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemaProvider.class);

  public static final String TYPE = "JSON";

  private static final Object NONE_MARKER = new Object();

  private final JsonNode jsonNode;

  private transient Schema schemaObj;

  private final Integer version;

  private final List<SchemaReference> references;

  private final Map<String, String> resolvedReferences;

  private transient String canonicalString;

  private static final ObjectMapper objectMapper = Jackson.newObjectMapper();

  @VisibleForTesting
  public JsonSchema(JsonNode jsonNode) throws JsonProcessingException {
    this(jsonNode, Collections.emptyList(), Collections.emptyMap(), null);
  }

  public JsonSchema(String schemaString) {
    this(schemaString, Collections.emptyList(), Collections.emptyMap(), null);
  }

  public JsonSchema(
      JsonNode jsonNode,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Integer version
  ) {
    this.jsonNode = jsonNode;
    this.version = version;
    this.references = Collections.unmodifiableList(references);
    this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences);
  }

  public JsonSchema(
      String schemaString,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Integer version
  ) {
    try {
      this.jsonNode = objectMapper.readTree(schemaString);
      this.version = version;
      this.references = Collections.unmodifiableList(references);
      this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON " + schemaString, e);
    }
  }

  public JsonSchema(Schema schemaObj) {
    this(schemaObj, null);
  }

  public JsonSchema(Schema schemaObj, Integer version) {
    try {
      this.jsonNode = schemaObj != null ? objectMapper.readTree(schemaObj.toString()) : null;
      this.schemaObj = schemaObj;
      this.version = version;
      this.references = Collections.emptyList();
      this.resolvedReferences = Collections.emptyMap();
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON " + schemaObj.toString(), e);
    }
  }

  private JsonSchema(
      JsonNode jsonNode,
      Schema schemaObj,
      Integer version,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      String canonicalString
  ) {
    this.jsonNode = jsonNode;
    this.schemaObj = schemaObj;
    this.version = version;
    this.references = references;
    this.resolvedReferences = resolvedReferences;
    this.canonicalString = canonicalString;
  }

  public JsonSchema copy() {
    return new JsonSchema(
        this.jsonNode,
        this.schemaObj,
        this.version,
        this.references,
        this.resolvedReferences,
        this.canonicalString
    );
  }

  public JsonSchema copy(Integer version) {
    return new JsonSchema(
        this.jsonNode,
        this.schemaObj,
        version,
        this.references,
        this.resolvedReferences,
        this.canonicalString
    );
  }

  @Override
  public Schema rawSchema() {
    if (jsonNode == null) {
      return null;
    }
    if (schemaObj == null) {
      try {
        SchemaLoader.SchemaLoaderBuilder builder = SchemaLoader.builder();
        for (Map.Entry<String, String> dep : resolvedReferences.entrySet()) {
          builder.registerSchemaByURI(new URI(dep.getKey()), new JSONObject(dep.getValue()));
        }
        JSONObject jsonObject = objectMapper.treeToValue((jsonNode), JSONObject.class);
        builder.schemaJson(jsonObject);
        SchemaLoader loader = builder.build();
        schemaObj = loader.load().build();
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Invalid dependency name", e);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid JSON", e);
      }
    }
    return schemaObj;
  }

  @Override
  public String schemaType() {
    return TYPE;
  }

  @Override
  public String name() {
    return getString("title");
  }

  public String getString(String key) {
    return jsonNode.has(key) ? jsonNode.get(key).asText() : null;
  }

  @Override
  public String canonicalString() {
    if (canonicalString == null) {
      try {
        canonicalString = objectMapper.writeValueAsString(jsonNode);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid JSON", e);
      }
    }
    return canonicalString;
  }

  public Integer version() {
    return version;
  }

  @Override
  public List<SchemaReference> references() {
    return references;
  }

  public Map<String, String> resolvedReferences() {
    return resolvedReferences;
  }

  @Override
  public void validate() {
    // Access the raw schema since it is computed lazily
    rawSchema();
  }

  public void validate(Object value) throws JsonProcessingException, ValidationException {
    Object primitiveValue = NONE_MARKER;
    if (isPrimitive(value)) {
      primitiveValue = value;
    } else if (value instanceof BinaryNode) {
      primitiveValue = ((BinaryNode) value).asText();
    } else if (value instanceof BooleanNode) {
      primitiveValue = ((BooleanNode) value).asBoolean();
    } else if (value instanceof NullNode) {
      primitiveValue = null;
    } else if (value instanceof NumericNode) {
      primitiveValue = ((NumericNode) value).numberValue();
    } else if (value instanceof TextNode) {
      primitiveValue = ((TextNode) value).asText();
    }
    if (primitiveValue != NONE_MARKER) {
      rawSchema().validate(primitiveValue);
    } else {
      Object jsonObject;
      if (value instanceof ArrayNode) {
        jsonObject = objectMapper.treeToValue(((ArrayNode) value), JSONArray.class);
      } else if (value instanceof JsonNode) {
        jsonObject = objectMapper.treeToValue(((JsonNode) value), JSONObject.class);
      } else if (value.getClass().isArray()) {
        jsonObject = objectMapper.convertValue(value, JSONArray.class);
      } else {
        jsonObject = objectMapper.convertValue(value, JSONObject.class);
      }
      rawSchema().validate(jsonObject);
    }
  }

  private static boolean isPrimitive(Object value) {
    return value == null
        || value instanceof Boolean
        || value instanceof Number
        || value instanceof String;
  }

  @Override
  public boolean isBackwardCompatible(ParsedSchema previousSchema) {
    if (!schemaType().equals(previousSchema.schemaType())) {
      return false;
    }
    final List<Difference> differences = SchemaDiff.compare(
        ((JsonSchema) previousSchema).rawSchema(),
        rawSchema()
    );
    final List<Difference> incompatibleDiffs = differences.stream()
        .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES.contains(diff.getType()))
        .collect(Collectors.toList());
    boolean isCompatible = incompatibleDiffs.isEmpty();
    if (!isCompatible) {
      boolean first = true;
      for (Difference incompatibleDiff : incompatibleDiffs) {
        if (first) {
          // Log first incompatible change as warning
          log.warn("Found incompatible change: {}", incompatibleDiff);
          first = false;
        } else {
          log.debug("Found incompatible change: {}", incompatibleDiff);
        }
      }
    }
    return isCompatible;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JsonSchema that = (JsonSchema) o;
    return Objects.equals(jsonNode, that.jsonNode)
        && Objects.equals(references, that.references)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jsonNode, references, version);
  }

  @Override
  public String toString() {
    return canonicalString();
  }
}
