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

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig;
import com.kjetland.jackson.jsonSchema.JsonSchemaDraft;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.annotations.Schema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import org.apache.commons.compress.utils.Lists;

public class JsonSchemaUtils {

  private static final ObjectMapper jsonMapper = Jackson.newObjectMapper();

  static final String ENVELOPE_SCHEMA_FIELD_NAME = "schema";
  static final String ENVELOPE_REFERENCES_FIELD_NAME = "references";
  static final String ENVELOPE_RESOLVED_REFS_FIELD_NAME = "resolvedReferences";
  static final String ENVELOPE_PAYLOAD_FIELD_NAME = "payload";

  public static ObjectNode envelope(JsonSchema schema, JsonNode payload) {
    Map<String, String> resolvedReferences = schema.resolvedReferences();
    Map<String, JsonNode> resolved = Collections.emptyMap();
    if (resolvedReferences != null && !resolvedReferences.isEmpty()) {
      resolved = resolvedReferences.entrySet().stream()
          .collect(Collectors.toMap(Entry::getKey, e -> {
            try {
              return jsonMapper.readTree(e.getValue());
            } catch (IOException ioe) {
              throw new IllegalArgumentException(ioe);
            }
          }));
    }
    return envelope(schema.toJsonNode(), schema.references(), resolved, payload);
  }

  public static ObjectNode envelope(JsonNode schema, JsonNode payload) {
    return envelope(schema, Collections.emptyList(), Collections.emptyMap(), payload);
  }

  public static ObjectNode envelope(JsonNode schema,
                                    List<SchemaReference> references,
                                    Map<String, JsonNode> resolvedReferences,
                                    JsonNode payload) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    result.set(ENVELOPE_SCHEMA_FIELD_NAME, schema);
    result.set(ENVELOPE_PAYLOAD_FIELD_NAME, payload);
    if (references != null && !references.isEmpty()) {
      result.set(ENVELOPE_REFERENCES_FIELD_NAME, jsonMapper.valueToTree(references));
    }
    if (resolvedReferences != null && !resolvedReferences.isEmpty()) {
      result.set(ENVELOPE_RESOLVED_REFS_FIELD_NAME, jsonMapper.valueToTree(resolvedReferences));
    }
    return result;
  }

  public static boolean isEnvelope(Object object) {
    if (object instanceof JsonNode) {
      JsonNode jsonValue = (JsonNode) object;
      return jsonValue.isObject() && jsonValue.has(ENVELOPE_SCHEMA_FIELD_NAME);
    }
    return false;
  }

  public static ObjectNode copyEnvelopeWithoutPayload(ObjectNode jsonValue) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    JsonNode schemaNode = jsonValue.get(ENVELOPE_SCHEMA_FIELD_NAME);
    result.set(ENVELOPE_SCHEMA_FIELD_NAME, schemaNode);
    JsonNode referencesNode = jsonValue.get(ENVELOPE_REFERENCES_FIELD_NAME);
    if (referencesNode != null && !referencesNode.isEmpty()) {
      result.set(ENVELOPE_REFERENCES_FIELD_NAME, referencesNode);
    }
    JsonNode resolvedReferencesNode = jsonValue.get(ENVELOPE_RESOLVED_REFS_FIELD_NAME);
    if (resolvedReferencesNode != null && !resolvedReferencesNode.isEmpty()) {
      result.set(ENVELOPE_RESOLVED_REFS_FIELD_NAME, resolvedReferencesNode);
    }
    return result;
  }

  public static JsonSchema copyOf(JsonSchema schema) {
    return schema.copy();
  }

  public static JsonSchema getSchema(Object object) throws IOException {
    return getSchema(object, null);
  }

  public static JsonSchema getSchema(Object object, SchemaRegistryClient client)
      throws IOException {
    return getSchema(object, null, true, client);
  }

  public static JsonSchema getSchema(
      Object object,
      SpecificationVersion specVersion,
      boolean useOneofForNullables,
      SchemaRegistryClient client) throws IOException {
    return getSchema(object, specVersion, useOneofForNullables, jsonMapper, client);
  }

  public static JsonSchema getSchema(
      Object object,
      SpecificationVersion specVersion,
      boolean useOneofForNullables,
      boolean failUnknownProperties,
      SchemaRegistryClient client) throws IOException {
    return getSchema(object, specVersion, useOneofForNullables,
        failUnknownProperties, jsonMapper, client);
  }

  public static JsonSchema getSchema(
      Object object,
      SpecificationVersion specVersion,
      boolean useOneofForNullables,
      ObjectMapper objectMapper,
      SchemaRegistryClient client) throws IOException {
    return getSchema(object, specVersion, useOneofForNullables, true, objectMapper, client);
  }

  public static JsonSchema getSchema(
      Object object,
      SpecificationVersion specVersion,
      boolean useOneofForNullables,
      boolean failUnknownProperties,
      ObjectMapper objectMapper,
      SchemaRegistryClient client) throws IOException {
    if (object == null) {
      return null;
    }
    if (specVersion == null) {
      specVersion = SpecificationVersion.DRAFT_7;
    }
    if (isEnvelope(object)) {
      return getSchemaFromEnvelope((JsonNode) object);
    }
    Class<?> cls = object.getClass();
    if (cls.isAnnotationPresent(Schema.class)) {
      Schema schema = (Schema) cls.getAnnotation(Schema.class);
      List<SchemaReference> references = Arrays.stream(schema.refs())
              .map(ref -> new SchemaReference(ref.name(), ref.subject(), ref.version()))
              .collect(Collectors.toList());
      if (client == null) {
        if (!references.isEmpty()) {
          throw new IllegalArgumentException("Cannot resolve schema " + schema.value()
                  + " with refs " + references);
        }
        return new JsonSchema(schema.value());
      } else {
        return (JsonSchema) client.parseSchema(JsonSchema.TYPE, schema.value(), references)
                .orElseThrow(() -> new IOException("Invalid schema " + schema.value()
                        + " with refs " + references));
      }
    }
    JsonSchemaConfig config = getConfig(useOneofForNullables, failUnknownProperties);
    JsonSchemaDraft draft;
    switch (specVersion) {
      case DRAFT_4:
        draft = JsonSchemaDraft.DRAFT_04;
        break;
      case DRAFT_6:
        draft = JsonSchemaDraft.DRAFT_06;
        break;
      case DRAFT_7:
        draft = JsonSchemaDraft.DRAFT_07;
        break;
      case DRAFT_2019_09:
        draft = JsonSchemaDraft.DRAFT_2019_09;
        break;
      default:
        draft = JsonSchemaDraft.DRAFT_07;
        break;
    }
    config = config.withJsonSchemaDraft(draft);
    JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(objectMapper, config);
    JsonNode jsonSchema = jsonSchemaGenerator.generateJsonSchema(cls);
    return new JsonSchema(jsonSchema);
  }

  private static JsonSchema getSchemaFromEnvelope(JsonNode jsonValue) {
    JsonNode referencesNode = jsonValue.get(ENVELOPE_REFERENCES_FIELD_NAME);
    List<SchemaReference> references = Collections.emptyList();
    if (referencesNode != null && !referencesNode.isEmpty()) {
      JavaType type = jsonMapper.getTypeFactory().constructParametricType(
          List.class, SchemaReference.class);
      references = jsonMapper.convertValue(referencesNode, type);
    }
    JsonNode resolvedReferencesNode = jsonValue.get(ENVELOPE_RESOLVED_REFS_FIELD_NAME);
    Map<String, String> resolvedReferences = Collections.emptyMap();
    if (resolvedReferencesNode != null && !resolvedReferencesNode.isEmpty()) {
      JavaType type = jsonMapper.getTypeFactory().constructParametricType(
          Map.class, String.class, JsonNode.class);
      Map<String, JsonNode> resolved = jsonMapper.convertValue(resolvedReferencesNode, type);
      resolvedReferences = resolved.entrySet().stream()
          .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().toString()));
    }
    JsonNode schemaNode = jsonValue.get(ENVELOPE_SCHEMA_FIELD_NAME);
    return new JsonSchema(schemaNode, references, resolvedReferences, null);
  }

  private static JsonSchemaConfig getConfig(
      boolean useOneofForNullables, boolean failUnknownProperties) {
    final JsonSchemaConfig vanilla = JsonSchemaConfig.vanillaJsonSchemaDraft4();
    return JsonSchemaConfig.create(
        vanilla.autoGenerateTitleForProperties(),
        Optional.empty(),
        true,
        useOneofForNullables,
        vanilla.usePropertyOrdering(),
        vanilla.hidePolymorphismTypeProperty(),
        vanilla.disableWarnings(),
        vanilla.useMinLengthForNotNull(),
        vanilla.useTypeIdForDefinitionName(),
        Collections.emptyMap(),
        vanilla.useMultipleEditorSelectViaProperty(),
        Collections.emptySet(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        vanilla.subclassesResolver(),
        failUnknownProperties,
        null
    );
  }

  public static Object getValue(Object object) {
    if (object == null) {
      return null;
    }
    if (isEnvelope(object)) {
      JsonNode jsonValue = (JsonNode) object;
      return jsonValue.get(ENVELOPE_PAYLOAD_FIELD_NAME);
    }
    return object;
  }

  public static Object toObject(JsonNode value, JsonSchema schema) throws IOException {
    return toObject(value, schema, true);
  }

  public static Object toObject(JsonNode value, JsonSchema schema, boolean validate)
      throws IOException {
    if (validate) {
      value = schema.validate(value);
    }
    return envelope(schema, value);
  }

  public static Object toObject(String value, JsonSchema schema) throws IOException {
    return toObject(value, schema, true);
  }

  public static Object toObject(String value, JsonSchema schema, boolean validate)
      throws IOException {
    return toObject(value != null ? jsonMapper.readTree(value) : null, schema, validate);
  }

  public static byte[] toJson(Object value) throws IOException {
    if (value == null) {
      return null;
    }
    StringWriter out = new StringWriter();
    jsonMapper.writeValue(out, value);
    String jsonString = out.toString();
    return jsonString.getBytes(StandardCharsets.UTF_8);
  }

  public static JsonNode findMatchingEntity(JsonNode node, SchemaEntity entity) {
    String[] identifiers = entity.getEntityPath().split("\\.");
    LinkedList<String> identifiersList = new LinkedList<>();
    Collections.addAll(identifiersList, identifiers);
    JsonNode entityNode = findNodeHelper(node, entity.getEntityType(), identifiersList);
    if (entityNode == null) {
      throw new IllegalArgumentException(String.format(
        "No matching path '%s' found in the schema", entity.getEntityPath()));
    }
    return entityNode;
  }

  public static JsonNode findNodeFromNameBuilder(JsonNode node,
                                                 LinkedList<String> identifiersList) {
    if (identifiersList.isEmpty() || node == null) {
      return null;
    }
    String word = identifiersList.poll();
    StringBuilder fieldNameBuilder = new StringBuilder(word);
    while (!node.has(fieldNameBuilder.toString()) && !identifiersList.isEmpty()) {
      fieldNameBuilder.append(".");
      word = identifiersList.poll();
      if (word == null) {
        return null;
      }
      fieldNameBuilder.append(word);
    }
    return node.get(fieldNameBuilder.toString());
  }

  private static JsonNode findNodeHelper(JsonNode node,
                                         SchemaEntity.EntityType entityType,
                                         LinkedList<String> identifiersList) {
    String type = identifiersList.poll();
    if (type == null) {
      return null;
    }
    switch (type) {
      case "object":
        if (SchemaEntity.EntityType.SR_RECORD == entityType && identifiersList.isEmpty()) {
          return node;
        }

        String word = identifiersList.peek();
        if (word == null) {
          return null;
        }
        JsonNode result;
        if ("definitions".equals(word)) {
          identifiersList.poll();
          result = findNodeFromNameBuilder(node.get("definitions"), identifiersList);
        } else if ("$defs".equals(word)) {
          identifiersList.poll();
          result = findNodeFromNameBuilder(node.get("$defs"), identifiersList);
        } else {
          result = findNodeFromNameBuilder(node.get("properties"), identifiersList);
        }
        if (result == null || identifiersList.isEmpty()) {
          return result;
        } else {
          return findNodeHelper(result, entityType, identifiersList);
        }
      case "array":
        return findNodeHelper(node.get("items"), entityType, identifiersList);
      case "anyof":
        if (node.has("anyOf")) {
          return findSubSchemasFromIndex(node, entityType, "anyOf", identifiersList);
        }

        // handle "type": ["null", "object"] etc
        identifiersList.poll();
        return findNodeHelper(node, entityType, identifiersList);
      case "allof":
        if (node.has("allOf")) {
          return findSubSchemasFromIndex(node, entityType, "allOf", identifiersList);
        }

        // handle if-then-else
        identifiersList.poll();
        if ("conditional".equals(identifiersList.peek())) {
          identifiersList.poll();
          return findNodeHelper(node.get(identifiersList.poll()), entityType, identifiersList);
        } else {
          return findNodeHelper(node, entityType, identifiersList);
        }
      case "oneof":
        return findSubSchemasFromIndex(node, entityType, "oneOf", identifiersList);
      case "not":
        return findNodeHelper(node.get(type), entityType, identifiersList);
      default:
        return null;
    }
  }

  private static JsonNode findSubSchemasFromIndex(JsonNode node,
                                                  SchemaEntity.EntityType entityType,
                                                  String compositionType,
                                                  LinkedList<String> identifiersList) {
    String index = identifiersList.poll();
    if (index == null) {
      return null;
    }
    List<JsonNode> jsonNodeList = Lists.newArrayList(node.get(compositionType).elements());
    jsonNodeList.sort(new JsonNodeComparator());
    return findNodeHelper(jsonNodeList.get(Integer.parseInt(index)), entityType, identifiersList);
  }
}
