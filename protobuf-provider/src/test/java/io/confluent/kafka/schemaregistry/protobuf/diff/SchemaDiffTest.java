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
 *
 */

package io.confluent.kafka.schemaregistry.protobuf.diff;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class SchemaDiffTest {

  private static final String badMessageSchemaString = "syntax = \"proto3\";\n"
      + "\n"
      + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
      + "option java_outer_classname = \"TestMapProtos\";\n"
      + "\n"
      + "import \"google/protobuf/descriptor.proto\";\n"
      + "\n"
      + "message TestBadMessage {\n"
      + "    bad.Message test_bad_message = 1;\n"
      + "}\n";

  private static final ProtobufSchema badMessageSchema = new ProtobufSchema(badMessageSchemaString);

  @Test
  public void checkProtobufSchemaCompatibility() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    ArrayNode nodes = (ArrayNode) objectMapper.readTree(readFile("diff-schema-examples.json"));
    checkProtobufSchemaCompatibility(nodes);
  }

  @Test
  public void checkProtobufSchemaCompatibilityForMeta() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    ArrayNode nodes =
            (ArrayNode) objectMapper.readTree(readFile("diff-schema-examples-with-meta.json"));
    checkProtobufSchemaCompatibility(nodes);
  }

  private void checkProtobufSchemaCompatibility(ArrayNode nodes) {
    for (int i = 0; i < nodes.size(); i++) {
      final ObjectNode testCase = (ObjectNode) nodes.get(i);
      String originalSchema = testCase.get("original_schema").asText();
      String updateSchema = testCase.get("update_schema").asText();
      ProtoFileElement original = getSchema(originalSchema);
      ProtoFileElement update = getSchema(updateSchema);
      List<SchemaReference> originalSchemaRefs = new ArrayList<>();
      ArrayNode originalRefs = (ArrayNode) testCase.get("original_references");
      if (originalRefs != null) {
        for (JsonNode ref : originalRefs) {
          ObjectNode node = (ObjectNode) ref;
          SchemaReference schemaRef = new SchemaReference(
              node.get("name").asText(), node.get("subject").asText(), node.get("version").asInt());
          originalSchemaRefs.add(schemaRef);
        }
      }
      List<SchemaReference> updateSchemaRefs = new ArrayList<>();
      ArrayNode updateRefs = (ArrayNode) testCase.get("update_references");
      if (updateRefs != null) {
        for (JsonNode ref : updateRefs) {
          ObjectNode node = (ObjectNode) ref;
          SchemaReference schemaRef = new SchemaReference(
              node.get("name").asText(), node.get("subject").asText(), node.get("version").asInt());
          updateSchemaRefs.add(schemaRef);
        }
      }
      Map<String, ProtoFileElement> originalDependencies = new HashMap<>();
      ArrayNode originalDeps = (ArrayNode) testCase.get("original_dependencies");
      if (originalDeps != null) {
        for (JsonNode dep : originalDeps) {
          ObjectNode node = (ObjectNode) dep;
          ProtoFileElement file = getDependency(node);
          originalDependencies.put(node.get("name").asText(), file);
        }
      }
      Map<String, ProtoFileElement> updateDependencies = new HashMap<>();
      ArrayNode updateDeps = (ArrayNode) testCase.get("update_dependencies");
      if (updateDeps != null) {
        for (JsonNode dep : updateDeps) {
          ObjectNode node = (ObjectNode) dep;
          ProtoFileElement file = getDependency(node);
          updateDependencies.put(node.get("name").asText(), file);
        }
      }
      final ArrayNode changes = (ArrayNode) testCase.get("changes");
      boolean isCompatible = testCase.get("compatible").asBoolean();
      final List<String> errorMessages = new ArrayList<>();
      for (int j = 0; j < changes.size(); j++) {
        errorMessages.add(changes.get(j).asText());
      }
      final String description = testCase.get("description").asText();

      ProtobufSchema o = new ProtobufSchema(original, originalSchemaRefs, originalDependencies);
      o = o.copy(getMetadata(testCase, "original"), null);
      ProtobufSchema u = new ProtobufSchema(update, updateSchemaRefs, updateDependencies);
      u = u.copy(getMetadata(testCase, "update"), null);
      List<Difference> differences = SchemaDiff.compare(o, u);
      final List<Difference> incompatibleDiffs = differences.stream()
          .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES.contains(diff.getType()))
          .collect(Collectors.toList());
      assertThat(
          description,
          differences.stream()
              .map(change -> change.getType().toString() + " " + change.getFullPath())
              .collect(toList()),
          is(errorMessages)
      );
      assertEquals(description, isCompatible, incompatibleDiffs.isEmpty());
    }
  }

  private Metadata getMetadata(ObjectNode testCase, String version) {
    ObjectMapper mapper = new ObjectMapper();
    switch (version) {
      case "original":
        if (testCase.has("original_metadata")) {
          return mapper.convertValue(testCase.get("original_metadata"), Metadata.class);
        }
        break;
      case "update":
        if (testCase.has("update_metadata")) {
          return mapper.convertValue(testCase.get("update_metadata"), Metadata.class);
        }
        break;
      default:
        return null;
    }
    return null;
  }

  private ProtoFileElement getSchema(String schema) {
    if (schema.endsWith(".proto")) {
      schema = readFile(schema);
    }
    return ProtoParser.Companion.parse(
            Location.get("unknown"), schema);
  }

  private ProtoFileElement getDependency(ObjectNode node) {
    String name = node.get("name").asText();
    JsonNode dep = node.get("dependency");
    String schema = dep != null ? dep.asText() : readFile(name);
    return ProtoParser.Companion.parse(
            Location.get("unknown"), schema);
  }

  @Test
  public void checkCompatibilityUsingProtoFiles() throws Exception {

    ResourceLoader resourceLoader = new ResourceLoader(
        "/io/confluent/kafka/schemaregistry/protobuf/diff/");
    ProtoFileElement original = resourceLoader.readObj("TestProto.proto");
    ProtoFileElement update = resourceLoader.readObj("TestProto2.proto");

    List<Difference> changes = SchemaDiff.compare(
        new ProtobufSchema(original, Collections.emptyList(), Collections.emptyMap()),
        new ProtobufSchema(update, Collections.emptyList(), Collections.emptyMap())
    );
    assertTrue(changes.contains(new Difference(
        Difference.Type.FIELD_NAME_CHANGED,
        "#/TestMessage/2"
    )));
    assertTrue(changes.contains(new Difference(
        Difference.Type.FIELD_SCALAR_KIND_CHANGED,
        "#/TestMessage/2"
    )));
  }

  @Test
  public void checkCompatibilityUsingBadMessage() throws Exception {
    SchemaDiff.compare(badMessageSchema, badMessageSchema);
  }

  private static String readFile(String fileName) {
    ResourceLoader resourceLoader = new ResourceLoader("/");
    return resourceLoader.toString(fileName);
  }
}