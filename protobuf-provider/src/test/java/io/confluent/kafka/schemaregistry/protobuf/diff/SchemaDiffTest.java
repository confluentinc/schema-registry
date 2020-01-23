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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SchemaDiffTest {

  @Test
  public void checkProtobufSchemaCompatibility() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    ArrayNode nodes = (ArrayNode) objectMapper.readTree(readFile("diff-schema-examples.json"));

    for (int i = 0; i < nodes.size(); i++) {
      final ObjectNode testCase = (ObjectNode) nodes.get(i);
      String originalSchema = testCase.get("original_schema").asText();
      String updateSchema = testCase.get("update_schema").asText();
      ProtoFileElement original = ProtoParser.parse(Location.get("unknown"), originalSchema);
      ProtoFileElement update = ProtoParser.parse(Location.get("unknown"), updateSchema);
      final ArrayNode changes = (ArrayNode) testCase.get("changes");
      boolean isCompatible = testCase.get("compatible").asBoolean();
      final List<String> errorMessages = new ArrayList<>();
      for (int j = 0; j < changes.size(); j++) {
        errorMessages.add(changes.get(j).asText());
      }
      final String description = testCase.get("description").asText();

      List<Difference> differences = SchemaDiff.compare(original, update);
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

  @Test
  public void checkCompatibilityUsingProtoFiles() throws Exception {

    ResourceLoader resourceLoader = new ResourceLoader(
        "/io/confluent/kafka/schemaregistry/protobuf/diff/");
    ProtoFileElement original = resourceLoader.readObj("TestProto.proto");
    ProtoFileElement update = resourceLoader.readObj("TestProto2.proto");

    List<Difference> changes = SchemaDiff.compare(original, update);

    assertTrue(changes.contains(new Difference(
        Difference.Type.FIELD_NAME_CHANGED,
        "#/TestMessage/2"
    )));
    assertTrue(changes.contains(new Difference(
        Difference.Type.FIELD_SCALAR_KIND_CHANGED,
        "#/TestMessage/2"
    )));
  }

  private static String readFile(String fileName) {
    ResourceLoader resourceLoader = new ResourceLoader("/");
    return resourceLoader.toString(fileName);
  }
}