/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.maven;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class DeriveSchemaMojoTest extends SchemaRegistryTest {

  DeriveSchemaMojo mojo;
  private final ObjectMapper mapper = new ObjectMapper();

  @Before
  public void createMojoAndFiles() {
    this.mojo = new DeriveSchemaMojo();
    makeMessageFile();
    this.mojo.messagePath = new File(this.tempDirectory, "/message.json");
    this.mojo.outputPath = new File(this.tempDirectory, "/ans.json");
  }

  private void makeMessageFile() {
    String message = "{\"F1\": 1}";
    try (FileWriter writer = new FileWriter(this.tempDirectory + "/message.json")) {
      writer.write(message);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void checkExpected(String expectedSchema) throws IOException {
    String schema = MojoUtils.readFile(mojo.outputPath, StandardCharsets.UTF_8);
    JsonNode schemaInfo = mapper.readTree(schema);
    assert (schemaInfo.has("schemas"));
    assert (schemaInfo.get("schemas") instanceof ArrayNode);
    assertEquals(schemaInfo.get("schemas").size(), 1);
    JsonNode outputSchema = schemaInfo.get("schemas").get(0).get("schema");
    if (outputSchema.isTextual()) {
      assertEquals(expectedSchema, outputSchema.asText());
    } else {
      assertEquals(expectedSchema, outputSchema.toString());
    }
  }

  @Test
  public void testProtobuf() throws MojoExecutionException, IOException {
    mojo.schemaType = "protobuf";
    mojo.execute();
    String expectedSchema = "syntax = \"proto3\";\n" + "\n" +
        "message Schema {\n" +
        "  int32 F1 = 1;\n" +
        "}\n";
    checkExpected(expectedSchema);
  }

  @Test
  public void testAvro() throws MojoExecutionException, IOException {
    mojo.schemaType = "avro";
    mojo.execute();
    String expectedSchema = "{\"type\":\"record\",\"name\":\"Schema\",\"fields\":[{\"name\":\"F1\",\"type\":\"int\"}]}";
    checkExpected(expectedSchema);
  }

  @Test
  public void testJson() throws MojoExecutionException, IOException {
    mojo.schemaType = "json";
    mojo.execute();
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"F1\":{\"type\":\"number\"}}}";
    checkExpected(expectedSchema);
  }
}