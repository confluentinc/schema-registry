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

import org.apache.maven.plugin.MojoExecutionException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DeriveSchemaMojoTest extends SchemaRegistryTest {

  DeriveSchemaMojo mojo;

  @Before
  public void createMojoAndFiles() {
    this.mojo = new DeriveSchemaMojo();
    String avroMessage = "{\"String\": \"John Smith\","
        + "    \"LongName\": 1202021021034,"
        + "    \"Integer\": 9999239,"
        + "    \"Boolean\": false,"
        + "    \"Float\": 1e16,"
        + "    \"Double\": 62323232.78901245,"
        + "    \"Null\": null"
        + "  }";
    makeFile(avroMessage, "message.json");
    this.mojo.messagePath = new File(this.tempDirectory + "/message.json");
  }

  public void makeFile(String message, String name) {
    try (FileWriter writer = new FileWriter(this.tempDirectory + "/" + name)) {
      writer.write(message);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testProtobuf() throws MojoExecutionException, IOException {
    mojo.schemaType = "protobuf";
    mojo.outputPath = new File(this.tempDirectory + "/ans.json");
    mojo.execute();
    String expectedSchema = "syntax = \"proto3\";\n" + "\n" +
        "import \"google/protobuf/any.proto\";\n" + "\n" +
        "message Schema {\n" +
        "  bool Boolean = 1;\n" +
        "  double Double = 2;\n" +
        "  double Float = 3;\n" +
        "  int32 Integer = 4;\n" +
        "  int64 LongName = 5;\n" +
        "  google.protobuf.Any Null = 6;\n" +
        "  string String = 7;\n" +
        "}\n";
    String schema = MojoUtils.readFile(mojo.outputPath, StandardCharsets.UTF_8);
    JSONObject schemaInfo = new JSONObject(schema);
    assert (schemaInfo.has("schemas"));
    assert (schemaInfo.get("schemas") instanceof JSONArray);
    assertEquals(schemaInfo.getJSONArray("schemas").length(), 1);
    String outputSchema = schemaInfo.getJSONArray("schemas").getJSONObject(0).getString("schema");
    assertEquals(expectedSchema, outputSchema);
  }

  @Test
  public void testError() throws MojoExecutionException {
    assertThrows(NullPointerException.class, () -> mojo.execute());
    mojo.messagePath = new File(this.tempDirectory + "/protoMessage.json");
    assertThrows(IllegalArgumentException.class, () -> mojo.execute());
    mojo.schemaType = "proto";
    assertThrows(IllegalArgumentException.class, () -> mojo.execute());
    mojo.schemaType = "protobuf";
    mojo.execute();
  }
}