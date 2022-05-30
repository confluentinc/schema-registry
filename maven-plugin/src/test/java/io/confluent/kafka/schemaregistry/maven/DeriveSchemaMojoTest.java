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

import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.ReadFileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DeriveSchemaMojoTest extends SchemaRegistryTest {
  DeriveSchemaMojo mojo;

  @Before
  public void createMojoAndFiles() {

    this.mojo = new DeriveSchemaMojo();

    String avroMessage =
        "{\n"
            + "    \"String\": \"John Smith\",\n"
            + "    \"LongName\": 1202021021034,\n"
            + "    \"Integer\": 9999239,\n"
            + "    \"Boolean\": false,\n"
            + "    \"Float\": 1e16,\n"
            + "    \"Double\": 62323232.78901245,\n"
            + "    \"Null\": null\n"
            + "  }";

    makeFile(avroMessage, "avroMessage.json");

    String jsonMessage =
        "{\n"
            + "    \"String\": \"John Smith\",\n"
            + "    \"LongName\": 12020210210,\n"
            + "    \"BigDataType\": 12020210222344343333333333120202102223443433333333331202021022234434333333333312020210222344343333333333120202102223443433333333331202021022234434333333333312020210222344343333333333,\n"
            + "    \"Integer\": 12,\n"
            + "    \"Boolean\": false,\n"
            + "    \"Float\": 1e16,\n"
            + "    \"Double\": 624333333333333333333333333333333333333333323232.789012332222222245,\n"
            + "    \"Null\": null\n"
            + "  }";

    makeFile(jsonMessage, "jsonMessage.json");

    String protoMessage =
        "{\n"
            + "    \"String\": \"John Smith\",\n"
            + "    \"LongName\": 12020210210567,\n"
            + "    \"Integer\": 12,\n"
            + "    \"Booleans\": true,\n"
            + "    \"Float\": 1e16,\n"
            + "    \"Double\": 53443343443.453,\n"
            + "    \"Null\": null"
            + "  }";

    makeFile(protoMessage, "protoMessage.json");

  }

  public void makeFile(String message, String name) {

    try (FileWriter writer = new FileWriter(this.tempDirectory + "/" + name)) {
      writer.write(message);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @Test
  public void testAvro() throws MojoExecutionException, MojoFailureException, IOException {
    mojo.schemaType = "Avro";
    mojo.messagePath = new File(this.tempDirectory + "/avroMessage.json");
    mojo.outputPath = new File(this.tempDirectory + "/ans.json");
    mojo.strictCheck = true;
    mojo.execute();
    String expectedSchema = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"Boolean\",\"type\":\"boolean\"},{\"name\":\"Double\",\"type\":\"double\"},{\"name\":\"Float\",\"type\":\"double\"},{\"name\":\"Integer\",\"type\":\"int\"},{\"name\":\"LongName\",\"type\":\"long\"},{\"name\":\"Null\",\"type\":\"null\"},{\"name\":\"String\",\"type\":\"string\"}]}";
    String schema = ReadFileUtils.readFile(this.tempDirectory + "/ans.json");
    JSONObject schemaInfo = new JSONObject(schema);
    assert(schemaInfo.has("schemas"));
    assert(schemaInfo.get("schemas") instanceof JSONArray);
    assertEquals(schemaInfo.getJSONArray("schemas").length(), 1);
    JSONObject outputSchema = schemaInfo.getJSONArray("schemas").getJSONObject(0).getJSONObject("schema");
    assertEquals(expectedSchema, outputSchema.toString());
  }

  @Test
  public void testJson() throws MojoExecutionException, MojoFailureException, IOException {
    mojo.schemaType = "json";
    mojo.messagePath = new File(this.tempDirectory + "/jsonMessage.json");
    mojo.outputPath = new File(this.tempDirectory + "/ans.json");
    mojo.strictCheck = true;
    mojo.execute();
    String expectedSchema = "{\"type\":\"object\",\"properties\":{\"Integer\":{\"type\":\"number\"},\"Float\":{\"type\":\"number\"},\"Null\":{\"type\":\"null\"},\"LongName\":{\"type\":\"number\"},\"String\":{\"type\":\"string\"},\"Boolean\":{\"type\":\"boolean\"},\"Double\":{\"type\":\"number\"},\"BigDataType\":{\"type\":\"number\"}}}";
    String schema = ReadFileUtils.readFile(this.tempDirectory + "/ans.json");
    JSONObject schemaInfo = new JSONObject(schema);
    assertEquals(expectedSchema, schemaInfo.getJSONObject("schema").toString());
  }

  @Test
  public void testProtobuf() throws MojoExecutionException, MojoFailureException, IOException {
    mojo.schemaType = "protobuf";
    mojo.messagePath = new File(this.tempDirectory + "/protoMessage.json");
    mojo.outputPath = new File(this.tempDirectory + "/ans.json");
    mojo.strictCheck = true;
    mojo.execute();
    String expectedSchema = "syntax = \"proto3\";\n" +
        "\n" +
        "import \"google/protobuf/any.proto\";\n" +
        "\n" +
        "message Record {\n" +
        "  bool Booleans = 1;\n" +
        "  double Double = 2;\n" +
        "  double Float = 3;\n" +
        "  int32 Integer = 4;\n" +
        "  int64 LongName = 5;\n" +
        "  google.protobuf.Any Null = 6;\n" +
        "  string String = 7;\n" +
        "}\n";
    String schema = ReadFileUtils.readFile(this.tempDirectory + "/ans.json");
    JSONObject schemaInfo = new JSONObject(schema);
    assert(schemaInfo.has("schemas"));
    assert(schemaInfo.get("schemas") instanceof JSONArray);
    assertEquals(schemaInfo.getJSONArray("schemas").length(), 1);
    String outputSchema = schemaInfo.getJSONArray("schemas").getJSONObject(0).getString("schema");
    assertEquals(expectedSchema, outputSchema);
  }

  @Test
  public void testError() throws MojoExecutionException, MojoFailureException {

    assertThrows(NullPointerException.class, () -> mojo.execute());

    mojo.messagePath = new File(this.tempDirectory + "/protoMessage.json");
    assertThrows(IllegalArgumentException.class, () -> mojo.execute());

    mojo.schemaType = "proto";
    assertThrows(IllegalArgumentException.class, () -> mojo.execute());

    mojo.schemaType = "protobuf";
    mojo.execute();

  }

}