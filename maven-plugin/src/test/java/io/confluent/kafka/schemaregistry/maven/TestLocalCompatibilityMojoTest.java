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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.Before;
import org.junit.Test;

/*
 * The tests for avro are taken from AvroCompatibilityTest
 */
public class TestLocalCompatibilityMojoTest extends SchemaRegistryTest{
  TestLocalCompatibilityMojo mojo;

  final String schema1 = "schema1";
  final String schema2 = "schema2";
  final String schema3 = "schema3";
  final String schema4 = "schema4";
  final String schema6 = "schema6";
  final String schema7 = "schema7";
  final String schema8 = "schema8";
  final String schema10 = "schema10";
  final String schema11 = "schema11";
  final String schema12 = "schema12";
  final String schema13 = "schema13";
  final String schema14 = "schema14";

  String fileExtension;

  @Before
  public void createMojoAndFiles() {
    this.mojo = new TestLocalCompatibilityMojo();
    makeFiles();
  }

  private void makeFile(String schemaString, String name){

    try (FileWriter writer = new FileWriter(this.tempDirectory+"/"+name)) {
      writer.write(schemaString);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  private void makeFiles(){

    String schemaString1 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}";
    makeFile(schemaString1, "schema1.avsc");

    String schemaString2 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}";
    makeFile(schemaString2, "schema2.avsc");

    String schemaString3 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}";
    makeFile(schemaString3, "schema3.avsc");

    String schemaString4 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1_new\", \"aliases\": [\"f1\"]}]}";
    makeFile(schemaString4, "schema4.avsc");

    String schemaString6 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":[\"null\", \"string\"],\"name\":\"f1\","
        + " \"doc\":\"doc of f1\"}]}";
    makeFile(schemaString6, "schema6.avsc");

    String schemaString7 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":[\"null\", \"string\", \"int\"],\"name\":\"f1\","
        + " \"doc\":\"doc of f1\"}]}";
    makeFile(schemaString7, "schema7.avsc");

    String schemaString8 = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]},"
        + " {\"type\":\"string\",\"name\":\"f3\", \"default\": \"bar\"}]}";
    makeFile(schemaString8, "schema8.avsc");

    String badDefaultNullString = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":[\"null\", \"string\"],\"name\":\"f1\", \"default\": \"null\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"},"
        + " {\"type\":\"string\",\"name\":\"f3\", \"default\": \"bar\"}]}";
    makeFile(badDefaultNullString, "schema9.avsc");

    String schemaString10 = "{\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"foo\": { \"type\": \"string\" },\n"
        + "    \"bar\": { \"type\": \"string\" }\n"
        + "  }\n"
        + "}";
    makeFile(schemaString10, "schema10.json");

    String schemaString11 = "{\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"foo\": { \"type\": \"string\" },\n"
        + "    \"bar\": { \"type\": \"string\" }\n"
        + "  },\n"
        + "  \"additionalProperties\": false\n"
        + "}";
    makeFile(schemaString11, "schema11.json");

    String schemaString12 = "{\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"foo\": { \"type\": \"string\" },\n"
        + "    \"bar\": { \"type\": \"string\" }\n"
        + "  },\n"
        + "  \"additionalProperties\": { \"type\": \"string\" }\n"
        + "}";

    makeFile(schemaString12, "schema12.json");

    String schemaString13 = "{\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"foo\": { \"type\": \"string\" },\n"
        + "    \"bar\": { \"type\": \"string\" },\n"
        + "    \"zap\": { \"type\": \"string\" }\n"
        + "  },\n"
        + "  \"additionalProperties\": { \"type\": \"string\" }\n"
        + "}";

    makeFile(schemaString13, "schema13.json");

    String schemaString14 = "{\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"foo\": { \"type\": \"string\" },\n"
        + "    \"bar\": { \"type\": \"string\" },\n"
        + "    \"zap\": { \n"
        + "      \"oneOf\": [ { \"type\": \"string\" }, { \"type\": \"integer\" } ] \n"
        + "    }\n"
        + "  },\n"
        + "  \"additionalProperties\": { \"type\": \"string\" }\n"
        + "}";

    makeFile(schemaString14, "schema14.json");

  }


  private void setMojo(String schema, List<String> previousSchemas){

    this.mojo.schemaPath = new File(this.tempDirectory + "/" + schema + fileExtension);
    this.mojo.previousSchemaPaths = new ArrayList<>();

    for (String path : previousSchemas) {
      this.mojo.previousSchemaPaths.add(new File(this.tempDirectory + "/" + path + fileExtension));
    }
    this.mojo.success = false;

  }

  private boolean isCompatible(String schema, List<String> previousSchemas)
      throws MojoExecutionException {

    setMojo(schema, previousSchemas);
    this.mojo.execute();
    return this.mojo.success;

  }



  /*
   * Backward compatibility: A new schema is backward compatible if it can be used to read the data
   * written in the previous schema.
   */
  @Test
  public void testBasicBackwardsCompatibility() throws MojoExecutionException {

    this.mojo.compatibilityLevel = CompatibilityLevel.BACKWARD;
    fileExtension = ".avsc";
    this.mojo.schemaType = "avro";

    assertTrue("adding a field with default is a backward compatible change",
        isCompatible(schema2, Collections.singletonList(schema1)));
    assertFalse("adding a field w/o default is not a backward compatible change",
        isCompatible(schema3, Collections.singletonList(schema1)));
    assertTrue("changing field name with alias is a backward compatible change",
        isCompatible(schema4, Collections.singletonList(schema1)));
    assertTrue("evolving a field type to a union is a backward compatible change",
        isCompatible(schema6, Collections.singletonList(schema1)));
    assertFalse("removing a type from a union is not a backward compatible change",
        isCompatible(schema1, Collections.singletonList(schema6)));
    assertTrue("adding a new type in union is a backward compatible change",
        isCompatible(schema7, Collections.singletonList(schema6)));
    assertFalse("removing a type from a union is not a backward compatible change",
        isCompatible(schema6, Collections.singletonList(schema7)));

    // Only schema 2 is checked
    assertTrue("removing a default is not a transitively compatible change",
        isCompatible(schema3, Arrays.asList(schema1, schema2)));

    fileExtension = ".json";
    this.mojo.schemaType = "json";
    assertTrue("setting additional properties to true from false is a backward compatible change",
        isCompatible(schema10, Collections.singletonList(schema11)));

    assertTrue("adding property of string type (same as additional properties type) is "
        + "a backward compatible change", isCompatible(schema13,
        Collections.singletonList(schema12)));

    assertTrue("adding property of string or int type (string is additional properties type) is "
        + "a backward compatible change", isCompatible(schema14,
        Collections.singletonList(schema12)));

  }
  @Test
  public void testBasicBackwardsTransitiveCompatibility() throws MojoExecutionException {

    this.mojo.compatibilityLevel = CompatibilityLevel.BACKWARD_TRANSITIVE;
    fileExtension = ".avsc";
    this.mojo.schemaType = "avro";

    // All compatible
    assertTrue("iteratively adding fields with defaults is a compatible change",
        isCompatible(schema8, Arrays.asList(schema1, schema2)));

//     1 == 2, 2 == 3, 3 != 1
    assertTrue("adding a field with default is a backward compatible change",
        isCompatible(schema2, Collections.singletonList(schema1)));
    assertTrue("removing a default is a compatible change, but not transitively",
        isCompatible(schema3, Collections.singletonList(schema2)));
    assertFalse("removing a default is not a transitively compatible change",
        isCompatible(schema3, Arrays.asList(schema2, schema1)));
  }

  /*
   * Forward compatibility: A new schema is forward compatible if the previous schema can read data written in this
   * schema.
   */
  @Test
  public void testBasicForwardsCompatibility() throws MojoExecutionException {

    this.mojo.compatibilityLevel = CompatibilityLevel.FORWARD;

    fileExtension = ".avsc";
    this.mojo.schemaType = "avro";

    assertTrue("adding a field is a forward compatible change",
        isCompatible(schema2, Collections.singletonList(schema1)));
    assertTrue("adding a field is a forward compatible change",
        isCompatible(schema3, Collections.singletonList(schema1)));
    assertTrue("adding a field is a forward compatible change",
        isCompatible(schema3, Collections.singletonList(schema2)));
    assertTrue("adding a field is a forward compatible change",
        isCompatible(schema2, Collections.singletonList(schema3)));

    // Only schema 2 is checked
    assertTrue("removing a default is not a transitively compatible change",
        isCompatible(schema1, Arrays.asList(schema3, schema2)));

    fileExtension = ".json";
    this.mojo.schemaType = "json";

    assertTrue("setting additional properties to false from true is a forward compatible change",
        isCompatible(schema11, Collections.singletonList(schema10)));

    assertTrue("removing property of string type (same as additional properties type)"
        + " is a backward compatible change", isCompatible(schema13,
        Collections.singletonList(schema12)));

    assertTrue("removing property of string or int type (string is additional properties type) is "
        + "a backward compatible change", isCompatible(schema12,
        Collections.singletonList(schema14)));

  }

  /*
   * Forward transitive compatibility: A new schema is forward compatible if all previous schemas can read data written
   * in this schema.
   */
  @Test
  public void testBasicForwardsTransitiveCompatibility() throws MojoExecutionException {

    this.mojo.compatibilityLevel = CompatibilityLevel.FORWARD_TRANSITIVE;
    fileExtension = ".avsc";
    this.mojo.schemaType = "avro";

    // All compatible
    assertTrue("iteratively removing fields with defaults is a compatible change",
        isCompatible(schema1, Arrays.asList(schema8, schema2)));

    // 1 == 2, 2 == 3, 3 != 1
    assertTrue("adding default to a field is a compatible change",
        isCompatible(schema2, Collections.singletonList(schema3)));
    assertTrue("removing a field with a default is a compatible change",
        isCompatible(schema1, Collections.singletonList(schema2)));
    assertFalse("removing a default is not a transitively compatible change",
        isCompatible(schema1, Arrays.asList(schema2, schema3)));
  }

  /*
   * Full compatibility: A new schema is fully compatible if it’s both backward and forward compatible.
   */
  @Test
  public void testBasicFullCompatibility() throws MojoExecutionException {

    this.mojo.compatibilityLevel = CompatibilityLevel.FULL;
    fileExtension = ".avsc";
    this.mojo.schemaType = "avro";

    assertTrue("adding a field with default is a backward and a forward compatible change",
        isCompatible(schema2, Collections.singletonList(schema1)));

    // Only schema 2 is checked!
    assertTrue("transitively adding a field without a default is not a compatible change",
        isCompatible(schema3, Arrays.asList(schema1, schema2)));
    // Only schema 2 is checked!
    assertTrue("transitively removing a field without a default is not a compatible change",
        isCompatible(schema1, Arrays.asList(schema3, schema2)));

  }

  /*
   * Full transitive compatibility: A new schema is fully compatible if it’s both transitively backward
   * and transitively forward compatible with the entire schema history.
   */
  @Test
  public void testBasicFullTransitiveCompatibility() throws MojoExecutionException {

    this.mojo.compatibilityLevel = CompatibilityLevel.FULL_TRANSITIVE;
    fileExtension = ".avsc";
    this.mojo.schemaType = "avro";

    // Simple check
    assertTrue("iteratively adding fields with defaults is a compatible change",
        isCompatible(schema8, Arrays.asList(schema1, schema2)));
    assertTrue("iteratively removing fields with defaults is a compatible change",
        isCompatible(schema1, Arrays.asList(schema8, schema2)));

    assertTrue("adding default to a field is a compatible change",
        isCompatible(schema2, Collections.singletonList(schema3)));
    assertTrue("removing a field with a default is a compatible change",
        isCompatible(schema1, Collections.singletonList(schema2)));

    assertTrue("adding a field with default is a compatible change",
        isCompatible(schema2, Collections.singletonList(schema1)));
    assertTrue("removing a default from a field compatible change",
        isCompatible(schema3, Collections.singletonList(schema2)));

    assertFalse("transitively adding a field without a default is not a compatible change",
        isCompatible(schema3, Arrays.asList(schema2, schema1)));
    assertFalse("transitively removing a field without a default is not a compatible change",
        isCompatible(schema1, Arrays.asList(schema2, schema3)));
  }
}