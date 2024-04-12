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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
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

    for(int i=1;i<=9;i++) {
      this.mojo.schemaTypes.put("schema"+i, "AVRO");
    }

    this.mojo.schemaTypes.put(schema10, "JSON");
    this.mojo.schemaTypes.put(schema13, "JSON");
    this.mojo.schemaTypes.put(schema14, "JSON");

  }

  private void makeFile(String schemaString, String name) {

    try (FileWriter writer = new FileWriter(this.tempDirectory+"/"+name)) {
      writer.write(schemaString);
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (name.contains("1.avsc") || name.contains("2.avsc")) {

      try (FileWriter writer = new FileWriter(this.tempDirectory+"/schema12Folder/"+name)) {
        writer.write(schemaString);
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

  }

  private void makeFiles(){

    File newFolder = new File(this.tempDirectory.toString() + "/schema12Folder");
    if( newFolder.mkdir()) {
      System.out.println("New Folder avro created successfully.");
    }

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
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"},"
        + " {\"type\":\"string\",\"name\":\"f3\", \"default\": \"bar\"}]}";
    makeFile(schemaString8, "schema8.avsc");

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


  private void setMojo(String schema, String previousSchemas){

    this.mojo.schemas = Collections.singletonMap(schema, new File(this.tempDirectory + "/" + schema + fileExtension));
    this.mojo.previousSchemaPaths = new HashMap<>();

    File temp = new File(this.tempDirectory + "/" + previousSchemas);
    if(temp.isDirectory())
      this.mojo.previousSchemaPaths.put(schema, new File(this.tempDirectory + "/" + previousSchemas));
    else
      this.mojo.previousSchemaPaths.put(schema, new File(this.tempDirectory + "/" + previousSchemas + fileExtension));

  }

  private boolean isCompatible(String schema, String previousSchemas, CompatibilityLevel compatibilityLevel)
      throws MojoExecutionException {

    setMojo(schema, previousSchemas);
    this.mojo.compatibilityLevels.put(schema, compatibilityLevel);
    this.mojo.execute();
    return true;

  }

  /*
   * Backward compatibility: A new schema is backward compatible if it can be used to read the data
   * written in the previous schema.
   */
  @Test
  public void testBasicBackwardsCompatibility() throws MojoExecutionException {

    fileExtension = ".avsc";

    assertTrue("adding a field with default is a backward compatible change",
        isCompatible(schema2, (schema1), CompatibilityLevel.BACKWARD));
    assertThrows("adding a field w/o default is not a backward compatible change",
        MojoExecutionException.class, () -> isCompatible(schema3, (schema1), CompatibilityLevel.BACKWARD));
    assertTrue("changing field name with alias is a backward compatible change",
        isCompatible(schema4, (schema1), CompatibilityLevel.BACKWARD));
    assertTrue("evolving a field type to a union is a backward compatible change",
        isCompatible(schema6, (schema1), CompatibilityLevel.BACKWARD));
    assertThrows("removing a type from a union is not a backward compatible change",
        MojoExecutionException.class, () -> isCompatible(schema1, (schema6), CompatibilityLevel.BACKWARD));
    assertTrue("adding a new type in union is a backward compatible change",
        isCompatible(schema7, (schema6), CompatibilityLevel.BACKWARD));
    assertThrows("removing a type from a union is not a backward compatible change",
        MojoExecutionException.class, () -> isCompatible(schema6, (schema7), CompatibilityLevel.BACKWARD));


    this.mojo.schemaTypes.put(schema10, "JSON");
    this.mojo.schemaTypes.put(schema13, "JSON");
    this.mojo.schemaTypes.put(schema14, "JSON");

    fileExtension = ".json";
    assertTrue("setting additional properties to true from false is a backward compatible change",
        isCompatible(schema10, schema11, CompatibilityLevel.BACKWARD));

    assertTrue("adding property of string type (same as additional properties type) is "
        + "a backward compatible change", isCompatible(schema13, schema12, CompatibilityLevel.BACKWARD));

    assertTrue("adding property of string or int type (string is additional properties type) is "
        + "a backward compatible change", isCompatible(schema14, schema12, CompatibilityLevel.BACKWARD));

  }

  @Test
  public void testBasicBackwardsTransitiveCompatibility() throws MojoExecutionException {

    fileExtension = ".avsc";

    // 1 == 2, 2 == 3, 3 != 1
    assertTrue("adding a field with default is a backward compatible change",
        isCompatible(schema2, (schema1), CompatibilityLevel.BACKWARD_TRANSITIVE));
    assertTrue("removing a default is a compatible change, but not transitively",
        isCompatible(schema3, (schema2), CompatibilityLevel.BACKWARD_TRANSITIVE));

    // Not compatible throws error
    assertThrows("removing a default is not a transitively compatible change",
        MojoExecutionException.class, () ->isCompatible(schema3, "schema12Folder", CompatibilityLevel.BACKWARD_TRANSITIVE));

    assertTrue("Checking if schema8 is backward compatible with schema1 and schema2 present in avro folder"
        , isCompatible(schema8, "schema12Folder", CompatibilityLevel.BACKWARD_TRANSITIVE ));


  }

  /*
   * Forward compatibility: A new schema is forward compatible if the previous schema can read data written in this
   * schema.
   */
  @Test
  public void testBasicForwardsCompatibility() throws MojoExecutionException {

    fileExtension = ".avsc";

    assertTrue("adding a field is a forward compatible change",
        isCompatible(schema2, (schema1), CompatibilityLevel.FORWARD));
    assertTrue("adding a field is a forward compatible change",
        isCompatible(schema3, (schema1), CompatibilityLevel.FORWARD));
    assertTrue("adding a field is a forward compatible change",
        isCompatible(schema3, (schema2), CompatibilityLevel.FORWARD));
    assertTrue("adding a field is a forward compatible change",
        isCompatible(schema2, (schema3), CompatibilityLevel.FORWARD));

    fileExtension = ".avsc";

    // Only schema 2 is checked
    assertThrows( MojoExecutionException.class, () ->
        isCompatible(schema1, "schema12Folder", CompatibilityLevel.FORWARD));

    fileExtension = ".json";
    this.mojo.schemaTypes.put(schema11, "JSON");
    this.mojo.schemaTypes.put(schema12, "JSON");
    this.mojo.schemaTypes.put(schema13, "JSON");

    assertTrue("setting additional properties to false from true is a forward compatible change",
        isCompatible(schema11, schema10, CompatibilityLevel.FORWARD));

    assertTrue("removing property of string type (same as additional properties type)"
        + " is a backward compatible change", isCompatible(schema13,
        schema12, CompatibilityLevel.FORWARD));

    assertTrue("removing property of string or int type (string is additional properties type) is "
        + "a backward compatible change", isCompatible(schema12,
        schema14, CompatibilityLevel.FORWARD));

  }

  /*
   * Forward transitive compatibility: A new schema is forward compatible if all previous schemas can read data written
   * in this schema.
   */
  @Test
  public void testBasicForwardsTransitiveCompatibility() throws MojoExecutionException {

    fileExtension = ".avsc";

    // 1 == 2, 2 == 3, 3 != 1
    assertTrue("adding default to a field is a compatible change",
        isCompatible(schema2, (schema3), CompatibilityLevel.FORWARD_TRANSITIVE));
    assertTrue("removing a field with a default is a compatible change",
        isCompatible(schema1, (schema2), CompatibilityLevel.FORWARD_TRANSITIVE));
  }

  /*
   * Full compatibility: A new schema is fully compatible if it’s both backward and forward compatible.
   */
  @Test
  public void testBasicFullCompatibility() throws MojoExecutionException {

    fileExtension = ".avsc";

    assertTrue("adding a field with default is a backward and a forward compatible change",
        isCompatible(schema2, (schema1), CompatibilityLevel.FULL));

    // Throws error, provide exactly one file for checking full compatibility
    assertThrows(MojoExecutionException.class, () ->
        isCompatible(schema3, "schema12Folder", CompatibilityLevel.FULL));

  }

  /*
   * Full transitive compatibility: A new schema is fully compatible if it’s both transitively backward
   * and transitively forward compatible with the entire schema history.
   */
  @Test
  public void testBasicFullTransitiveCompatibility() throws MojoExecutionException {

    fileExtension = ".avsc";

    assertTrue("iteratively adding fields with defaults is a compatible change",
        isCompatible(schema8, "schema12Folder", CompatibilityLevel.FULL_TRANSITIVE));
    assertTrue("adding default to a field is a compatible change",
        isCompatible(schema2, (schema3), CompatibilityLevel.FULL_TRANSITIVE));
    assertTrue("removing a field with a default is a compatible change",
        isCompatible(schema1, (schema2), CompatibilityLevel.FULL_TRANSITIVE));

    assertTrue("adding a field with default is a compatible change",
        isCompatible(schema2, (schema1), CompatibilityLevel.FULL_TRANSITIVE));
    assertTrue("removing a default from a field compatible change",
        isCompatible(schema3, (schema2), CompatibilityLevel.FULL_TRANSITIVE));

    assertThrows( "transitively adding a field without a default is not a compatible change",
        MojoExecutionException.class, () -> isCompatible(schema3, "schema12Folder", CompatibilityLevel.FULL_TRANSITIVE));

  }
}