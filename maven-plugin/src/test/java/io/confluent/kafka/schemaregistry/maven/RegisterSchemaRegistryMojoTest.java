/*
 * Copyright 2016-2025 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class RegisterSchemaRegistryMojoTest extends SchemaRegistryTest {
  RegisterSchemaRegistryMojo mojo;

  @Before
  public void createMojo(){
    this.mojo = new RegisterSchemaRegistryMojo();
    this.mojo.client(new MockSchemaRegistryClient());
  }

  @Test
  public void register() throws IOException, MojoFailureException, MojoExecutionException {
    Map<String, Integer> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    int version = 1;
    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-key", i);
      String valueSubject = String.format("TestSubject%03d-value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      writeSchema(keySchemaFile, keySchema);
      writeSchema(valueSchemaFile, valueSchema);
      subjectToFile.put(keySubject, keySchemaFile);
      expectedVersions.put(keySubject, version);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(valueSubject, version);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();

    Assert.assertThat(this.mojo.schemaVersions, IsEqual.equalTo(expectedVersions));
  }

  @Test
  public void registerSubjectWithSlash() throws IOException, MojoFailureException, MojoExecutionException {
    Map<String, Integer> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    int version = 1;
    for (int i = 0; i < 100; i++) {
      // Slash is _x2F
      String keySubject = String.format("TestSubject%03d_x2Fkey", i);
      String valueSubject = String.format("TestSubject%03d_x2Fvalue", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      writeSchema(keySchemaFile, keySchema);
      writeSchema(valueSchemaFile, valueSchema);
      subjectToFile.put(keySubject, keySchemaFile);
      expectedVersions.put(UploadSchemaRegistryMojo.decode(keySubject), version);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(UploadSchemaRegistryMojo.decode(valueSubject), version);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();

    Assert.assertThat(this.mojo.schemaVersions, IsEqual.equalTo(expectedVersions));
  }

  @Test
  public void registerSubjectWithSlashAndUnderscoreX() throws IOException, MojoFailureException, MojoExecutionException {
    Map<String, Integer> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    int version = 1;
    for (int i = 0; i < 100; i++) {
      // Slash is _x2F
      String keySubject = String.format("TestSubject%03d_x2Fkey_x", i);
      String valueSubject = String.format("TestSubject%03d_x2Fvalue_x", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      writeSchema(keySchemaFile, keySchema);
      writeSchema(valueSchemaFile, valueSchema);
      subjectToFile.put(keySubject, keySchemaFile);
      expectedVersions.put(UploadSchemaRegistryMojo.decode(keySubject), version);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(UploadSchemaRegistryMojo.decode(valueSubject), version);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();

    Assert.assertThat(this.mojo.schemaVersions, IsEqual.equalTo(expectedVersions));
  }

  @Test
  public void registerSubjectWithSlashDontDecode() throws IOException, MojoFailureException, MojoExecutionException {
    Map<String, Integer> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    int version = 1;
    for (int i = 0; i < 100; i++) {
      // Slash is _x2F
      String keySubject = String.format("TestSubject%03d_x2Fkey", i);
      String valueSubject = String.format("TestSubject%03d_x2Fvalue", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      writeSchema(keySchemaFile, keySchema);
      writeSchema(valueSchemaFile, valueSchema);
      subjectToFile.put(keySubject, keySchemaFile);
      expectedVersions.put(keySubject, version);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(valueSubject, version);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.decodeSubject = false;
    this.mojo.execute();

    Assert.assertThat(this.mojo.schemaVersions, IsEqual.equalTo(expectedVersions));
  }

  @Test(expected = IllegalStateException.class)
  public void malformedSchema() throws IOException, MojoFailureException, MojoExecutionException {
    Map<String, Integer> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    int version = 1;
    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-key", i);
      String valueSubject = String.format("TestSubject%03d-value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      if (i % 7 == 0) {
        writeMalformedFile(keySchemaFile);
        writeMalformedFile(valueSchemaFile);
      } else {
        writeSchema(keySchemaFile, keySchema);
        writeSchema(valueSchemaFile, valueSchema);
      }
      subjectToFile.put(keySubject, keySchemaFile);
      expectedVersions.put(keySubject, version);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(valueSubject, version);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();
  }

  @Test(expected = IllegalStateException.class)
  public void missingSchemas() throws IOException, MojoFailureException, MojoExecutionException {
    Map<String, Integer> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    int version = 1;
    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-key", i);
      String valueSubject = String.format("TestSubject%03d-value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      if (i % 7 == 0) {
        writeSchema(keySchemaFile, keySchema);
        writeSchema(valueSchemaFile, valueSchema);
      }
      subjectToFile.put(keySubject, keySchemaFile);
      expectedVersions.put(keySubject, version);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(valueSubject, version);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();
  }
}
