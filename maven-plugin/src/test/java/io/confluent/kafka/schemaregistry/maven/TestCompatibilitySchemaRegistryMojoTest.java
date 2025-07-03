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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
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

public class TestCompatibilitySchemaRegistryMojoTest extends SchemaRegistryTest {
  TestCompatibilitySchemaRegistryMojo mojo;

  @Before
  public void createMojo() {
    this.mojo = new TestCompatibilitySchemaRegistryMojo();
    this.mojo.client(new MockSchemaRegistryClient());
  }

  @Test
  public void register() throws IOException, MojoFailureException, MojoExecutionException, RestClientException {
    Map<String, Boolean> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-Key", i);
      String valueSubject = String.format("TestSubject%03d-Value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      this.mojo.client().register(keySubject, new AvroSchema(keySchema));
      this.mojo.client().register(valueSubject, new AvroSchema(valueSchema));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      writeSchema(keySchemaFile, keySchema);
      writeSchema(valueSchemaFile, valueSchema);
      subjectToFile.put(keySubject, keySchemaFile);
      expectedVersions.put(keySubject, true);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(valueSubject, true);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();

    Assert.assertThat(this.mojo.schemaCompatibility, IsEqual.equalTo(expectedVersions));
  }

  @Test(expected = IllegalStateException.class)
  public void malformedSchema() throws IOException, MojoFailureException, MojoExecutionException, RestClientException {


    Map<String, File> subjectToFile = new LinkedHashMap<>();
    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-Key", i);
      String valueSubject = String.format("TestSubject%03d-Value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      this.mojo.client().register(keySubject, new AvroSchema(keySchema));
      this.mojo.client().register(valueSubject, new AvroSchema(valueSchema));
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
      subjectToFile.put(valueSubject, valueSchemaFile);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();
  }

  @Test(expected = IllegalStateException.class)
  public void missingSchemas() throws IOException, MojoFailureException, MojoExecutionException, RestClientException {

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-Key", i);
      String valueSubject = String.format("TestSubject%03d-Value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      this.mojo.client().register(keySubject, new AvroSchema(keySchema));
      this.mojo.client().register(valueSubject, new AvroSchema(valueSchema));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      if (i % 7 == 0) {
        writeSchema(keySchemaFile, keySchema);
        writeSchema(valueSchemaFile, valueSchema);
      }
      subjectToFile.put(keySubject, keySchemaFile);
      subjectToFile.put(valueSubject, valueSchemaFile);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();
  }

  @Test
  public void incompatibleSchemas() throws IOException, MojoFailureException, MojoExecutionException, RestClientException {
    Map<String, Boolean> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-Key", i);
      String valueSubject = String.format("TestSubject%03d-Value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      this.mojo.client().register(keySubject, new AvroSchema(keySchema));
      this.mojo.client().register(valueSubject, new AvroSchema(valueSchema));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");

      subjectToFile.put(keySubject, keySchemaFile);
      boolean keyCompatible = true, valueCompatible = true;
      if (i % 7 == 0) {
        keyCompatible = false;
        valueCompatible = false;
        keySchema = Schema.create(Schema.Type.INT);
        valueSchema = Schema.create(Schema.Type.BYTES);
      }
      writeSchema(keySchemaFile, keySchema);
      writeSchema(valueSchemaFile, valueSchema);

      expectedVersions.put(keySubject, keyCompatible);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(valueSubject, valueCompatible);
    }

    this.mojo.subjects = subjectToFile;
    try {
      this.mojo.execute();
      Assert.fail("IllegalState exception should have been thrown.");
    } catch (IllegalStateException ex) {

    }

    Assert.assertThat(this.mojo.schemaCompatibility, IsEqual.equalTo(expectedVersions));
  }
}
