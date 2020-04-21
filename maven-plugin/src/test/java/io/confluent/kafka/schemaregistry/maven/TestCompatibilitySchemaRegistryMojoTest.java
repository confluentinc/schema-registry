/*
 * Copyright 2018 Confluent Inc.
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
      registerSchemas(i, subjectToFile, expectedVersions);
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
      this.mojo.client().register(keySubject, keySchema);
      this.mojo.client().register(valueSubject, valueSchema);
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      if (i % 7 == 0) {
        writeMalformedFile(keySchemaFile);
        writeMalformedFile(valueSchemaFile);
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
      this.mojo.client().register(keySubject, keySchema);
      this.mojo.client().register(valueSubject, valueSchema);
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
      this.mojo.client().register(keySubject, keySchema);
      this.mojo.client().register(valueSubject, valueSchema);
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

  @Test(expected = IllegalStateException.class)
  public void badSchemaRegistryIOException() throws IOException, MojoFailureException, MojoExecutionException, RestClientException {

    this.mojo.client(new IOExceptionThrowingSchemaRegistryClient());
    Map<String, File> subjectToFile = new LinkedHashMap<>();
    Map<String, Boolean> expectedVersions = new LinkedHashMap<>();
    registerSchemas(1, subjectToFile, expectedVersions);
    this.mojo.subjects = subjectToFile;
    this.mojo.execute();

  }

  @Test(expected = IllegalStateException.class)
  public void badSchemaRegistryRestClientException() throws IOException, MojoFailureException, MojoExecutionException, RestClientException {

    this.mojo.client(new RestClientExceptionThrowingSchemaRegistryClient());
    Map<String, File> subjectToFile = new LinkedHashMap<>();
    Map<String, Boolean> expectedVersions = new LinkedHashMap<>();
    registerSchemas(1, subjectToFile, expectedVersions);
    this.mojo.subjects = subjectToFile;
    this.mojo.execute();

  }

  /**
   * Reuseable Code for Creating a Schema
   * @param subjectNumber The Number to append to the Subject Name
   * @param subjectToFile A Map of string to Files
   * @param expectedVersions A Map of String to Successful Updates
   * @throws IOException Thrown On an Error on the Response Payload
   * @throws RestClientException Thrown when we cammpt access the server
   *
   */
  private void registerSchemas(Integer subjectNumber, Map<String, File> subjectToFile, Map<String, Boolean> expectedVersions) throws IOException, RestClientException{
    String keySubject = String.format("TestSubject%03d-Key", subjectNumber);
    String valueSubject = String.format("TestSubject%03d-Value", subjectNumber);
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
    this.mojo.client().register(keySubject, keySchema);
    this.mojo.client().register(valueSubject, valueSchema);
    File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
    File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
    writeSchema(keySchemaFile, keySchema);
    writeSchema(valueSchemaFile, valueSchema);
    subjectToFile.put(keySubject, keySchemaFile);
    expectedVersions.put(keySubject, true);
    subjectToFile.put(valueSubject, valueSchemaFile);
    expectedVersions.put(valueSubject, true);
  }


  class IOExceptionThrowingSchemaRegistryClient extends MockSchemaRegistryClient {
    @Override
    public boolean testCompatibility(String subject, Schema newSchema) throws IOException {
      throw new IOException();
    }
  }

  class RestClientExceptionThrowingSchemaRegistryClient extends MockSchemaRegistryClient {
    @Override
    public boolean testCompatibility(String subject, Schema newSchema) throws RestClientException {
      throw new RestClientException();
    }
  }
}
