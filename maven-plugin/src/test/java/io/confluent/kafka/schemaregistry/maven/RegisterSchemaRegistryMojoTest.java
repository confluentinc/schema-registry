package io.confluent.kafka.schemaregistry.maven;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.hamcrest.core.IsEqual;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class RegisterSchemaRegistryMojoTest {
  RegisterSchemaRegistryMojo mojo;
  File tempDirectory;

  @Before
  public void before() throws IOException {
    this.mojo = new RegisterSchemaRegistryMojo();
    this.mojo.client(new MockSchemaRegistryClient());
    this.tempDirectory = File.createTempFile(this.getClass().getSimpleName(), "tmp");
    this.tempDirectory.delete();
    this.tempDirectory.mkdirs();
  }

  void writeSchema(File outputPath, Schema schema) throws IOException {
    try (FileWriter writer = new FileWriter(outputPath)) {
      writer.write(schema.toString(true));
    }
  }

  @Test
  public void register() throws IOException, MojoFailureException, MojoExecutionException {
    Map<String, Integer> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    int version = 1;
    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-Key", i);
      String valueSubject = String.format("TestSubject%03d-Value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      writeSchema(keySchemaFile, keySchema);
      writeSchema(valueSchemaFile, valueSchema);
      subjectToFile.put(keySubject, keySchemaFile);
      expectedVersions.put(keySubject, version++);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(valueSubject, version++);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();

    Assert.assertThat(this.mojo.schemaVersions, IsEqual.equalTo(expectedVersions));
  }

  void writeMalformedFile(File file) throws IOException {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("[");
    }
  }

  @Test(expected = IllegalStateException.class)
  public void malformedSchema() throws IOException, MojoFailureException, MojoExecutionException {
    Map<String, Integer> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    int version = 1;
    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-Key", i);
      String valueSubject = String.format("TestSubject%03d-Value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      if (i % 7 == 0) {
        writeMalformedFile(keySchemaFile);
        writeMalformedFile(valueSchemaFile);
      }
      subjectToFile.put(keySubject, keySchemaFile);
      expectedVersions.put(keySubject, version++);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(valueSubject, version++);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();

    Assert.assertThat(this.mojo.schemaVersions, IsEqual.equalTo(expectedVersions));
  }

  @Test(expected = IllegalStateException.class)
  public void missingSchemas() throws IOException, MojoFailureException, MojoExecutionException {
    Map<String, Integer> expectedVersions = new LinkedHashMap<>();

    Map<String, File> subjectToFile = new LinkedHashMap<>();
    int version = 1;
    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-Key", i);
      String valueSubject = String.format("TestSubject%03d-Value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      if (i % 7 == 0) {
        writeSchema(keySchemaFile, keySchema);
        writeSchema(valueSchemaFile, valueSchema);
      }
      subjectToFile.put(keySubject, keySchemaFile);
      expectedVersions.put(keySubject, version++);
      subjectToFile.put(valueSubject, valueSchemaFile);
      expectedVersions.put(valueSubject, version++);
    }

    this.mojo.subjects = subjectToFile;
    this.mojo.execute();

    Assert.assertThat(this.mojo.schemaVersions, IsEqual.equalTo(expectedVersions));
  }

  @After
  public void after() throws IOException {
    for (File tempFile : this.tempDirectory.listFiles()) {
      tempFile.delete();
    }
    this.tempDirectory.delete();
  }

}
