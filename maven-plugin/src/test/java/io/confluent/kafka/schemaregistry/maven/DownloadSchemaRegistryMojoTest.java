package io.confluent.kafka.schemaregistry.maven;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DownloadSchemaRegistryMojoTest {
  File tempDirectory;
  DownloadSchemaRegistryMojo mojo;

  @Before
  public void before() throws IOException {
    this.tempDirectory = File.createTempFile(this.getClass().getSimpleName(), "tmp");
    this.tempDirectory.delete();
    this.tempDirectory.mkdirs();
    this.mojo = new DownloadSchemaRegistryMojo();
    this.mojo.client(new MockSchemaRegistryClient());
  }

  @Test
  public void download() throws IOException, RestClientException {
    int version = 1;

    List<File> files = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-Key", i);
      String valueSubject = String.format("TestSubject%03d-Value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      this.mojo.client().register(keySubject, keySchema);
      this.mojo.client().register(valueSubject, valueSchema);
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      files.add(keySchemaFile);
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");
      files.add(valueSchemaFile);
    }
  }

  @Test
  public void specificSubjects() throws IOException, RestClientException {
    int version = 1;

    List<File> files = new ArrayList<>();
    this.mojo.subjectPatterns.clear();

    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-Key", i);
      String valueSubject = String.format("TestSubject%03d-Value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      this.mojo.client().register(keySubject, keySchema);
      this.mojo.client().register(valueSubject, valueSchema);
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");

      if (i % 10 == 0) {
        String subjectPattern = String.format("^TestSubject%03d-(Key|Value)$", i);
        files.add(keySchemaFile);
        files.add(valueSchemaFile);
        this.mojo.subjectPatterns.add(subjectPattern);
      }
    }
  }

  @After
  public void after() throws IOException {
    for (File tempFile : this.tempDirectory.listFiles()) {
      tempFile.delete();
    }
    this.tempDirectory.delete();
  }

}
