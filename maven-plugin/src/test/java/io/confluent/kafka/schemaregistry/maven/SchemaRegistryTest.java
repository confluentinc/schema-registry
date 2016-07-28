package io.confluent.kafka.schemaregistry.maven;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SchemaRegistryTest {
  protected File tempDirectory;

  @Before
  public void createTempDirectory() throws IOException {
    this.tempDirectory = File.createTempFile(this.getClass().getSimpleName(), "tmp");
    this.tempDirectory.delete();
    this.tempDirectory.mkdirs();
  }

  protected void writeSchema(File outputPath, Schema schema) throws IOException {
    try (FileWriter writer = new FileWriter(outputPath)) {
      writer.write(schema.toString(true));
    }
  }

  protected void writeMalformedFile(File file) throws IOException {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("[");
    }
  }

  @After
  public void cleanupTempDirectory() throws IOException {
    for (File tempFile : this.tempDirectory.listFiles()) {
      tempFile.delete();
    }
    this.tempDirectory.delete();
  }
}
