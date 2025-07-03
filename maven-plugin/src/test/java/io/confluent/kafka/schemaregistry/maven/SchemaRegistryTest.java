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
