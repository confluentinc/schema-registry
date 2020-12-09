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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class DownloadSchemaRegistryMojoTest extends SchemaRegistryTest {
  DownloadSchemaRegistryMojo mojo;

  @Before
  public void createMojo() {
    this.mojo = new DownloadSchemaRegistryMojo();
    this.mojo.client(new MockSchemaRegistryClient());
  }

  @Test
  public void specificSubjects() throws IOException, RestClientException, MojoFailureException, MojoExecutionException {
    this.mojo.outputDirectory = this.tempDirectory;
    this.mojo.subjectPatterns.clear();

    List<File> filesToDownload = new ArrayList<>();
    List<File> filesNotToDownload = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      String keySubject = String.format("TestSubject%03d-key", i);
      String valueSubject = String.format("TestSubject%03d-value", i);
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
      this.mojo.client().register(keySubject, new AvroSchema(keySchema));
      this.mojo.client().register(valueSubject, new AvroSchema(valueSchema));
      File keySchemaFile = new File(this.tempDirectory, keySubject + ".avsc");
      File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");

      if (i % 10 == 0) {
        String subjectPattern = String.format("^TestSubject%03d-(key|value)$", i);
        filesToDownload.add(keySchemaFile);
        filesToDownload.add(valueSchemaFile);
        this.mojo.subjectPatterns.add(subjectPattern);
      } else {
        filesNotToDownload.add(keySchemaFile);
        filesNotToDownload.add(valueSchemaFile);
      }
    }

    this.mojo.execute();

    for (File file : filesToDownload) {
      Assert.assertThat(file.exists(), is(true));
    }
    for (File file : filesNotToDownload) {
      Assert.assertThat(file.exists(), is(false));
    }
  }

  @Test
  public void specificSubjectAndVersion() throws IOException, RestClientException, MojoFailureException, MojoExecutionException {
    this.mojo.outputDirectory = this.tempDirectory;
    this.mojo.subjectPatterns.clear();

    String valueSubject = "TestSubject-value";
    Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
    this.mojo.client().register(valueSubject, new AvroSchema(valueSchema), 1, 101);

    Schema valueSchemaUpdated = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL)));
    this.mojo.client().register(valueSubject, new AvroSchema(valueSchemaUpdated), 2, 102);

    File valueSchemaFile = new File(this.tempDirectory, valueSubject + ".avsc");

    String subjectPattern = "^TestSubject-(key|value)$:2";
    this.mojo.subjectPatterns.add(subjectPattern);

    this.mojo.execute();

    Assert.assertThat(valueSchemaFile.exists(), is(true));
    Assert.assertThat(new Schema.Parser().parse(valueSchemaFile), is(valueSchemaUpdated));
  }

  @Test(expected = MojoExecutionException.class)
  public void specificSubjectAndVersionNotFound() throws IOException, RestClientException, MojoFailureException, MojoExecutionException {
    this.mojo.outputDirectory = this.tempDirectory;
    this.mojo.subjectPatterns.clear();

    String valueSubject = "TestSubject-value";
    Schema valueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
    this.mojo.client().register(valueSubject, new AvroSchema(valueSchema), 1, 101);

    String subjectPattern = "^TestSubject-(key|value)$:9999";
    this.mojo.subjectPatterns.add(subjectPattern);

    this.mojo.execute();
  }

}
