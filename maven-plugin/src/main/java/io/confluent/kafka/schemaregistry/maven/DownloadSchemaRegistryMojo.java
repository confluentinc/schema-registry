/**
 * ` * Copyright 2016 Confluent Inc.
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
 **/

package io.confluent.kafka.schemaregistry.maven;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Mojo(name = "download")
public class DownloadSchemaRegistryMojo extends SchemaRegistryMojo {

  @Parameter(required = false, defaultValue = ".avsc")
  String schemaExtension;
  @Parameter(required = true)
  List<String> subjectPatterns = new ArrayList<>();
  @Parameter(required = true)
  File outputDirectory;

  @Parameter(required = false, defaultValue = "true")
  boolean prettyPrintSchemas;

  Map<String, Schema> downloadSchemas(Collection<String> subjects) throws MojoExecutionException {
    Map<String, Schema> results = new LinkedHashMap<>();

    for (String subject : subjects) {
      Schema.Parser parser = new Schema.Parser();
      SchemaMetadata schemaMetadata;
      try {
        getLog().info(String.format("Downloading latest metadata for %s.", subject));
        schemaMetadata = this.client().getLatestSchemaMetadata(subject);
        Schema schema = parser.parse(schemaMetadata.getSchema());
        results.put(subject, schema);
      } catch (RestClientException | IOException ex) {
        throw new MojoExecutionException(
            String.format("Exception thrown while downloading metadata for %s.", subject),
            ex
        );
      } catch (SchemaParseException ex) {
        throw new MojoExecutionException(
            String.format("Exception thrown while parsing avro schema for %s.", subject),
            ex
        );
      }
    }

    return results;
  }

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    try {
      getLog().debug(
          String.format("Checking if '%s' exists and is not a directory.", this.outputDirectory));
      if (outputDirectory.exists() && !outputDirectory.isDirectory()) {
        throw new IllegalStateException("outputDirectory must be a directory");
      }
      getLog()
          .debug(String.format("Checking if outputDirectory('%s') exists.", this.outputDirectory));
      if (!outputDirectory.isDirectory()) {
        getLog().debug(String.format("Creating outputDirectory('%s').", this.outputDirectory));
        if (!outputDirectory.mkdirs()) {
          throw new IllegalStateException(
              "Could not create output directory " + this.outputDirectory);
        }
      }
    } catch (Exception ex) {
      throw new MojoExecutionException("Exception thrown while creating outputDirectory", ex);
    }

    List<Pattern> patterns = new ArrayList<>();

    for (String subject : subjectPatterns) {
      try {
        getLog().debug(String.format("Creating pattern for '%s'", subject));
        Pattern pattern = Pattern.compile(subject);
        patterns.add(pattern);
      } catch (Exception ex) {
        throw new IllegalStateException(
            String.format("Exception thrown while creating pattern '%s'", subject),
            ex
        );
      }
    }

    Collection<String> allSubjects;
    try {
      getLog().info("Getting all subjects on schema registry...");
      allSubjects = this.client().getAllSubjects();
    } catch (Exception ex) {
      throw new MojoExecutionException("Exception thrown", ex);
    }

    getLog().info(String.format("Schema Registry has %s subject(s).", allSubjects.size()));
    Set<String> subjectsToDownload = new LinkedHashSet<>();

    for (String subject : allSubjects) {
      for (Pattern pattern : patterns) {
        getLog()
            .debug(String.format("Checking '%s' against pattern '%s'", subject, pattern.pattern()));
        Matcher matcher = pattern.matcher(subject);

        if (matcher.matches()) {
          getLog().debug(String.format("'%s' matches pattern '%s' so downloading.", subject,
                                       pattern.pattern()));
          subjectsToDownload.add(subject);
          break;
        }
      }
    }

    Map<String, Schema> subjectToSchema = downloadSchemas(subjectsToDownload);

    for (Map.Entry<String, Schema> kvp : subjectToSchema.entrySet()) {
      String fileName = String.format("%s%s", kvp.getKey(), this.schemaExtension);
      File outputFile = new File(this.outputDirectory, fileName);

      getLog().info(
          String.format("Writing schema for Subject(%s) to %s.", kvp.getKey(), outputFile)
      );

      try (FileWriter writer = new FileWriter(outputFile)) {
        writer.write(kvp.getValue().toString(this.prettyPrintSchemas));
      } catch (IOException ex) {
        throw new MojoExecutionException(
            String.format("Exception thrown while writing subject('%s') schema to %s", kvp.getKey(),
                          outputFile),
            ex
        );
      }
    }
  }
}
