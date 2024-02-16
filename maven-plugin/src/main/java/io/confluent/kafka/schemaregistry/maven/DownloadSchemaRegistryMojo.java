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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.net.URLEncoder;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Mojo(name = "download")
public class DownloadSchemaRegistryMojo extends SchemaRegistryMojo {

  public static final String PERCENT_REPLACEMENT = "_x";

  @Parameter(required = false)
  String schemaExtension;

  @Parameter(required = true)
  List<String> subjectPatterns = new ArrayList<>();

  @Parameter(required = false)
  List<String> versions = new ArrayList<>();

  @Parameter(required = true)
  File outputDirectory;

  @Parameter(required = false)
  boolean encodeSubject = true;

  Map<String, ParsedSchema> downloadSchemas(List<String> subjects, List<String> versionsToDownload)
      throws MojoExecutionException {
    Map<String, ParsedSchema> results = new LinkedHashMap<>();

    if (versionsToDownload.size() != subjects.size()) {
      throw new MojoExecutionException("Number of versions specified should "
          + "be same as number of subjects");
    }
    for (int i = 0; i < subjects.size(); i++) {
      SchemaMetadata schemaMetadata;
      try {
        getLog().info(String.format("Downloading metadata "
            + "for %s.for version %s", subjects.get(i), versionsToDownload.get(i)));
        schemaMetadata = this.client().getLatestSchemaMetadata(subjects.get(i));
        if (!versionsToDownload.get(i).equalsIgnoreCase("latest")) {
          Integer maxVersion = schemaMetadata.getVersion();
          if (maxVersion < Integer.parseInt(versionsToDownload.get(i))) {
            throw new MojoExecutionException(
                String.format("Max possible version "
                    + "for %s is %d", subjects.get(i), maxVersion));
          } else {
            schemaMetadata = this.client().getSchemaMetadata(subjects.get(i),
                Integer.parseInt(versionsToDownload.get(i)));
          }
        }
        Optional<ParsedSchema> schema = this.client().parseSchema(new Schema(null, schemaMetadata));
        if (schema.isPresent()) {
          results.put(subjects.get(i), schema.get());
        } else {
          throw new MojoExecutionException(
              String.format("Error while parsing schema for %s", subjects.get(i))
          );
        }
      } catch (Exception ex) {
        throw new MojoExecutionException(
            String.format("Exception thrown while downloading metadata for %s.", subjects.get(i)),
            ex
        );
      }
    }

    return results;
  }

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (skip) {
      getLog().info("Plugin execution has been skipped");
      return;
    }
    outputDirValidation();
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
    List<String> subjectsToDownload = new ArrayList<>();
    List<String> versionsToDownload = new ArrayList<>();

    if (!versions.isEmpty()) {
      if (versions.size() != subjectPatterns.size()) {
        throw new IllegalStateException("versions size should be same as subjectPatterns size");
      }
    }
    for (String subject : allSubjects) {
      for (int i = 0 ; i < patterns.size() ; i++) {
        getLog()
            .debug(String.format("Checking '%s' against pattern '%s'",
                subject, patterns.get(i).pattern()));
        Matcher matcher = patterns.get(i).matcher(subject);

        if (matcher.matches()) {
          getLog().debug(String.format("'%s' matches "
                  + "pattern '%s' so downloading.", subject,
                                       patterns.get(i).pattern()));
          if (versions.isEmpty()) {
            versionsToDownload.add("latest");
          } else {
            versionsToDownload.add(versions.get(i));
          }
          subjectsToDownload.add(subject);
          break;
        }
      }
    }
    Map<String, ParsedSchema> subjectToSchema =
        downloadSchemas(subjectsToDownload, versionsToDownload);

    for (Map.Entry<String, ParsedSchema> kvp : subjectToSchema.entrySet()) {
      String subject = kvp.getKey();
      String encodedSubject = encodeSubject ? encode(subject) : subject;
      String fileName = String.format("%s%s", encodedSubject, getExtension(kvp.getValue()));
      File outputFile = new File(this.outputDirectory, fileName);
      getLog().info(
          String.format("Writing schema for Subject(%s) to %s.", subject, outputFile)
      );

      try (OutputStreamWriter writer = new OutputStreamWriter(
          new FileOutputStream(outputFile), StandardCharsets.UTF_8)
      ) {
        writer.write(kvp.getValue().toString());
      } catch (Exception ex) {
        throw new MojoExecutionException(
            String.format("Exception thrown while writing subject('%s') schema to %s", subject,
                          outputFile),
            ex
        );
      }
    }
    try {
      close();
    } catch (IOException e) {
      throw new MojoExecutionException("Exception while closing schema registry client", e);
    }
  }

  public void outputDirValidation() throws MojoExecutionException, MojoFailureException {
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
  }

  private String getExtension(ParsedSchema parsedSchema) {
    if (this.schemaExtension != null) {
      return schemaExtension;
    }
    switch (parsedSchema.schemaType()) {
      case AvroSchema.TYPE:
        return ".avsc";
      case JsonSchema.TYPE:
        return ".schema.json";
      case ProtobufSchema.TYPE:
        return ".proto";
      default:
        return ".txt";
    }
  }

  protected String encode(String subject) {
    try {
      String newSubject = URLEncoder.encode(subject, "UTF-8");
      return newSubject.replaceAll("%", PERCENT_REPLACEMENT);
    } catch (Exception e) {
      getLog().warn(String.format("Could not encode subject '%s'", subject));
      return subject;
    }
  }
}