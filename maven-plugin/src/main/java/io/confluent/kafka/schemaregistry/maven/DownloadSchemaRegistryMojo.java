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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Mojo(name = "download")
public class DownloadSchemaRegistryMojo extends SchemaRegistryMojo {

  @Parameter(required = false)
  String schemaExtension;

  @Parameter(required = true)
  List<String> subjectPatterns = new ArrayList<>();

  @Parameter(required = true)
  File outputDirectory;

  Map<String, ParsedSchema> downloadSchemas(Collection<SubjectVersion> subjectVersions)
      throws MojoExecutionException {
    Map<String, ParsedSchema> results = new LinkedHashMap<>();

    for (SubjectVersion subjectVersion : subjectVersions) {
      SchemaMetadata schemaMetadata;
      try {
        if(subjectVersion.version == -1) {
            getLog().info(String.format("Downloading latest metadata for %s.", subjectVersion));
            schemaMetadata = this.client().getLatestSchemaMetadata(subjectVersion.subject);
        } else {
            getLog().info(String.format("Downloading latest metadata for %s.", subjectVersion));
            schemaMetadata = this.client().getSchemaMetadata(subjectVersion.subject, subjectVersion.version);
        }
        Optional<ParsedSchema> schema =
            this.client().parseSchema(
                schemaMetadata.getSchemaType(),
                schemaMetadata.getSchema(),
                schemaMetadata.getReferences());
        if (schema.isPresent()) {
          results.put(subjectVersion.subject, schema.get());
        } else {
          throw new MojoExecutionException(
              String.format("Error while parsing schema for %s", subjectVersion.subject)
          );
        }
      } catch (Exception ex) {
        throw new MojoExecutionException(
            String.format(
                    "Exception thrown while downloading metadata for %s with version %s.",
                    subjectVersion.subject,
                    (subjectVersion.version == -1) ? "latest" : subjectVersion.version
            ),
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

    Collection<String> allSubjects;
    try {
      getLog().info("Getting all subjects on schema registry...");
      allSubjects = this.client().getAllSubjects();
    } catch (Exception ex) {
      throw new MojoExecutionException("Exception thrown", ex);
    }

    Set<SubjectVersion> subjectsToDownload = new LinkedHashSet<>();
    for (String subject : allSubjects) {
      for (String subjectPattern : subjectPatterns) {
        int version = -1;
        if(subjectPattern.indexOf(":") != -1) {
          String[] patternAndVersion = subjectPattern.split(":");
          subjectPattern = patternAndVersion[0];
          version = Integer.valueOf(patternAndVersion[1]);
        }
        getLog().debug(String.format("Checking '%s' against pattern '%s'", subject, subjectPattern));
        if(subject.matches(subjectPattern)) {
          getLog().debug(String.format("'%s' matches pattern '%s' so downloading.", subject,
            subjectPattern));
          subjectsToDownload.add(new SubjectVersion(subject, version));
        }
      }
    }

    Map<String, ParsedSchema> subjectToSchema = downloadSchemas(subjectsToDownload);

    for (Map.Entry<String, ParsedSchema> kvp : subjectToSchema.entrySet()) {
      String fileName = String.format("%s%s", kvp.getKey(), getExtension(kvp.getValue()));
      File outputFile = new File(this.outputDirectory, fileName);

      getLog().info(
          String.format("Writing schema for Subject(%s) to %s.", kvp.getKey(), outputFile)
      );

      try (OutputStreamWriter writer = new OutputStreamWriter(
          new FileOutputStream(outputFile), StandardCharsets.UTF_8)
      ) {
        writer.write(kvp.getValue().toString());
      } catch (Exception ex) {
        throw new MojoExecutionException(
            String.format("Exception thrown while writing subject('%s') schema to %s", kvp.getKey(),
                          outputFile),
            ex
        );
      }
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

}
