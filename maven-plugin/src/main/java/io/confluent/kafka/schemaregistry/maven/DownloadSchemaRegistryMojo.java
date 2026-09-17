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
  private static final String LOGICAL_FORMAT = "logical";

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

  /**
   * Renders the downloaded schemas in this format, which the registry interprets -- {@code
   * logical} emits logical types DDL, and a schema type may offer others. What the registry
   * returns is written as-is, since re-reading it locally would undo the rendering.
   */
  @Parameter(required = false)
  String format;

  Map<String, SchemaMetadata> downloadSchemas(
      List<String> subjects, List<String> versionsToDownload)
      throws MojoExecutionException {
    Map<String, SchemaMetadata> results = new LinkedHashMap<>();

    if (versionsToDownload.size() != subjects.size()) {
      throw new MojoExecutionException("Number of versions specified should "
          + "be same as number of subjects");
    }
    for (int i = 0; i < subjects.size(); i++) {
      SchemaMetadata schemaMetadata;
      try {
        getLog().info(String.format("Downloading metadata "
            + "for %s.for version %s", subjects.get(i), versionsToDownload.get(i)));
        // Only ask for a rendering when one was requested, so that a client which does not
        // implement the format-aware reads still serves an ordinary download.
        schemaMetadata = isFormatted()
            ? this.client().getLatestSchemaMetadata(subjects.get(i), format)
            : this.client().getLatestSchemaMetadata(subjects.get(i));
        if (!versionsToDownload.get(i).equalsIgnoreCase("latest")) {
          Integer maxVersion = schemaMetadata.getVersion();
          if (maxVersion < Integer.parseInt(versionsToDownload.get(i))) {
            throw new MojoExecutionException(
                String.format("Max possible version "
                    + "for %s is %d", subjects.get(i), maxVersion));
          } else {
            int version = Integer.parseInt(versionsToDownload.get(i));
            schemaMetadata = isFormatted()
                ? this.client().getSchemaMetadata(subjects.get(i), version, format)
                : this.client().getSchemaMetadata(subjects.get(i), version);
          }
        }
        results.put(subjects.get(i), schemaMetadata);
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
    Map<String, SchemaMetadata> subjectToSchema =
        downloadSchemas(subjectsToDownload, versionsToDownload);

    for (Map.Entry<String, SchemaMetadata> kvp : subjectToSchema.entrySet()) {
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
        writer.write(schemaBody(kvp.getValue()));
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

  /**
   * Returns what the registry sent, unless nothing was asked of it: with no format the schema is
   * read back through the providers as before, which also keeps the written form exactly what
   * earlier versions wrote. Reading a rendered schema that way would undo the rendering -- the
   * providers convert logical types DDL back to its native form.
   */
  private String schemaBody(SchemaMetadata schemaMetadata) throws MojoExecutionException {
    if (isFormatted()) {
      return schemaMetadata.getSchema();
    }
    Optional<ParsedSchema> schema = this.client().parseSchema(new Schema(null, schemaMetadata));
    if (!schema.isPresent()) {
      throw new MojoExecutionException(
          String.format("Error while parsing schema %s", schemaMetadata.getSchema()));
    }
    return schema.get().toString();
  }

  private String getExtension(SchemaMetadata schemaMetadata) {
    if (this.schemaExtension != null) {
      return schemaExtension;
    }
    // A rendered schema is no longer in its own format, so the schema type does not name it.
    if (LOGICAL_FORMAT.equalsIgnoreCase(format)) {
      return ".ddl";
    }
    switch (schemaMetadata.getSchemaType()) {
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

  private boolean isFormatted() {
    return format != null && !format.trim().isEmpty();
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