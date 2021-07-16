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

import com.google.common.base.Preconditions;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public abstract class UploadSchemaRegistryMojo extends SchemaRegistryMojo {

  public static final String PERCENT_REPLACEMENT = "_x";

  @Parameter(required = true)
  Map<String, File> subjects = new HashMap<>();

  @Parameter(required = false)
  Map<String, String> schemaTypes = new HashMap<>();

  @Parameter(required = false)
  Map<String, List<Reference>> references = new HashMap<>();

  @Parameter(required = false)
  boolean decodeSubject = true;

  Map<String, ParsedSchema> schemas = new HashMap<>();
  Map<String, Integer> schemaVersions = new HashMap<>();
  Set<String> subjectsProcessed = new HashSet<>();

  int errors = 0;
  int failures = 0;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (skip) {
      getLog().info("Plugin execution has been skipped");
      return;
    }

    errors = 0;
    failures = 0;

    for (String subject : subjects.keySet()) {
      processSubject(subject, false);
    }

    Preconditions.checkState(errors == 0, "One or more exceptions were encountered.");
    Preconditions.checkState(failures == 0, failureMessage());
  }

  private void processSubject(String key, boolean isReference) {
    if (subjectsProcessed.contains(key)) {
      return;
    }

    StringBuilder message = new StringBuilder(
            String.format("Processing schema for subject(%s).", key));
    if (isReference) {
      message.append("Obtained via references");
    }
    getLog().info(message.toString());

    try {
      //Use encoded subject for reference, subjects, schematype maps lookup
      //as xml tag cannot contain "/"
      String encodedSubject = encode(key);
      String schemaType = schemaTypes.getOrDefault(encodedSubject, AvroSchema.TYPE);
      List<SchemaReference> schemaReferences = getReferences(encodedSubject, schemaVersions);
      File file = subjects.get(encodedSubject);
      if (file == null) {
        if (!isReference) {
          getLog().error("File for " + encodedSubject + " could not be found.");
          errors++;
        }
        return;
      }
      String schemaString = readFile(file, StandardCharsets.UTF_8);
      Optional<ParsedSchema> schema = client().parseSchema(
          schemaType, schemaString, schemaReferences);
      if (schema.isPresent()) {
        schemas.put(encodedSubject, schema.get());
      } else {
        getLog().error("Schema for " + encodedSubject + " could not be parsed.");
        errors++;
        return;
      }

      //Decode the subject before registering it to schema registry
      String subject = decode(encodedSubject);
      boolean success = processSchema(subject, file, schema.get(), schemaVersions);
      if (!success) {
        failures++;
      }
      //processSchema() adds decoded subject as key and registered version to schemaVersions map.
      //Here we are just adding encoded subject as key and same registered version
      //as value to schemaVersions map.
      addOriginalKeyToSchemaVersions(key, subject);
    } catch (Exception ex) {
      getLog().error("Exception thrown while processing " + key, ex);
      errors++;
    } finally {
      subjectsProcessed.add(key);
    }
  }

  private void addOriginalKeyToSchemaVersions(String key, String subject) {
    //Add original encoded key to schemaVersions map.
    if (!schemaVersions.containsKey(key)) {
      Integer version = schemaVersions.get(subject);
      schemaVersions.put(key, version);
      getLog().info(
          String.format(
              "Marking subject(%s) as registered with version %s as it was "
              + "registered under subject(%s)",
              key,
              version,
              subject
          ));
    }
  }

  protected String decode(String encodedSubject) throws UnsupportedEncodingException {
    String subject = encodedSubject;
    //Default value of decodeSubject flag is true and we don't intend to
    //set it to false in our pom.xml
    if (decodeSubject && encodedSubject.contains(PERCENT_REPLACEMENT)) {
      // Replace _x with percent sign, since percent is not allowed in XML name
      subject = encodedSubject.replaceAll(PERCENT_REPLACEMENT, "%");
      subject = URLDecoder.decode(subject, "UTF-8");
      getLog().info(
          String.format(
              "key (%s) decoded to subject(%s)",
              encodedSubject,
              subject
          ));
    }
    return subject;
  }

  private String encode(String subject) throws UnsupportedEncodingException {
    String encodedSubject = subject;
    if (subject.contains("/")) {
      encodedSubject = URLEncoder.encode(subject, "UTF8");
      encodedSubject = encodedSubject.replaceAll("%", PERCENT_REPLACEMENT);
      getLog().info(
          String.format(
              "key (%s) encoded to subject(%s)",
              subject,
              encodedSubject
          ));
    }
    return encodedSubject;
  }

  protected abstract boolean processSchema(String subject,
                                           File schemaPath,
                                           ParsedSchema schema,
                                           Map<String, Integer> schemaVersions)
      throws IOException, RestClientException;


  protected String failureMessage() {
    return "Failed to process one or more schemas.";
  }

  private List<SchemaReference> getReferences(String subject, Map<String, Integer> schemaVersions)
          throws UnsupportedEncodingException {
    List<Reference> refs = references.getOrDefault(subject, Collections.emptyList());
    List<SchemaReference> result = new ArrayList<>();
    for (Reference ref : refs) {
      // Process refs
      processSubject(ref.subject, true);

      //Use decoded subject name as key for finding registered schema versions.
      String decodedRefSubject = decode(ref.subject);
      Integer version = ref.version != null ? ref.version : schemaVersions.get(decodedRefSubject);
      if (version == null) {
        getLog().warn(
            String.format(
                "Version not specified for ref with name '%s' and subject '%s"
                + " (aka decoded subject %s)', using latest version",
                ref.name,
                ref.subject,
                decodedRefSubject
        ));
        version = -1;
      }
      // Always pass decoded subject name in otherwise you will get exceptions.
      result.add(new SchemaReference(ref.name, decodedRefSubject, version));
    }
    return result;
  }

  private static String readFile(File file, Charset encoding) throws IOException {
    byte[] encoded = Files.readAllBytes(file.toPath());
    return new String(encoded, encoding);
  }

}
