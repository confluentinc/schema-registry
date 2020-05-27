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

  @Parameter(required = true)
  Map<String, File> subjects = new HashMap<>();

  @Parameter(required = false)
  Map<String, String> schemaTypes = new HashMap<>();

  @Parameter(required = false)
  Map<String, List<Reference>> references = new HashMap<>();

  Map<String, ParsedSchema> schemas = new HashMap<>();
  Map<String, Integer> schemaVersions = new HashMap<>();
  Set<String> subjectsProcessed = new HashSet<>();

  int errors = 0;
  int failures = 0;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    errors = 0;
    failures = 0;

    for (String subject : subjects.keySet()) {
      processSubject(subject);
    }

    Preconditions.checkState(errors == 0, "One or more exceptions were encountered.");
    Preconditions.checkState(failures == 0, failureMessage());
  }

  private void processSubject(String key) {
    if (subjectsProcessed.contains(key)) {
      return;
    }

    getLog().debug(String.format("Processing schema for subject(%s).", key));

    String schemaType = schemaTypes.getOrDefault(key, AvroSchema.TYPE);
    try {
      List<SchemaReference> schemaReferences = getReferences(key, schemaVersions);
      File file = subjects.get(key);
      if (file == null) {
        getLog().error("File for " + key + " could not be found.");
        errors++;
        return;
      }
      String schemaString = readFile(file, StandardCharsets.UTF_8);
      Optional<ParsedSchema> schema = client().parseSchema(
          schemaType, schemaString, schemaReferences);
      if (schema.isPresent()) {
        schemas.put(key, schema.get());
      } else {
        getLog().error("Schema for " + key + " could not be parsed.");
        errors++;
        return;
      }

      boolean success = processSchema(key, schema.get(), schemaVersions);
      if (!success) {
        failures++;
      }
    } catch (Exception ex) {
      getLog().error("Exception thrown while processing " + key, ex);
      errors++;
    } finally {
      subjectsProcessed.add(key);
    }
  }

  protected abstract boolean processSchema(String subject,
                                           ParsedSchema schema,
                                           Map<String, Integer> schemaVersions)
      throws IOException, RestClientException;


  protected String failureMessage() {
    return "Failed to process one or more schemas.";
  }

  private List<SchemaReference> getReferences(String subject, Map<String, Integer> schemaVersions) {
    List<Reference> refs = references.getOrDefault(subject, Collections.emptyList());
    List<SchemaReference> result = new ArrayList<>();
    for (Reference ref : refs) {
      // Process refs
      processSubject(ref.subject);

      Integer version = ref.version != null ? ref.version : schemaVersions.get(ref.subject);
      if (version == null) {
        getLog().warn(
            String.format("Version not specified for ref with name '%s' and subject '%s', "
                + "using latest version", ref.name, ref.subject));
        version = -1;
      }
      result.add(new SchemaReference(ref.name, ref.subject, version));
    }
    return result;
  }

  private static String readFile(File file, Charset encoding) throws IOException {
    byte[] encoded = Files.readAllBytes(file.toPath());
    return new String(encoded, encoding);
  }

}
