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
import java.net.URLDecoder;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    if (decodeSubject) {
      subjects = decode(subjects);
      schemaTypes = decode(schemaTypes);
      references = decode(references);
    }

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

    getLog().debug(String.format("Processing schema for subject(%s).", key));

    String schemaType = schemaTypes.getOrDefault(key, AvroSchema.TYPE);
    try {
      List<SchemaReference> schemaReferences = getReferences(key, schemaVersions);
      File file = subjects.get(key);
      if (file == null) {
        if (!isReference) {
          getLog().error("File for " + key + " could not be found.");
          errors++;
        }
        return;
      }
      String schemaString = MojoUtils.readFile(file, StandardCharsets.UTF_8);
      Optional<ParsedSchema> schema = client().parseSchema(
          schemaType, schemaString, schemaReferences);
      if (schema.isPresent()) {
        schemas.put(key, schema.get());
      } else {
        getLog().error("Schema for " + key + " could not be parsed.");
        errors++;
        return;
      }

      boolean success = processSchema(key, file, schema.get(), schemaVersions);
      if (!success) {
        failures++;
      }
      close();
    } catch (Exception ex) {
      getLog().error("Exception thrown while processing " + key, ex);
      errors++;
    } finally {
      subjectsProcessed.add(key);
    }
  }

  protected static <V> Map<String, V> decode(Map<String, V> map) {
    return map.entrySet().stream()
        .collect(Collectors.toMap(
            e -> e.getKey().contains(PERCENT_REPLACEMENT) ? decode(e.getKey()) : e.getKey(),
            Entry::getValue));
  }

  protected static String decode(String subject) {
    try {
      // Replace _x colon with percent sign, since percent is not allowed in XML name
      String newSubject = subject.replaceAll(PERCENT_REPLACEMENT, "%");
      return URLDecoder.decode(newSubject, "UTF-8");
    } catch (Exception e) {
      try {
        // Try just replacing _x2F (slash), and ignore other occurrences of _x
        String newSubject = subject.replaceAll(PERCENT_REPLACEMENT + "2F", "%2F");
        return URLDecoder.decode(newSubject, "UTF-8");
      } catch (Exception e2) {
        // Not a URL encoded string, return original subject
        return subject;
      }
    }
  }

  protected abstract boolean processSchema(String subject,
                                           File schemaPath,
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
      processSubject(ref.subject, true);

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


}
