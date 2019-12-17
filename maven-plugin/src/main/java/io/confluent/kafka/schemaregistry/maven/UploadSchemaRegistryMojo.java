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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
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

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    int errors = 0;
    int failures = 0;

    for (Map.Entry<String, File> kvp : subjects.entrySet()) {
      getLog().debug(
          String.format(
              "Loading schema for subject(%s) from %s.",
              kvp.getKey(),
              kvp.getValue()
          )
      );

      String schemaType = schemaTypes.getOrDefault(kvp.getKey(), AvroSchema.TYPE);
      SchemaProvider schemaProvider = client().getSchemaProviders().get(schemaType);
      if (schemaProvider == null) {
        getLog().error("Invalid schema type " + schemaType);
        errors++;
        continue;
      }
      try {
        List<SchemaReference> schemaReferences = getReferences(kvp.getKey(), schemaVersions);
        String schemaString = readFile(kvp.getValue(), StandardCharsets.UTF_8);
        Optional<ParsedSchema> schema = schemaProvider.parseSchema(schemaString, schemaReferences);
        if (!schema.isPresent()) {
          throw new IllegalStateException("Schema for " + kvp.getKey() + " could not be loaded.");
        } else {
          schemas.put(kvp.getKey(), schema.get());
        }

        boolean success = processSchema(kvp.getKey(), schema.get(), schemaVersions);
        if (!success) {
          failures++;
        }
      } catch (IOException | RestClientException ex) {
        getLog().error("Exception thrown while processing " + kvp.getKey(), ex);
        errors++;
      } catch (IllegalArgumentException ex) {
        getLog().error("Exception thrown while retrieving reference " + kvp.getValue(), ex);
        errors++;
      }
    }

    Preconditions.checkState(errors == 0, "One or more exceptions were encountered.");
    Preconditions.checkState(failures == 0, failureMessage());
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
      Integer version = ref.version != null ? ref.version : schemaVersions.get(ref.subject);
      if (version == null) {
        throw new IllegalArgumentException("Could not retrieve version for subject " + ref.subject);
      }
      result.add(new SchemaReference(ref.name, ref.subject, version));
    }
    return result;
  }

  private static String readFile(File file, Charset encoding) throws IOException {
    byte[] encoded = Files.readAllBytes(file.toPath());
    return new String(encoded, encoding);
  }

  public static class Reference {

    public Reference(String name, String subject, Integer version) {
      this.name = name;
      this.subject = subject;
      this.version = version;
    }

    @Parameter(required = true)
    private String name;

    @Parameter(required = true)
    private String subject;

    @Parameter(required = false)
    private Integer version;

    @Override
    public String toString() {
      return "SchemaReference{"
          + "name='"
          + name
          + '\''
          + ", subject='"
          + subject
          + '\''
          + ", version="
          + version
          + '}';
    }
  }
}
