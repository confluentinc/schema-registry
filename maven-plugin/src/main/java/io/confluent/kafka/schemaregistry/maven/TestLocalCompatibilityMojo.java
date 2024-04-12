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

import io.confluent.kafka.schemaregistry.CompatibilityChecker;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "test-local-compatibility", configurator = "custom-basic")
public class TestLocalCompatibilityMojo extends AbstractMojo {

  @Parameter(required = true)
  Map<String, File> schemas = new HashMap<>();

  @Parameter(required = false)
  Map<String, String> schemaTypes = new HashMap<>();

  @Parameter(required = true)
  Map<String, File> previousSchemaPaths = new HashMap<>();

  @Parameter(required = true)
  Map<String, CompatibilityLevel> compatibilityLevels = new HashMap<>();

  protected Optional<ParsedSchema> parseSchema(
      String schemaType,
      String schemaString,
      List<SchemaReference> references,
      Map<String, SchemaProvider> providers) throws MojoExecutionException {

    SchemaProvider schemaProvider = providers.get(schemaType.toUpperCase());
    if (schemaProvider == null) {
      throw new MojoExecutionException(
          String.format("Invalid schema type %s", schemaType));
    }

    return schemaProvider.parseSchema(schemaString, references);

  }

  protected ParsedSchema loadSchema(File path, String schemaType,
      Map<String, SchemaProvider> schemaProviders) throws MojoExecutionException {

    String schemaString;
    try {
      schemaString = MojoUtils.readFile(path, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new MojoExecutionException(
          String.format("File cannot be found at: %s", path));
    }
    List<SchemaReference> references = new ArrayList<>();
    Optional<ParsedSchema> schema = parseSchema(schemaType, schemaString,
        references, schemaProviders);

    if (schema.isPresent()) {
      return schema.get();
    }

    throw new MojoExecutionException(String.format("Unable to parse schema from %s "
        + "with schema type as %s", path, schemaType));
  }

  protected ArrayList<File> getFiles(File previousSchemaPath) {

    ArrayList<File> previousSchemaFiles = new ArrayList<>();

    getLog().debug(String.format("Loading File %s", previousSchemaPath));
    // Add all files inside a directory, inside directories are skipped
    if (previousSchemaPath.isDirectory()) {

      File[] fileList = previousSchemaPath.listFiles();
      if (fileList == null) {
        return previousSchemaFiles;
      }

      for (File f : fileList) {
        if (!f.isDirectory()) {
          previousSchemaFiles.add(f);
        }
      }

    } else {
      previousSchemaFiles.add(previousSchemaPath);
    }

    return previousSchemaFiles;
  }


  protected void testSchema(String key, Map<String, SchemaProvider> schemaProviders)
      throws MojoExecutionException {

    File schemaPath = schemas.get(key);

    if (!previousSchemaPaths.containsKey(key)) {
      throw new MojoExecutionException(String.format("Previous schemas not found for %s", key));
    }

    File previousSchemaPath = previousSchemaPaths.get(key);
    String schemaType = schemaTypes.getOrDefault(key, AvroSchema.TYPE);

    if (!compatibilityLevels.containsKey(key)) {
      throw new MojoExecutionException(String.format("Compatibility Level not found for %s", key));
    }

    CompatibilityLevel compatibilityLevel = compatibilityLevels.get(key);

    ArrayList<File> previousSchemaFiles = getFiles(previousSchemaPath);

    if (previousSchemaFiles.size() > 1
        && (compatibilityLevel == CompatibilityLevel.BACKWARD
        || compatibilityLevel == CompatibilityLevel.FORWARD
        || compatibilityLevel == CompatibilityLevel.FULL)) {

      throw new MojoExecutionException(String.format("Provide exactly one file for %s check "
              + "for schema %s", compatibilityLevel.name.toLowerCase(), schemaPath));

    }

    ParsedSchema schema = loadSchema(schemaPath, schemaType, schemaProviders);
    ArrayList<ParsedSchema> previousSchemas = new ArrayList<>();

    for (File previousSchemaFile : previousSchemaFiles) {
      previousSchemas.add(loadSchema(previousSchemaFile, schemaType, schemaProviders));
    }

    CompatibilityChecker checker = CompatibilityChecker.checker(compatibilityLevel);
    List<String> errorMessages = checker.isCompatible(schema, previousSchemas);

    boolean success = errorMessages.isEmpty();

    if (success) {
      getLog().info(String.format("Schema is %s compatible with previous schemas",
          compatibilityLevel.name.toLowerCase()));
    } else {
      String errorLog = String.format("Schema is not %s compatible with previous schemas. ",
          compatibilityLevel.name.toLowerCase()) + errorMessages.get(0);
      throw new MojoExecutionException(errorLog);
    }

  }

  public void execute() throws MojoExecutionException {

    List<SchemaProvider> providers = MojoUtils.defaultSchemaProviders();
    Map<String, SchemaProvider> schemaProviders = providers.stream()
        .collect(Collectors.toMap(SchemaProvider::schemaType, p -> p));

    Set<String> keys = schemas.keySet();

    for (String key : keys) {
      testSchema(key, schemaProviders);
    }

  }

}
