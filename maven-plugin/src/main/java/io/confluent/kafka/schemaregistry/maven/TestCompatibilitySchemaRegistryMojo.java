/**
 * Copyright 2016 Confluent Inc.
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

import com.google.inject.internal.util.Preconditions;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Mojo(name = "test-compatibility")
public class TestCompatibilitySchemaRegistryMojo extends SchemaRegistryMojo {

  @Parameter(required = true)
  Map<String, File> subjects = new HashMap<>();

  Map<String, Boolean> schemaCompatibility;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    Map<String, Schema> subjectToSchemaLookup = loadSchemas(this.subjects);
    this.schemaCompatibility = new LinkedHashMap<>();

    int errorCount = 0;

    for (Map.Entry<String, Schema> kvp : subjectToSchemaLookup.entrySet()) {
      try {
        File schemaPath = this.subjects.get(kvp.getKey());

        if (getLog().isDebugEnabled()) {
          getLog().debug(
              String.format("Calling register('%s', '%s')", kvp.getKey(),
                            kvp.getValue().toString(true))
          );
        }

        boolean compatible = this.client().testCompatibility(kvp.getKey(), kvp.getValue());

        if (compatible) {
          getLog().info(
              String.format(
                  "Schema %s is compatible with subject(%s)",
                  schemaPath,
                  kvp.getKey()
              )
          );
        } else {
          getLog().error(
              String.format(
                  "Schema %s is not compatible with subject(%s)",
                  schemaPath,
                  kvp.getKey()
              )
          );
          errorCount++;
        }

        this.schemaCompatibility.put(kvp.getKey(), compatible);
      } catch (IOException | RestClientException e) {
        getLog().error(
            String.format("Exception thrown while registering subject(%s)", kvp.getKey()),
            e
        );
      }
    }

    Preconditions.checkState(errorCount == 0,
                             "One or more schema was found to be incompatible with the current "
                             + "version.");
  }
}
