/*
 * Copyright 2016-2025 Confluent Inc.
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
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Mojo(name = "test-compatibility", configurator = "custom-basic")
public class TestCompatibilitySchemaRegistryMojo extends UploadSchemaRegistryMojo {

  @Parameter(required = false)
  boolean verbose = true;

  Map<String, Boolean> schemaCompatibility = new HashMap<>();

  @Override
  protected boolean processSchema(String subject,
                                  File schemaPath,
                                  ParsedSchema schema,
                                  Map<String, Integer> schemaVersions)
      throws IOException, RestClientException {

    if (getLog().isDebugEnabled()) {
      getLog().debug(
          String.format("Calling testCompatibility('%s', '%s')", subject, schema)
      );
    }

    List<String> errorMessages = this.client().testCompatibilityVerbose(subject, schema);
    boolean compatible = errorMessages.isEmpty();

    if (compatible) {
      getLog().info(
          String.format(
              "Schema %s is compatible with subject(%s)",
              schemaPath,
              subject
          )
      );
    } else {
      String errorLog = String.format(
          "Schema %s is not compatible with subject(%s)", schemaPath, subject);
      if (verbose) {
        errorLog += " with error " + errorMessages.toString();
      }
      getLog().error(errorLog);
    }

    this.schemaCompatibility.put(subject, compatible);
    return compatible;
  }

  @Override
  protected String failureMessage() {
    return "One or more schemas found to be incompatible with the current version.";
  }
}
