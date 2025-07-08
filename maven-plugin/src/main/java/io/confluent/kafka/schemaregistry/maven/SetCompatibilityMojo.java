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

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Mojo(name = "set-compatibility",  configurator = "custom-basic")
public class SetCompatibilityMojo extends SchemaRegistryMojo {

  @Parameter(required = true)
  Map<String, String> compatibilityLevels = new HashMap<>();

  public void execute() throws MojoExecutionException {
    if (skip) {
      getLog().info("Plugin execution has been skipped");
      return;
    }

    for (Map.Entry<String, String> entry : compatibilityLevels.entrySet()) {
      if (entry.getValue().equalsIgnoreCase("null")) {
        deleteConfig(entry.getKey());
      } else {
        updateConfig(entry.getKey(), CompatibilityLevel.valueOf(entry.getValue()));
      }
    }
  }

  public void updateConfig(String subject, CompatibilityLevel compatibility)
      throws MojoExecutionException {

    try {
      String updatedCompatibility;

      if (subject.equalsIgnoreCase("null") || subject.equals("__GLOBAL")) {
        updatedCompatibility = this.client().updateCompatibility(null, compatibility.toString());
        getLog().info("Global Compatibility set to "
            + updatedCompatibility);
      } else {
        Collection<String> allSubjects = this.client().getAllSubjects();
        if (!allSubjects.contains(subject)) {
          throw new MojoExecutionException(
              "Subject not found"
          );
        }
        updatedCompatibility = this.client().updateCompatibility(subject, compatibility.toString());
        getLog().info("Compatibility of " + subject
            + " set to " + updatedCompatibility);
      }
    } catch (RestClientException | IOException e) {
      e.printStackTrace();
      throw new MojoExecutionException(
          "Exception thrown while updating config",
          e
      );
    }

  }

  public void deleteConfig(String subject) throws MojoExecutionException {
    if (getLog().isDebugEnabled()) {
      getLog().info("Deleting compatibility");
    }
    try {
      this.client().deleteCompatibility(subject);
      if (subject.equalsIgnoreCase("null") || subject.equals("__GLOBAL")) {
        getLog().info("Deleted global compatibility");
      } else {
        getLog().info(String.format("Deleted compatibility of %s", subject));
      }

    } catch (IOException | RestClientException e) {
      throw new MojoExecutionException(
          "Exception thrown while updating config",
          e
      );
    }
  }

  public String getConfig(String subject) throws MojoExecutionException {
    if (getLog().isDebugEnabled()) {
      getLog().info(String.format("Getting compatibility of %s", subject));
    }
    try {
      return String.format(this.client().getCompatibility(subject));
    } catch (IOException | RestClientException e) {
      e.printStackTrace();
      throw new MojoExecutionException(
          "Exception thrown while getting config",
          e
      );
    }
  }
}