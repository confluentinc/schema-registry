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
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.File;
import java.util.Optional;
import org.apache.maven.plugins.annotations.Mojo;

import java.io.IOException;
import java.util.Map;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "register", configurator = "custom-basic")
public class RegisterSchemaRegistryMojo extends UploadSchemaRegistryMojo {

  @Parameter(required = false)
  boolean normalizeSchemas = false;

  @Parameter(required = false)
  boolean propagateSchemaTags = false;

  @Override
  protected boolean processSchema(String subject,
                                  File schemaPath,
                                  RegisterSchemaRequest request,
                                  Map<String, Integer> schemaVersions)
      throws IOException, RestClientException {

    if (getLog().isDebugEnabled()) {
      getLog().debug(
          String.format("Calling register('%s', '%s')", subject, request.getSchema())
      );
    }

    if (propagateSchemaTags) {
      request.setPropagateSchemaTags(true);
    }
    RegisterSchemaResponse response =
        this.client().registerWithRequestResponse(subject, request, normalizeSchemas);
    Integer id = response.getId();
    Integer version = resolveVersion(subject, request, response);
    getLog().info(
        String.format(
            "Registered subject(%s) with id %s version %s",
            subject,
            id,
            version
        ));
    if (version != null) {
      schemaVersions.put(subject, version);
    }
    return true;
  }

  /**
   * Resolves the registered version, preferring the one the response carries. A registry before
   * CP 8.0 leaves it unset, so the version is looked up instead -- by the schema the response
   * echoes when there is one, and otherwise by re-sending the request, which also covers a
   * registry that returns nothing but an id.
   *
   * <p>Leaving it unresolved would not fail the registration, but another subject referencing
   * this one without an explicit version would fall back to {@code -1}, binding to whatever is
   * latest rather than to what was just registered.
   */
  private Integer resolveVersion(
      String subject, RegisterSchemaRequest request, RegisterSchemaResponse response)
      throws IOException, RestClientException {
    if (response.getVersion() != null && response.getVersion() > 0) {
      return response.getVersion();
    }
    if (response.getSchema() != null) {
      Optional<ParsedSchema> schema = this.client().parseSchema(new Schema(subject, response));
      if (schema.isPresent()) {
        return this.client().getVersion(subject, schema.get(), normalizeSchemas);
      }
    }
    Integer version = this.client()
        .getIdWithRequestResponse(subject, request, normalizeSchemas)
        .getVersion();
    if (version == null) {
      // Leaves this subject out of schemaVersions, so a dependent subject referencing it without
      // an explicit version falls back to latest. That fallback warns on its own, but says nothing
      // about why the version was unknown.
      getLog().warn(String.format("Could not determine the registered version of subject(%s)",
          subject));
    }
    return version;
  }
}
