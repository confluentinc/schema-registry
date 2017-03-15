/*
 * Copyright 2015 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/compatibility")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class CompatibilityResource {

  private static final Logger log = LoggerFactory.getLogger(CompatibilityResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  public CompatibilityResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @POST
  @Path("/subjects/{subject}/versions/{version}")
  @PerformanceMetric("compatibility.subjects.versions.verify")
  public void lookUpSchemaUnderSubject(final @Suspended AsyncResponse asyncResponse,
                                       final @HeaderParam("Content-Type") String contentType,
                                       final @HeaderParam("Accept") String accept,
                                       @PathParam("subject") String subject,
                                       @PathParam("version") String version,
                                       @NotNull RegisterSchemaRequest request) {
    // returns true if posted schema is compatible with the specified version. "latest" is 
    // a special version
    Map<String, String> headerProperties = new HashMap<String, String>();
    headerProperties.put("Content-Type", contentType);
    headerProperties.put("Accept", accept);
    boolean isCompatible = false;
    CompatibilityCheckResponse compatibilityCheckResponse = new CompatibilityCheckResponse();
    String errorMessage = "Error while retrieving list of all subjects";
    try {
      if (!schemaRegistry.listSubjects().contains(subject)) {
        throw Errors.subjectNotFoundException();
      }
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    Schema schemaForSpecifiedVersion = null;
    VersionId versionId = null;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException();
    }
    try {
      schemaForSpecifiedVersion = schemaRegistry.get(subject, versionId.getVersionId());
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException();
    } catch (SchemaRegistryException e) {
      throw Errors.storeException("Error while retrieving schema for subject "
                                  + subject + " and version "
                                  + versionId.getVersionId(), e);
    }
    if (schemaForSpecifiedVersion == null) {
      if (versionId.isLatest()) {
        isCompatible = true;
        compatibilityCheckResponse.setIsCompatible(isCompatible);
        asyncResponse.resume(compatibilityCheckResponse);
      } else {
        throw Errors.versionNotFoundException();
      }
    } else {
      try {
        isCompatible = schemaRegistry
            .isCompatible(subject, request.getSchema(), schemaForSpecifiedVersion.getSchema());
      } catch (InvalidSchemaException e) {
        throw Errors.invalidAvroException("Invalid input schema " + request.getSchema(), e);
      } catch (SchemaRegistryStoreException e) {
        throw Errors.storeException(
            "Error while getting compatibility level for subject " + subject, e);
      } catch (SchemaRegistryException e) {
        throw Errors.schemaRegistryException(
            "Error while getting compatibility level for subject " + subject, e);
      }
      compatibilityCheckResponse.setIsCompatible(isCompatible);
      asyncResponse.resume(compatibilityCheckResponse);
    }
  }
}
