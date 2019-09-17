/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest.resources;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
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
  @ApiOperation(value = "Test input schema against a particular version of a subject's schema for "
      + "compatibility.",
      notes = "the compatibility level applied for the check is the configured compatibility level "
          + "for the subject (http:get:: /config/(string: subject)). If this subject's "
          + "compatibility level was never changed, then the global compatibility level "
          + "applies (http:get:: /config).",
      response = CompatibilityCheckResponse.class
  )
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "Error code 40401 -- Subject not found\n"
          + "Error code 40402 -- Version not found"),
      @ApiResponse(code = 422, message = "Error code 42201 -- Invalid Avro schema\n"
          + "Error code 42202 -- Invalid version"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store") })
  @PerformanceMetric("compatibility.subjects.versions.verify")
  public void testCompatabilityBySubjectName(
      final @Suspended AsyncResponse asyncResponse,
      final @HeaderParam("Content-Type") String contentType,
      final @HeaderParam("Accept") String accept,
      @ApiParam(value = "Subject of the schema version against which compatibility is to be tested",
          required = true)@PathParam("subject") String subject,
      @ApiParam(value = "Version of the subject's schema against which compatibility is to be "
          + "tested. Valid values for versionId are between [1,2^31-1] or the string \"latest\"."
          + "\"latest\" checks compatibility of the input schema with the last registered schema "
          + "under the specified subject", required = true)@PathParam("version") String version,
      @ApiParam(value = "Schema", required = true)
      @NotNull RegisterSchemaRequest request) {
    // returns true if posted schema is compatible with the specified version. "latest" is 
    // a special version
    Map<String, String> headerProperties = new HashMap<String, String>();
    headerProperties.put("Content-Type", contentType);
    headerProperties.put("Accept", accept);
    boolean isCompatible = false;
    CompatibilityCheckResponse compatibilityCheckResponse = new CompatibilityCheckResponse();
    String errorMessage = "Error while retrieving list of all subjects";
    Schema schemaForSpecifiedVersion = null;
    VersionId versionId = parseVersionId(version);
    try {
      //Don't check compatibility against deleted schema
      schemaForSpecifiedVersion = schemaRegistry.get(subject, versionId.getVersionId(), false);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException();
    } catch (SchemaRegistryException e) {
      throw Errors.storeException("Error while retrieving schema for subject "
                                  + subject + " and version "
                                  + versionId.getVersionId(), e);
    }
    registerWithError(subject, errorMessage);
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

  private static VersionId parseVersionId(String version) {
    final VersionId versionId;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException();
    }
    return versionId;
  }

  private void registerWithError(final String subject, final String errorMessage) {
    try {
      if (!schemaRegistry.hasSubjects(subject)) {
        throw Errors.subjectNotFoundException();
      }
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
  }
}
