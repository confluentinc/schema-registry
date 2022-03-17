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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
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
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.rest.annotations.PerformanceMetric;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.QueryParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
  @Operation(summary = "Test schema compatibility against a particular schema subject-version",
      description =
          "Test input schema against a particular version of a subject's schema for compatibility. "
              + "The compatibility level applied for the check is the configured compatibility "
              + "level for the subject (http:get:: /config/(string: subject)). If this subject's "
              + "compatibility level was never changed, then the global compatibility level "
              + "applies (http:get:: /config).",
      responses = {
          @ApiResponse(responseCode = "200", description = "Compatibility check result",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
              CompatibilityCheckResponse.class))),
          @ApiResponse(responseCode = "404", description = "Error code 40401 -- Subject not found\n"
              + "Error code 40402 -- Version not found"),
          @ApiResponse(responseCode = "422", description =
              "Error code 42201 -- Invalid schema or schema type\n"
                  + "Error code 42202 -- Invalid version"),
          @ApiResponse(responseCode = "500", description = "Error code 50001 -- Error in the "
              + "backend data store")
      })
  @PerformanceMetric("compatibility.subjects.versions.verify")
  public void testCompatibilityBySubjectName(
      final @Suspended AsyncResponse asyncResponse,
      @Parameter(description = "Subject of the schema version against which compatibility is to "
          + "be tested",
          required = true) @PathParam("subject") String subject,
      @Parameter(description =
          "Version of the subject's schema against which compatibility is to be "
             + "tested. Valid values for versionId are between [1,2^31-1] or the string "
             + "\"latest\"."
             + "\"latest\" checks compatibility of the input schema with the last registered "
             + "schema "
             + "under the specified subject", required = true) @PathParam("version") String version,
      @Parameter(description = "Schema", required = true)
      @NotNull RegisterSchemaRequest request,
      @Parameter(description = "Whether to return detailed error messages")
      @QueryParam("verbose") boolean verbose) {
    log.info("Testing schema subject {} compatibility between existing version {} and "
             + "specified version {}, id {}, type {}",
             subject, version, request.getVersion(), request.getId(), request.getSchemaType());

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    // returns true if posted schema is compatible with the specified version. "latest" is
    // a special version
    List<String> errorMessages;
    VersionId versionId = parseVersionId(version);
    Schema schemaForSpecifiedVersion;
    try {
      //Don't check compatibility against deleted schema
      schemaForSpecifiedVersion = schemaRegistry.get(subject, versionId.getVersionId(), false);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.storeException("Error while retrieving schema for subject "
                                  + subject + " and version "
                                  + versionId.getVersionId(), e);
    }
    if (schemaForSpecifiedVersion == null && !versionId.isLatest()) {
      throw Errors.versionNotFoundException(versionId.getVersionId());
    }
    Schema schema = new Schema(
        subject,
        0,
        -1,
        request.getSchemaType() != null ? request.getSchemaType() : AvroSchema.TYPE,
        request.getReferences(),
        request.getSchema()
    );
    try {
      errorMessages = schemaRegistry.isCompatible(
          subject, schema,
          schemaForSpecifiedVersion != null
              ? Collections.singletonList(schemaForSpecifiedVersion)
              : Collections.emptyList()
      );
    } catch (InvalidSchemaException e) {
      if (verbose) {
        errorMessages = Collections.singletonList(e.getMessage());
      } else {
        throw Errors.invalidSchemaException(e);
      }
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(
          "Error while getting compatibility level for subject " + subject, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while getting compatibility level for subject " + subject, e);
    }

    CompatibilityCheckResponse compatibilityCheckResponse =
            createCompatiblityCheckResponse(errorMessages, verbose);
    asyncResponse.resume(compatibilityCheckResponse);
  }

  @POST
  @Path("/subjects/{subject}/versions")
  @Operation(summary = "Test schema compatibility against all schemas under a subject",
      description =
          "Test input schema against a subject's schemas for compatibility, "
              + "based on the configured compatibility level of the subject. In other words, "
              + "it will perform the same compatibility check as register for that subject. "
              + "The compatibility level applied for the check is the configured compatibility "
              + "level for the subject (http:get:: /config/(string: subject)). If this subject's "
              + "compatibility level was never changed, then the global compatibility level "
              + "applies (http:get:: /config).",
      responses = {
          @ApiResponse(responseCode = "200", description = "Compatibility check result",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
              CompatibilityCheckResponse.class))),
          @ApiResponse(responseCode = "422", description =
              "Error code 42201 -- Invalid schema or schema type\n"
                  + "Error code 42202 -- Invalid version"),
          @ApiResponse(responseCode = "500", description = "Error code 50001 -- Error in the "
              + "backend data store")
      })

  @PerformanceMetric("compatibility.subjects.versions.verify")
  public void testCompatibilityForSubject(
      final @Suspended AsyncResponse asyncResponse,
      @Parameter(description = "Subject of the schema version against which compatibility is to "
          + "be tested",
          required = true) @PathParam("subject") String subject,
      @Parameter(description = "Schema", required = true)
      @NotNull RegisterSchemaRequest request,
      @Parameter(description = "Whether to return detailed error messages")
      @QueryParam("verbose") boolean verbose) {
    log.info("Testing schema subject {} compatibility with specified version {}, id {}, type {}",
        subject, request.getVersion(), request.getId(), request.getSchemaType());

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    // returns true if posted schema is compatible with the specified subject.
    List<String> errorMessages;
    List<Schema> previousSchemas = new ArrayList<>();
    try {
      //Don't check compatibility against deleted schema
      schemaRegistry.getAllVersions(subject, false)
          .forEachRemaining(previousSchemas::add);
    } catch (SchemaRegistryException e) {
      throw Errors.storeException("Error while retrieving schema for subject "
          + subject, e);
    }
    Schema schema = new Schema(
        subject,
        0,
        -1,
        request.getSchemaType() != null ? request.getSchemaType() : AvroSchema.TYPE,
        request.getReferences(),
        request.getSchema()
    );
    try {
      errorMessages = schemaRegistry.isCompatible(subject, schema, previousSchemas);
    } catch (InvalidSchemaException e) {
      if (verbose) {
        errorMessages = Collections.singletonList(e.getMessage());
      } else {
        throw Errors.invalidSchemaException(e);
      }
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(
          "Error while getting compatibility level for subject " + subject, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while getting compatibility level for subject " + subject, e);
    }

    CompatibilityCheckResponse compatibilityCheckResponse =
        createCompatiblityCheckResponse(errorMessages, verbose);
    asyncResponse.resume(compatibilityCheckResponse);
  }

  private static CompatibilityCheckResponse createCompatiblityCheckResponse(
          List<String> errorMessages,
          boolean verbose) {
    CompatibilityCheckResponse compatibilityCheckResponse = new CompatibilityCheckResponse();
    compatibilityCheckResponse.setIsCompatible(errorMessages.isEmpty());
    if (verbose) {
      compatibilityCheckResponse.setMessages(errorMessages);
    }
    return compatibilityCheckResponse;
  }

  private static VersionId parseVersionId(String version) {
    final VersionId versionId;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }
    return versionId;
  }
}
