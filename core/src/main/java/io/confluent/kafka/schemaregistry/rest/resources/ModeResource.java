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

import com.google.common.base.CharMatcher;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Mode;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidModeException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import java.util.Locale;
import java.util.Map;

@Path("/mode")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class ModeResource {

  private static final Logger log = LoggerFactory.getLogger(ModeResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public ModeResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Path("/{subject}")
  @PUT
  @Operation(summary = "Update subject mode",
      description = "Update mode for the specified subject. "
        + "On success, echoes the original request back to the client.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The original request",
            content = @Content(schema = @Schema(implementation = ModeUpdateRequest.class))),
        @ApiResponse(responseCode = "422", description = "Error code 42204 -- Invalid mode\n"
            + "Error code 42205 -- Operation not permitted"),
        @ApiResponse(responseCode = "500",
            description = "Error code 50001 -- Error in the backend data store\n"
            + "Error code 50003 -- Error while forwarding the request to the primary\n"
            + "Error code 50004 -- Unknown leader")})
  public ModeUpdateRequest updateMode(
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Context HttpHeaders headers,
      @Parameter(description = "Update Request", required = true)
      @NotNull ModeUpdateRequest request,
      @Parameter(description =
          "Whether to force update if setting mode to IMPORT and schemas currently exist")
      @QueryParam("force") boolean force
  ) {

    if (subject != null && CharMatcher.javaIsoControl().matchesAnyOf(subject)) {
      throw Errors.invalidSubjectException(subject);
    }
    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    io.confluent.kafka.schemaregistry.storage.Mode mode;
    try {
      mode = Enum.valueOf(io.confluent.kafka.schemaregistry.storage.Mode.class,
          request.getMode().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new RestInvalidModeException();
    }
    try {
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
          headers, schemaRegistry.config().whitelistHeaders());
      schemaRegistry.setModeOrForward(subject, mode, force, headerProperties);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to update mode", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Failed to update mode", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding update mode request"
                                                    + " to the leader", e);
    }

    return request;
  }

  @Path("/{subject}")
  @GET
  @Operation(summary = "Get subject mode",
      description = "Retrieves the subject mode.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The subject mode",
            content = @Content(schema = @Schema(implementation = Mode.class))),
        @ApiResponse(responseCode = "404", description = "Subject not found"),
        @ApiResponse(responseCode = "500",
            description = "Error code 50001 -- Error in the backend data store")
      })
  public Mode getMode(
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Whether to return the global mode if subject mode not found")
      @QueryParam("defaultToGlobal") boolean defaultToGlobal) {

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    try {
      io.confluent.kafka.schemaregistry.storage.Mode mode = defaultToGlobal
          ? schemaRegistry.getModeInScope(subject)
          : schemaRegistry.getMode(subject);
      if (mode == null) {
        throw Errors.subjectLevelModeNotConfiguredException(subject);
      }
      return new Mode(mode.name());
    } catch (SchemaRegistryException e) {
      throw Errors.storeException("Failed to get mode", e);
    }
  }

  @PUT
  @Operation(summary = "Update global mode",
      description = "Update global mode. "
        + "On success, echoes the original request back to the client.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The original request",
            content = @Content(schema = @Schema(implementation = ModeUpdateRequest.class))),
        @ApiResponse(responseCode = "422", description = "Error code 42204 -- Invalid mode\n"
            + "Error code 42205 -- Operation not permitted"),
        @ApiResponse(responseCode = "500", description =
            "Error code 50001 -- Error in the backend data store\n"
                + "Error code 50003 -- Error while forwarding the request to the primary\n"
                + "Error code 50004 -- Unknown leader")})
  public ModeUpdateRequest updateTopLevelMode(
      @Context HttpHeaders headers,
      @Parameter(description = "Update Request", required = true)
      @NotNull ModeUpdateRequest request,
      @Parameter(description =
          "Whether to force update if setting mode to IMPORT and schemas currently exist")
      @QueryParam("force") boolean force) {
    return updateMode(null, headers, request, force);
  }

  @GET
  @Operation(summary = "Get global mode",
      description = "Retrieves global mode.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The global mode",
            content = @Content(schema = @Schema(implementation = Mode.class))),
        @ApiResponse(responseCode = "500",
            description = "Error code 50001 -- Error in the backend data store")
      })
  public Mode getTopLevelMode() {
    return getMode(null, false);
  }

  @DELETE
  @Path("/{subject}")
  @Operation(summary = "Delete subject mode",
      description = "Deletes the specified subject-level mode and reverts to "
        + "the global default.",
      responses = {
        @ApiResponse(responseCode = "200", description = "Operation succeeded. Returns old mode",
          content = @Content(schema = @Schema(implementation = Mode.class))),
        @ApiResponse(responseCode = "404", description = "Error code 40401 -- Subject not found"),
        @ApiResponse(responseCode = "500", description = "Error code 50001 -- Error in the backend "
          + "datastore")
      })
  public void deleteSubjectMode(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers,
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject) {
    log.info("Deleting mode for subject {}", subject);

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    io.confluent.kafka.schemaregistry.storage.Mode deletedMode;
    Mode deleteModeResponse;
    try {
      deletedMode = schemaRegistry.getMode(subject);
      if (deletedMode == null) {
        throw Errors.subjectNotFoundException(subject);
      }

      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
          headers, schemaRegistry.config().whitelistHeaders());
      schemaRegistry.deleteSubjectModeOrForward(subject, headerProperties);
      deleteModeResponse = new Mode(deletedMode.name());
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to delete mode", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Failed to delete mode", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding delete mode request"
          + " to the leader", e);
    }
    asyncResponse.resume(deleteModeResponse);
  }
}
