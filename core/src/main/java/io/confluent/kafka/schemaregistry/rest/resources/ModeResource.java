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

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Mode;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.ReferenceExistsException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
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
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.tags.Tags;
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

  public static final String apiTag = "Modes (v1)";
  private static final Logger log = LoggerFactory.getLogger(ModeResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public ModeResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Path("/{subject}")
  @PUT
  @DocumentedName("updateSubjectMode")
  @Operation(summary = "Update subject mode",
      description = "Update mode for the specified subject. "
        + "On success, echoes the original request back to the client.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The original request.",
            content = @Content(schema = @Schema(implementation = ModeUpdateRequest.class))),
        @ApiResponse(responseCode = "422",
          description = "Unprocessable Entity. "
                  + "Error code 42204 indicates an invalid mode. "
                  + "Error code 42205 indicates operation not permitted.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store. "
                  + "Error code 50003 indicates a failure forwarding the request to the primary. "
                  + "Error code 50004 indicates unknown leader.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
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

    if (subject != null && !QualifiedSubject.isValidSubject(schemaRegistry.tenant(), subject)) {
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
    } catch (ReferenceExistsException e) {
      throw Errors.referenceExistsException(e.getMessage());
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to update mode", e);
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Update mode operation timed out", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Failed to update mode", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding update mode request"
                                                    + " to the leader", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while updating the mode", e);
    }

    return request;
  }

  @Path("/{subject}")
  @GET
  @DocumentedName("getSubjectMode")
  @Operation(summary = "Get subject mode",
      description = "Retrieves the subject mode.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The subject mode.",
          content = @Content(schema = @Schema(implementation = Mode.class))),
        @ApiResponse(responseCode = "404",
          description = "Not Found. Error code 40401 indicates subject not found.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
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
  @DocumentedName("updateGlobalMode")
  @Operation(summary = "Update global mode",
      description = "Update global mode. "
        + "On success, echoes the original request back to the client.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The original request.",
          content = @Content(schema = @Schema(implementation = ModeUpdateRequest.class))),
        @ApiResponse(responseCode = "422",
          description = "Unprocessable Entity. "
                  + "Error code 42204 indicates an invalid mode. "
                  + "Error code 42205 indicates operation not permitted.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store. "
                  + "Error code 50003 indicates a failure forwarding the request to the primary. "
                  + "Error code 50004 indicates unknown leader.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
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
  @DocumentedName("getGlobalMode")
  @Operation(summary = "Get global mode",
      description = "Retrieves global mode.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The global mode",
            content = @Content(schema = @Schema(implementation = Mode.class))),
        @ApiResponse(responseCode = "500",
            description = "Error code 50001 -- Error in the backend data store")
      })
  @Tags(@Tag(name = apiTag))
  public Mode getTopLevelMode() {
    return getMode(null, false);
  }

  @DELETE
  @Path("/{subject}")
  @DocumentedName("deleteSubjectMode")
  @Operation(summary = "Delete subject mode",
      description = "Deletes the specified subject-level mode and reverts to "
        + "the global default.",
      responses = {
        @ApiResponse(responseCode = "200", description = "Operation succeeded. Returns old mode.",
          content = @Content(schema = @Schema(implementation = Mode.class))),
        @ApiResponse(responseCode = "404",
          description = "Not Found. Error code 40401 indicates subject not found.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public void deleteSubjectMode(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers,
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject) {
    log.debug("Deleting mode for subject {}", subject);

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
