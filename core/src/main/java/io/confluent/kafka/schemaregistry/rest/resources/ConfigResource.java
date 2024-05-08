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

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidCompatibilityException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidRuleSetException;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
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
import java.util.Map;

@Path("/config")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class ConfigResource {

  public static final String apiTag = "Config (v1)";
  private static final Logger log = LoggerFactory.getLogger(ConfigResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public ConfigResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Path("/{subject}")
  @PUT
  @DocumentedName("updateSubjectConfig")
  @Operation(summary = "Update subject compatibility level",
      description = "Update compatibility level for the specified subject. "
        + "On success, echoes the original request back to the client.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The original request.",
            content = @Content(schema = @Schema(implementation = ConfigUpdateRequest.class))),
        @ApiResponse(responseCode = "404",
            description = "Not Found. Error code 40401 indicates subject not found.",
            content = @Content(schema = @Schema(implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "422",
            description = "Unprocessable Entity. "
                    + "Error code 42203 indicates invalid compatibility level.",
            content = @Content(schema = @Schema(implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
            description = "Internal Server Error. "
                    + "Error code 50001 indicates a failure in the backend data store. "
                    + "Error code 50003 indicates a failure forwarding the request to the primary.",
            content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public ConfigUpdateRequest updateSubjectLevelConfig(
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Context HttpHeaders headers,
      @Parameter(description = "Config Update Request", required = true)
      @NotNull ConfigUpdateRequest request) {

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, schemaRegistry.config().whitelistHeaders());
    schemaRegistry.getCompositeUpdateRequestHandler().handle(subject, request, headerProperties);

    CompatibilityLevel compatibilityLevel =
        CompatibilityLevel.forName(request.getCompatibilityLevel());
    if (request.getCompatibilityLevel() != null && compatibilityLevel == null) {
      throw new RestInvalidCompatibilityException();
    }
    if (request.getDefaultRuleSet() != null) {
      try {
        request.getDefaultRuleSet().validate();
      } catch (RuleException e) {
        throw new RestInvalidRuleSetException(e.getMessage());
      }
    }
    if (request.getOverrideRuleSet() != null) {
      try {
        request.getOverrideRuleSet().validate();
      } catch (RuleException e) {
        throw new RestInvalidRuleSetException(e.getMessage());
      }
    }
    if (subject != null && !QualifiedSubject.isValidSubject(schemaRegistry.tenant(), subject)) {
      throw Errors.invalidSubjectException(subject);
    }

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    try {
      schemaRegistry.updateConfigOrForward(subject, new Config(request), headerProperties);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to update compatibility level", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Failed to update compatibility level", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding update config request"
                                                    + " to the leader", e);
    }

    return request;
  }

  @Path("/{subject}")
  @GET
  @DocumentedName("getSubjectConfig")
  @Operation(summary = "Get subject compatibility level",
      description = "Retrieves compatibility level for a subject.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The subject compatibility level.",
            content = @Content(schema = @Schema(implementation = Config.class))),
        @ApiResponse(responseCode = "404",
            description = "Not Found. Error code 40401 indicates subject not found.",
            content = @Content(schema = @Schema(implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
            description = "Internal Server Error. "
                    + "Error code 50001 indicates a failure in the backend data store.",
            content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public Config getSubjectLevelConfig(
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Parameter(description =
          "Whether to return the global compatibility level "
              + " if subject compatibility level not found")
      @QueryParam("defaultToGlobal") boolean defaultToGlobal) {

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    Config config;
    try {
      config =
          defaultToGlobal
          ? schemaRegistry.getConfigInScope(subject)
          : schemaRegistry.getConfig(subject);
      if (config == null) {
        throw Errors.subjectLevelCompatibilityNotConfiguredException(subject);
      }
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to get the configs for subject "
                                  + subject, e);
    }

    return config;
  }

  @PUT
  @DocumentedName("updateGlobalConfig")
  @Operation(summary = "Update global compatibility level",
      description = "Updates the global compatibility level. "
      + "On success, echoes the original request back to the client.", responses = {
        @ApiResponse(responseCode = "200", description = "The original request.",
            content = @Content(schema = @Schema(implementation = ConfigUpdateRequest.class))),
        @ApiResponse(responseCode = "422",
            description = "Unprocessable Entity. "
                    + "Error code 42203 indicates invalid compatibility level.",
            content = @Content(schema = @Schema(implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store. "
                  + "Error code 50003 indicates a failure forwarding the request to the primary.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public ConfigUpdateRequest updateTopLevelConfig(
      @Context HttpHeaders headers,
      @Parameter(description = "Config Update Request", required = true)
      @NotNull ConfigUpdateRequest request) {

    schemaRegistry.getCompositeUpdateRequestHandler().handle(request);

    CompatibilityLevel compatibilityLevel =
        CompatibilityLevel.forName(request.getCompatibilityLevel());
    if (request.getCompatibilityLevel() != null && compatibilityLevel == null) {
      throw new RestInvalidCompatibilityException();
    }
    if (request.getDefaultRuleSet() != null) {
      try {
        request.getDefaultRuleSet().validate();
      } catch (RuleException e) {
        throw new RestInvalidRuleSetException(e.getMessage());
      }
    }
    if (request.getOverrideRuleSet() != null) {
      try {
        request.getOverrideRuleSet().validate();
      } catch (RuleException e) {
        throw new RestInvalidRuleSetException(e.getMessage());
      }
    }
    try {
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
          headers, schemaRegistry.config().whitelistHeaders());
      schemaRegistry.updateConfigOrForward(null, new Config(request), headerProperties);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to update compatibility level", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Failed to update compatibility level", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding update config request"
                                                    + " to the leader", e);
    }

    return request;
  }

  @GET
  @DocumentedName("getGlobalConfig")
  @Operation(summary = "Get global compatibility level",
      description = "Retrieves the global compatibility level.", responses = {
        @ApiResponse(responseCode = "200", description = "The global compatibility level.",
          content = @Content(schema = @Schema(implementation = Config.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public Config getTopLevelConfig() {
    Config config;
    try {
      config = schemaRegistry.getConfig(null);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to get compatibility level", e);
    }
    return config;
  }

  @DELETE
  @DocumentedName("deleteGlobalConfig")
  @Operation(summary = "Delete global compatibility level",
          description = "Deletes the global compatibility level config and reverts to the default.",
          responses = {
            @ApiResponse(responseCode = "200",
              description = "Operation succeeded. Returns old global compatibility level.",
              content = @Content(schema = @Schema(implementation = CompatibilityLevel.class),
                      examples = {@ExampleObject(value = "FULL_TRANSITIVE")})),
            @ApiResponse(responseCode = "500",
              description = "Internal Server Error. "
                          + "Error code 50001 indicates a failure in the backend data store.",
              content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public void deleteTopLevelConfig(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers) {
    log.debug("Deleting Global compatibility setting and reverting back to default");

    Config deletedConfig;
    try {
      deletedConfig = schemaRegistry.getConfig(null);
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
          headers, schemaRegistry.config().whitelistHeaders());
      schemaRegistry.deleteConfigOrForward(null, headerProperties);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to delete compatibility level", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Failed to delete compatibility level", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding delete config request"
          + " to the leader", e);
    }
    asyncResponse.resume(deletedConfig);
  }

  @DELETE
  @Path("/{subject}")
  @DocumentedName("deleteSubjectConfig")
  @Operation(summary = "Delete subject compatibility level",
      description = "Deletes the specified subject-level compatibility level config and "
      + "reverts to the global default.", responses = {
        @ApiResponse(responseCode = "200",
          description = "Operation succeeded. Returns old compatibility level.",
          content = @Content(schema = @Schema(implementation = CompatibilityLevel.class),
                  examples = {@ExampleObject(value = "FULL_TRANSITIVE")})),
        @ApiResponse(responseCode = "404",
          description = "Not Found. Error code 40401 indicates subject not found.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store.",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public void deleteSubjectConfig(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers,
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject) {
    log.debug("Deleting compatibility setting for subject {}", subject);

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    Config deletedConfig;
    try {
      deletedConfig = schemaRegistry.getConfig(subject);
      if (deletedConfig == null) {
        throw Errors.subjectNotFoundException(subject);
      }

      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
          headers, schemaRegistry.config().whitelistHeaders());
      schemaRegistry.deleteConfigOrForward(subject, headerProperties);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to delete compatibility level", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Failed to delete compatibility level", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding delete config request"
          + " to the leader", e);
    }
    asyncResponse.resume(deletedConfig);
  }
}
