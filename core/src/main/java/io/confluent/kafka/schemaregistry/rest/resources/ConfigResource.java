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

import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownMasterException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidCompatibilityException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;

@Path("/config")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class ConfigResource {

  private static final Logger log = LoggerFactory.getLogger(ConfigResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public ConfigResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Path("/{subject}")
  @PUT
  @ApiOperation(value = "Update compatibility level for the specified subject.")
  @ApiResponses(value = {
      @ApiResponse(code = 422, message = "Error code 42203 -- Invalid compatibility level\n"
          + "Error code 40402 -- Version not found"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store\n"
          + "Error code 50003 -- Error while forwarding the request to the primary")
  })
  public ConfigUpdateRequest updateSubjectLevelConfig(
      @ApiParam(value = "Name of the Subject", required = true)@PathParam("subject") String subject,
      @Context HttpHeaders headers,
      @ApiParam(value = "Config Update Request", required = true)
      @NotNull ConfigUpdateRequest request) {
    Set<String> subjects = null;
    try {
      subjects = schemaRegistry.listSubjects();
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to retrieve a list of all subjects"
                                  + " from the registry", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Failed to retrieve a list of all subjects"
                                           + " from the registry", e);
    }
    CompatibilityLevel compatibilityLevel =
        CompatibilityLevel.forName(request.getCompatibilityLevel());
    if (compatibilityLevel == null) {
      throw new RestInvalidCompatibilityException();
    }
    try {
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
          headers, schemaRegistry.config().whitelistHeaders());
      schemaRegistry.updateConfigOrForward(subject, compatibilityLevel, headerProperties);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to update compatibility level", e);
    } catch (UnknownMasterException e) {
      throw Errors.unknownMasterException("Failed to update compatibility level", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding update config request"
                                                    + " to the master", e);
    }
    if (!subjects.contains(subject)) {
      log.debug("Updated compatibility level for unregistered subject " + subject + " to "
                + request.getCompatibilityLevel());
    } else {
      log.debug("Updated compatibility level for subject " + subject + " to "
                + request.getCompatibilityLevel());
    }

    return request;
  }

  @Path("/{subject}")
  @GET
  @ApiOperation(value = "Get compatibility level for a subject.")
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "Subject not found"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store")})
  public Config getSubjectLevelConfig(
      @PathParam("subject") String subject,
      @QueryParam("defaultToGlobal") boolean defaultToGlobal) {
    Config config = null;
    try {
      CompatibilityLevel compatibilityLevel =
          defaultToGlobal
          ? schemaRegistry.getCompatibilityLevelInScope(subject)
          : schemaRegistry.getCompatibilityLevel(subject);
      if (compatibilityLevel == null) {
        throw Errors.subjectNotFoundException(subject);
      }
      config = new Config(compatibilityLevel.name);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to get the configs for subject "
                                  + subject, e);
    }

    return config;
  }

  @PUT
  @ApiOperation(value = "Update global compatibility level.")
  @ApiResponses(value = {
      @ApiResponse(code = 422, message = "Error code 42203 -- Invalid compatibility level"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store\n"
          + "Error code 50003 -- Error while forwarding the request to the primary\n")
  })
  public ConfigUpdateRequest updateTopLevelConfig(
      @Context HttpHeaders headers,
      @ApiParam(value = "Config Update Request", required = true)
      @NotNull ConfigUpdateRequest request) {
    CompatibilityLevel compatibilityLevel =
        CompatibilityLevel.forName(request.getCompatibilityLevel());
    if (compatibilityLevel == null) {
      throw new RestInvalidCompatibilityException();
    }
    try {
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
          headers, schemaRegistry.config().whitelistHeaders());
      schemaRegistry.updateConfigOrForward(null, compatibilityLevel, headerProperties);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to update compatibility level", e);
    } catch (UnknownMasterException e) {
      throw Errors.unknownMasterException("Failed to update compatibility level", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding update config request"
                                                    + " to the master", e);
    }

    return request;
  }

  @GET
  @ApiOperation(value = "Get global compatibility level.")
  @ApiResponses(value = {@ApiResponse(code = 500,
      message = "Error code 50001 -- Error in the backend data store")}
  )
  public Config getTopLevelConfig() {
    Config config = null;
    try {
      CompatibilityLevel compatibilityLevel = schemaRegistry.getCompatibilityLevel(null);
      config = new Config(compatibilityLevel == null ? null : compatibilityLevel.name);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to get compatibility level", e);
    }
    return config;
  }
}
